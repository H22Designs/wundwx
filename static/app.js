document.addEventListener('DOMContentLoaded', () => {

    // Global chart configs
    Chart.defaults.color = '#8b949e';
    Chart.defaults.font.family = "'Outfit', sans-serif";

    let tempChartInstance = null;
    let windChartInstance = null;
    let modalChartInstance = null;
    let currentModalMetric = null;
    let currentModalHours = 24;

    const metricNames = {
        'temperature': 'Temperature (°F)',
        'humidity': 'Humidity (%)',
        'wind_speed': 'Wind Speed (mph)',
        'pressure': 'Pressure (inHg)',
        'precip_total': 'Precipitation (in)',
        'solar_radiation': 'Solar Radiation (W/m²)'
    };

    const metricColors = {
        'temperature': '#58a6ff',
        'humidity': '#2ea043',
        'wind_speed': '#a371f7',
        'pressure': '#f85149',
        'precip_total': '#3fb950',
        'solar_radiation': '#d29922'
    };

    async function fetchCurrentData() {
        try {
            const response = await fetch('/api/weather/current');
            if (!response.ok) throw new Error("No data yet.");
            const data = await response.json();
            updateDashboard(data);
        } catch (err) {
            console.error(err);
        }
    }

    async function fetchHistoricalData() {
        try {
            const response = await fetch('/api/weather/history?hours=24');
            if (!response.ok) throw new Error("Could not fetch history");
            const data = await response.json();
            updateCharts(data);
        } catch (err) {
            console.error(err);
        }
    }

    function updateDashboard(data) {
        document.getElementById('val-temp').innerText = (data.temperature ?? '--').toFixed(1);
        document.getElementById('val-dew').innerText = (data.dew_point ?? '--').toFixed(1);
        document.getElementById('val-humidity').innerText = Math.round(data.humidity ?? 0);
        document.getElementById('val-wind').innerText = (data.wind_speed ?? 0).toFixed(1);
        document.getElementById('val-winddir').innerText = data.wind_dir ?? '--';
        document.getElementById('val-gust').innerText = (data.wind_gust ?? 0).toFixed(1);
        document.getElementById('val-pressure').innerText = (data.pressure ?? '--').toFixed(2);
        document.getElementById('val-precip').innerText = (data.precip_total ?? 0).toFixed(2);
        document.getElementById('val-precip-rate').innerText = (data.precip_rate ?? 0).toFixed(2);
        document.getElementById('val-solar').innerText = Math.round(data.solar_radiation ?? 0);
        document.getElementById('val-uv').innerText = data.uv_index ?? '--';

        // Update timestamp
        const date = new Date(data.timestamp + 'Z'); // Add Z to fix UTC parse if needed, but db stores naive utc
        document.getElementById('last-updated').innerHTML = `Last Updated: <strong>${new Date().toLocaleTimeString()}</strong>`;
    }

    function createOrUpdateChart(ctxId, chartInstance, label, labels, dataPoints, color) {
        const ctx = document.getElementById(ctxId).getContext('2d');

        if (chartInstance) {
            chartInstance.data.labels = labels;
            chartInstance.data.datasets[0].data = dataPoints;
            chartInstance.update();
            return chartInstance;
        }

        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: label,
                    data: dataPoints,
                    borderColor: color,
                    backgroundColor: `${color}33`, // 20% opacity
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true,
                    pointRadius: 0,
                    pointHitRadius: 10
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: 'rgba(22, 27, 34, 0.9)',
                        titleColor: '#fff',
                        bodyColor: '#e6edf3',
                        borderColor: 'rgba(255, 255, 255, 0.1)',
                        borderWidth: 1
                    }
                },
                scales: {
                    x: {
                        grid: { display: false, drawBorder: false },
                        ticks: { maxTicksLimit: 8 }
                    },
                    y: {
                        grid: { color: 'rgba(255, 255, 255, 0.05)' }
                    }
                },
                interaction: {
                    mode: 'nearest',
                    axis: 'x',
                    intersect: false
                }
            }
        });
    }

    function updateCharts(historyData) {
        if (!historyData || historyData.length === 0) return;

        // historyData is chronological (oldest to newest)
        const labels = historyData.map(d => {
            const dt = new Date(d.timestamp + 'Z');
            return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        });

        const temps = historyData.map(d => d.temperature);
        const winds = historyData.map(d => d.wind_speed);

        tempChartInstance = createOrUpdateChart('tempChart', tempChartInstance, 'Temperature (°F)', labels, temps, '#58a6ff');
        windChartInstance = createOrUpdateChart('windChart', windChartInstance, 'Wind Speed (mph)', labels, winds, '#2ea043');
    }

    // Modal Logic
    window.openModal = function (metricKey) {
        currentModalMetric = metricKey;

        document.getElementById('modal-title').innerText = `${metricNames[metricKey] || 'Metric'} History`;
        document.getElementById('chart-modal').classList.remove('hidden');
        document.body.style.overflow = 'hidden'; // prevent background scrolling

        // Set default times to last 24 hours in local browser time to populate inputs
        const now = new Date();
        const yesterday = new Date(now.getTime() - (24 * 60 * 60 * 1000));

        // Format for datetime-local (YYYY-MM-DDThh:mm)
        const formatLocal = (dt) => {
            return new Date(dt.getTime() - (dt.getTimezoneOffset() * 60000)).toISOString().slice(0, 16);
        };

        document.getElementById('start-time').value = formatLocal(yesterday);
        document.getElementById('end-time').value = formatLocal(now);

        loadModalChartData();
    };

    window.closeModal = function () {
        document.getElementById('chart-modal').classList.add('hidden');
        document.body.style.overflow = '';
        currentModalMetric = null;
    };

    document.getElementById('apply-time-btn').addEventListener('click', () => {
        loadModalChartData();
    });

    async function loadModalChartData() {
        if (!currentModalMetric) return;

        const startLocal = document.getElementById('start-time').value;
        const endLocal = document.getElementById('end-time').value;

        let url = `/api/weather/history?hours=24`; // fallback

        if (startLocal && endLocal) {
            // Convert back to UTC ISO for the backend
            const startUtc = new Date(startLocal).toISOString();
            const endUtc = new Date(endLocal).toISOString();
            url = `/api/weather/history?start=${encodeURIComponent(startUtc)}&end=${encodeURIComponent(endUtc)}`;
        }
        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error("Could not fetch extended history");
            const historyData = await response.json();

            if (!historyData || historyData.length === 0) return;

            const labels = historyData.map(d => {
                const dt = new Date(d.timestamp + 'Z');
                return dt.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
            });

            const dataPoints = historyData.map(d => d[currentModalMetric]);
            const label = metricNames[currentModalMetric] || currentModalMetric;
            const color = metricColors[currentModalMetric] || '#e6edf3';

            modalChartInstance = createOrUpdateChart('modalChart', modalChartInstance, label, labels, dataPoints, color);
        } catch (err) {
            console.error(err);
        }
    }

    // Modal background click close
    document.getElementById('chart-modal').addEventListener('click', (e) => {
        if (e.target.id === 'chart-modal') closeModal();
    });

    // Initial fetch
    fetchCurrentData();
    fetchHistoricalData();

    // Poll every 30 seconds
    setInterval(() => {
        fetchCurrentData();
        fetchHistoricalData();
    }, 30000);

});
