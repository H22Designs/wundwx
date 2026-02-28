'use strict';

const BASE_URL = 'https://api.weather.com/v2';

/**
 * Build a Weather Underground (weather.com) API URL.
 * @param {string} path - API path
 * @param {Record<string, string>} params - query parameters (apiKey will be added automatically)
 * @returns {string} full URL
 */
function buildUrl(path, params) {
  const apiKey = process.env.WU_API_KEY;
  if (!apiKey) {
    throw new Error('WU_API_KEY environment variable is not set');
  }

  const url = new URL(`${BASE_URL}${path}`);
  url.searchParams.set('apiKey', apiKey);
  url.searchParams.set('format', 'json');

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, value);
    }
  }

  return url.toString();
}

/**
 * Fetch current conditions from Weather Underground.
 * @param {object} options
 * @param {string} [options.stationId] - personal weather station ID (e.g. "KNYNYC123")
 * @param {string} [options.lat] - latitude (used when stationId is not provided)
 * @param {string} [options.lon] - longitude
 * @param {string} [options.units] - 'e' (imperial), 'm' (metric), 'h' (uk hybrid)
 * @returns {Promise<object>} weather data
 */
async function fetchCurrentConditions({ stationId, lat, lon, units = 'e' }) {
  const { fetch } = await import('node-fetch');

  let url;
  if (stationId) {
    url = buildUrl('/pws/observations/current', { stationId, units, numericPrecision: 'decimal' });
  } else {
    url = buildUrl('/pws/observations/current', { geocode: `${lat},${lon}`, units, numericPrecision: 'decimal' });
  }

  const response = await fetch(url);

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Weather Underground API error ${response.status}: ${body}`);
  }

  return response.json();
}

/**
 * Fetch a 5-day hourly forecast from Weather Underground.
 * @param {object} options
 * @param {string} options.lat - latitude
 * @param {string} options.lon - longitude
 * @param {string} [options.units] - 'e' (imperial), 'm' (metric), 'h' (uk hybrid)
 * @returns {Promise<object>} forecast data
 */
async function fetchForecast({ lat, lon, units = 'e' }) {
  const { fetch } = await import('node-fetch');

  const url = buildUrl('/pws/dailyforecast/5day/current', { geocode: `${lat},${lon}`, units });

  const response = await fetch(url);

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Weather Underground API error ${response.status}: ${body}`);
  }

  return response.json();
}

module.exports = { fetchCurrentConditions, fetchForecast };
