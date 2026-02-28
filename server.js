'use strict';

const express = require('express');
const weatherRoutes = require('./routes/weather');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get('/', (req, res) => {
  res.json({ name: 'wundwx', description: 'Weather Underground weather data server', version: '1.0.0' });
});

app.use('/api/weather', weatherRoutes);

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Internal server error' });
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`wundwx server running on port ${PORT}`);
  });
}

module.exports = app;
