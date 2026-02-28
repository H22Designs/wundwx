'use strict';

const express = require('express');
const { fetchCurrentConditions, fetchForecast } = require('../lib/wunderground');

const router = express.Router();

/**
 * GET /api/weather/current
 * Query params: stationId or lat & lon, units (e (imperial) | m (metric) | h (uk hybrid))
 */
router.get('/current', async (req, res, next) => {
  try {
    const { stationId, lat, lon, units = 'e' } = req.query;

    if (!stationId && !(lat && lon)) {
      return res.status(400).json({ error: 'Provide stationId or lat and lon query parameters' });
    }

    const data = await fetchCurrentConditions({ stationId, lat, lon, units });
    return res.json(data);
  } catch (err) {
    return next(err);
  }
});

/**
 * GET /api/weather/forecast
 * Query params: lat & lon required, units (e | m | h)
 */
router.get('/forecast', async (req, res, next) => {
  try {
    const { lat, lon, units = 'e' } = req.query;

    if (!lat || !lon) {
      return res.status(400).json({ error: 'lat and lon query parameters are required' });
    }

    const data = await fetchForecast({ lat, lon, units });
    return res.json(data);
  } catch (err) {
    return next(err);
  }
});

module.exports = router;
