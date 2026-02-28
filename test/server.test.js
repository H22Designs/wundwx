'use strict';

const { test, describe } = require('node:test');
const assert = require('node:assert/strict');

// Test the server routes with supertest-style assertions using the built-in http module
const http = require('node:http');
const app = require('../server');

function request(server, options, body) {
  return new Promise((resolve, reject) => {
    const req = http.request({ ...options, host: '127.0.0.1' }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

describe('wundwx server', () => {
  let server;
  let port;

  test('setup server for testing', async () => {
    await new Promise((resolve) => {
      server = app.listen(0, '127.0.0.1', resolve);
    });
    port = server.address().port;
  });

  test('GET / returns service info', async () => {
    const res = await request(server, { port, path: '/', method: 'GET' });
    assert.equal(res.status, 200);
    assert.equal(res.body.name, 'wundwx');
    assert.ok(res.body.description);
    assert.ok(res.body.version);
  });

  test('GET /unknown returns 404', async () => {
    const res = await request(server, { port, path: '/unknown', method: 'GET' });
    assert.equal(res.status, 404);
  });

  test('GET /api/weather/current without params returns 400', async () => {
    const res = await request(server, { port, path: '/api/weather/current', method: 'GET' });
    assert.equal(res.status, 400);
    assert.ok(res.body.error);
  });

  test('GET /api/weather/forecast without params returns 400', async () => {
    const res = await request(server, { port, path: '/api/weather/forecast', method: 'GET' });
    assert.equal(res.status, 400);
    assert.ok(res.body.error);
  });

  test('cleanup server after testing', async () => {
    await new Promise((resolve) => server.close(resolve));
  });
});
