import WebSocket from 'ws';
import jwt from 'jsonwebtoken';

const JWT_SECRET = '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';
const riderToken = jwt.sign({ id: 'rider-001', role: 'rider' }, JWT_SECRET, { noTimestamp: true });
const driverToken = jwt.sign({ id: 'ffe12862-83d8-468b-8c56-1481cf18b818', role: 'driver' }, JWT_SECRET, { noTimestamp: true });

const riderWs = new WebSocket(`wss://real.shelteric.com?token=${riderToken}`);
const driverWs = new WebSocket(`wss://real.shelteric.com?token=${driverToken}`);

let riderConnected = false;
let driverConnected = false;
let testPassed = false;

riderWs.on('open', () => {
  console.log('[Rider] Connected to WS');
  riderWs.send(JSON.stringify({ type: 'auth', role: 'rider', id: 'rider-001' }));
});

driverWs.on('open', () => {
  console.log('[Driver] Connected to WS');
  driverWs.send(JSON.stringify({ type: 'auth', role: 'driver', id: 'ffe12862-83d8-468b-8c56-1481cf18b818' }));
});

riderWs.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  console.log('[Rider WS]', msg.type);
  if (msg.type === 'auth_success') riderConnected = true;
  if (msg.type === 'instant_ride_started') {
    console.log('✅ Rider received instant_ride_started!');
    if (testPassed) {
      process.exit(0);
    }
    testPassed = true;
  }
});

driverWs.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  console.log('[Driver WS]', msg.type);
  if (msg.type === 'auth_success') {
    driverConnected = true;
    setTimeout(() => {
      console.log('[Driver] Sending instant_ride_start...');
      driverWs.send(JSON.stringify({
        type: 'instant_ride_start',
        bookingId: 'test-booking-123',
        riderId: 'rider-001',
        driverName: 'Test Driver',
        code: '1234',
        vehicle: 'bike',
        fare: 50
      }));
    }, 1000);
  }
  if (msg.type === 'instant_ride_confirmed') {
    console.log('✅ Driver received instant_ride_confirmed!');
    if (testPassed) {
      process.exit(0);
    }
    testPassed = true;
  }
});

setTimeout(() => {
  console.log('Timeout. Exiting...');
  process.exit(1);
}, 5000);
