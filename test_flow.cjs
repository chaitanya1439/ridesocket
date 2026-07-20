const WebSocket = require('ws');
const jwt = require('jsonwebtoken');

const JWT_SECRET = '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';
const riderToken = jwt.sign({ id: 'rider-1', role: 'rider' }, JWT_SECRET, { noTimestamp: true });
const driverToken = jwt.sign({ id: 'driver-1', role: 'driver' }, JWT_SECRET, { noTimestamp: true });

console.log('Testing full flow with Local or Remote Server...');
const SERVER_URL = 'wss://real.shelteric.com'; 
// To test live tracking without DB credentials blocking us, we will connect Rider first.

const riderWs = new WebSocket(`${SERVER_URL}?token=${riderToken}`);
let riderConnected = false;

riderWs.on('open', () => {
  riderWs.send(JSON.stringify({ type: 'auth', role: 'rider', id: 'rider-1' }));
});

riderWs.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  console.log('RIDER RECEIVED:', msg.type, msg.payload || msg.message || '');
  
  if (msg.type === 'auth_success') {
    riderConnected = true;
    console.log('Rider connected! Simulating ride request...');
    riderWs.send(JSON.stringify({
      type: 'ride_request',
      payload: { pickupLocation: { lat: 17.0, lng: 78.0 }, vehicleType: 'bike', fare: 100 }
    }));
  }
});

setTimeout(() => {
  console.log('Test completed.');
  riderWs.close();
}, 4000);
