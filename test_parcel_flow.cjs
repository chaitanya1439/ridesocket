const WebSocket = require('ws');
const jwt = require('jsonwebtoken');

const JWT_SECRET = '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';
const riderToken = jwt.sign({ id: 'rider-test', role: 'rider' }, JWT_SECRET, { noTimestamp: true });
const driverToken = jwt.sign({ id: 'driver-test', role: 'driver' }, JWT_SECRET, { noTimestamp: true });

const SERVER_URL = 'wss://real.shelteric.com';

const riderWs = new WebSocket(`${SERVER_URL}?token=${riderToken}`);
const driverWs = new WebSocket(`${SERVER_URL}?token=${driverToken}`);

let driverConnected = false;

driverWs.on('open', () => {
  driverWs.send(JSON.stringify({ type: 'auth', role: 'driver', id: 'driver-test' }));
});

riderWs.on('open', () => {
  riderWs.send(JSON.stringify({ type: 'auth', role: 'rider', id: 'rider-test' }));
});

driverWs.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  console.log('[DRIVER RX]', msg.type, msg.payload || '');
  
  if (msg.type === 'auth_success') {
    driverConnected = true;
  }
  
  if (msg.type === 'new_ride_request') {
    console.log('[DRIVER TX] Accepting ride...');
    driverWs.send(JSON.stringify({ type: 'ride_accept', payload: { rideId: msg.payload.id, riderId: 'rider-test' } }));
    
    setTimeout(() => {
      console.log('[DRIVER TX] status: arrived');
      driverWs.send(JSON.stringify({ type: 'trip_status_update', payload: { riderId: 'rider-test', status: 'arrived' } }));
    }, 1000);

    setTimeout(() => {
      console.log('[DRIVER TX] status: started (pickup OTP verified)');
      driverWs.send(JSON.stringify({ type: 'trip_status_update', payload: { riderId: 'rider-test', status: 'started' } }));
    }, 2000);

    setTimeout(() => {
      console.log('[DRIVER TX] status: completed (drop OTP verified)');
      driverWs.send(JSON.stringify({ type: 'trip_status_update', payload: { riderId: 'rider-test', status: 'completed' } }));
    }, 3000);
  }
});

riderWs.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  console.log('[RIDER RX]', msg.type, msg.payload || '');
  
  if (msg.type === 'auth_success') {
    setTimeout(() => {
      console.log('[RIDER TX] Requesting parcel ride...');
      riderWs.send(JSON.stringify({
        type: 'ride_request',
        payload: { pickupLocation: { lat: 17.0, lng: 78.0 }, vehicleType: 'parcel', fare: 100 }
      }));
    }, 500); // give driver time to auth
  }
});

setTimeout(() => {
  riderWs.close();
  driverWs.close();
  console.log('Test finished.');
}, 4500);
