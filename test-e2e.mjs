import WebSocket from 'ws';

const RIDER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJpZGVyLTAwMSIsInJvbGUiOiJyaWRlciJ9.pz5qZubhjBOCuM-BwbaImq21Hfm-4Iu_W4NF3JL2_ig';
const DRIVER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImRyaXZlci0wMDEiLCJyb2xlIjoiZHJpdmVyIn0.LlItUpllcsY5aXlIHt9pH8eI7oRzAc-WJoOcIo0H9NY';
const WS_URL = 'wss://real.shelteric.com';

const driverWs = new WebSocket(`${WS_URL}?token=${DRIVER_TOKEN}`);
const riderWs = new WebSocket(`${WS_URL}?token=${RIDER_TOKEN}`);

let step = 0;

driverWs.on('open', () => {
  console.log('Driver connected.');
  driverWs.send(JSON.stringify({ type: 'auth', role: 'driver', id: 'driver-001' }));
});

riderWs.on('open', () => {
  console.log('Rider connected.');
  riderWs.send(JSON.stringify({ type: 'auth', role: 'rider', id: 'rider-001' }));
});

driverWs.on('message', (data) => {
  const msg = JSON.parse(data);
  console.log('Driver received:', msg.type);
  
  if (msg.type === 'auth_success') {
    // Driver sets status to available and updates location
    driverWs.send(JSON.stringify({ type: 'driver_status', status: 'available' }));
    driverWs.send(JSON.stringify({ 
      type: 'location_update', 
      location: { lat: 17.385, lng: 78.4867 } 
    }));
  }
  
  if (msg.type === 'new_ride_request') {
    console.log('SUCCESS: Driver received new_ride_request!');
    process.exit(0);
  }
});

riderWs.on('message', (data) => {
  const msg = JSON.parse(data);
  console.log('Rider received:', msg.type);
  
  if (msg.type === 'auth_success') {
    // Wait a sec for driver to be ready, then send ride request
    setTimeout(() => {
      console.log('Rider sending ride_request...');
      riderWs.send(JSON.stringify({
        type: 'ride_request',
        payload: {
          riderId: 'rider-001',
          pickupLocation: { lat: 17.385, lng: 78.4867 },
          dropLocation: { lat: 17.4, lng: 78.5 },
          fare: 150,
          vehicleType: 'Bike'
        }
      }));
    }, 1000);
  }
});

setTimeout(() => {
  console.log('FAILED: Timeout waiting for new_ride_request');
  process.exit(1);
}, 5000);
