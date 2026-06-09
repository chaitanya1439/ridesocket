import WebSocket from 'ws';

const SOCKET_URL = 'wss://real.shelteric.com';
const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJpZGVyLTAwMSIsInJvbGUiOiJyaWRlciJ9.xxx'; // We will use a mock token or let's use the actual rider token if we can construct one
// Wait, we need a valid rider token signed with the server's JWT_SECRET.
import jwt from 'jsonwebtoken';
const secret = '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';
const riderToken = jwt.sign({ id: 'rider-001', role: 'rider' }, secret, { noTimestamp: true });

const urlWithAuth = `${SOCKET_URL}?token=${encodeURIComponent(riderToken)}`;

console.log(`Attempting to connect to ${SOCKET_URL} as rider...`);

const ws = new WebSocket(urlWithAuth);

ws.on('open', () => {
  console.log('✅ WebSocket connection opened as Rider!');
  
  const authMsg = { type: 'auth', role: 'rider', id: '+91 5666666666' };
  console.log('Sending auth message:', authMsg);
  ws.send(JSON.stringify(authMsg));

  // Request a ride after 2 seconds
  setTimeout(() => {
    const requestMsg = {
      type: 'ride_request',
      payload: {
        riderId: '+91 5666666666',
        pickupLocation: { lat: 17.3401256, lng: 78.5517129 },
        dropLocation: { lat: 17.3496413, lng: 78.54802889999999 },
        fare: 56,
        vehicleType: 'auto',
        pickupAddress: 'ROMA ARCADE',
        dropAddress: 'Metro Station L B Nagar',
        riderName: 'Test Rider'
      }
    };
    console.log('Sending ride_request:', requestMsg);
    ws.send(JSON.stringify(requestMsg));
  }, 2000);

  setTimeout(() => {
    console.log('Closing rider connection...');
    ws.close();
    process.exit(0);
  }, 10000);
});

ws.on('message', (data) => {
  console.log('📩 Rider received message from server:', data.toString());
});

ws.on('error', (err) => {
  console.error('❌ WebSocket error:', err.message);
});
