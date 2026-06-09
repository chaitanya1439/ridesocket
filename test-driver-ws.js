import WebSocket from 'ws';

const SOCKET_URL = 'ws://54.175.18.119:8080';
const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImRyaXZlci0wMDEiLCJyb2xlIjoiZHJpdmVyIn0.LlItUpllcsY5aXlIHt9pH8eI7oRzAc-WJoOcIo0H9NY';
const urlWithAuth = `${SOCKET_URL}?token=${encodeURIComponent(token)}`;

console.log(`Attempting to connect to ${SOCKET_URL}...`);

const ws = new WebSocket(urlWithAuth);

ws.on('open', () => {
  console.log('✅ WebSocket connection opened!');
  
  const authMsg = { type: 'auth', role: 'driver', id: 'D001' };
  console.log('Sending auth message:', authMsg);
  ws.send(JSON.stringify(authMsg));

  // Also simulate going "ON DUTY"
  setTimeout(() => {
    const statusMsg = { type: 'driver_status', status: 'available' };
    console.log('Sending driver_status:', statusMsg);
    ws.send(JSON.stringify(statusMsg));

    const locMsg = { 
      type: 'location_update', 
      location: { lat: 17.3401256, lng: 78.5517129 } 
    };
    console.log('Sending location_update:', locMsg);
    ws.send(JSON.stringify(locMsg));
  }, 1000);

  // Close after 30 seconds
  setTimeout(() => {
    console.log('Closing test connection...');
    ws.close();
    process.exit(0);
  }, 30000);
});

ws.on('message', (data) => {
  console.log('📩 Received message from server:', data.toString());
});

ws.on('error', (err) => {
  console.error('❌ WebSocket error:', err.message);
});

ws.on('close', (code, reason) => {
  console.log(`WebSocket closed. Code: ${code}, Reason: ${reason}`);
});
