import WebSocket from 'ws';

const SOCKET_URL = 'wss://real.shelteric.com';
const RIDER_TOKEN  = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJpZGVyLTAwMSIsInJvbGUiOiJyaWRlciJ9.pz5qZubhjBOCuM-BwbaImq21Hfm-4Iu_W4NF3JL2_ig';
const DRIVER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImRyaXZlci0wMDEiLCJyb2xlIjoiZHJpdmVyIn0.LlItUpllcsY5aXlIHt9pH8eI7oRzAc-WJoOcIo0H9NY';

let step = 0;

function connect(role, token, onMessage) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`${SOCKET_URL}?token=${token}`);
    ws.on('open', () => {
      ws.send(JSON.stringify({ type: 'auth', role, id: `${role}-001` }));
    });
    ws.on('message', (raw) => {
      const data = JSON.parse(raw.toString());
      if (data.type === 'auth_success') resolve(ws);
      else onMessage(ws, data);
    });
  });
}

async function main() {
  console.log('Starting full flow test...');
  let driverWs, riderWs;

  const onDriverMessage = (ws, data) => {
    if (data.type === 'new_ride_request') {
      console.log('✅ Driver received ride request');
      ws.send(JSON.stringify({
        type: 'ride_accept',
        riderId: 'rider-001',
        payload: { vehicleType: 'Auto' }
      }));
    } else if (data.type === 'trip_status_changed' && data.payload.status === 'cancelled') {
      console.log('✅ Driver received ride cancellation');
      console.log('🎉 FULL FLOW TEST PASSED');
      process.exit(0);
    } else if (data.type === 'CHAT_MESSAGE') {
      console.log('✅ Driver received chat message:', data.payload.text);
    }
  };

  const onRiderMessage = (ws, data) => {
    if (data.type === 'ride_accepted') {
      console.log('✅ Rider received ride accepted. OTP:', data.payload.otp);
      
      // Send chat message
      ws.send(JSON.stringify({
        type: 'CHAT_MESSAGE',
        toId: 'driver-001',
        text: 'Where are you?'
      }));

      // Cancel ride after 1 sec
      setTimeout(() => {
        ws.send(JSON.stringify({
          type: 'ride_cancel',
          riderId: 'rider-001',
          reason: 'cancelled_by_rider'
        }));
      }, 1000);
    }
  };

  driverWs = await connect('driver', DRIVER_TOKEN, onDriverMessage);
  console.log('Driver connected');
  driverWs.send(JSON.stringify({ type: 'driver_status', status: 'available' }));
  driverWs.send(JSON.stringify({ type: 'location_update', location: { lat: 17.385, lng: 78.4867 } }));

  riderWs = await connect('rider', RIDER_TOKEN, onRiderMessage);
  console.log('Rider connected');

  // Request ride
  riderWs.send(JSON.stringify({
    type: 'ride_request',
    payload: {
      riderId: 'rider-001',
      pickupLocation: { lat: 17.385, lng: 78.4867 },
      dropLocation: { lat: 17.4401, lng: 78.3489 },
      fare: 150,
      vehicleType: 'Auto',
      parcelDetails: { senderName: 'John', receiverName: 'Doe' }
    }
  }));

  setTimeout(() => {
    console.error('❌ Test timed out');
    process.exit(1);
  }, 10000);
}

main();
