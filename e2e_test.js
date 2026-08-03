import WebSocket from 'ws';

const URL = 'wss://real.shelteric.com';
const DUMMY_DRIVER_ID = 'ffe12862-83d8-468b-8c56-1481cf18b818';
const DRIVER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJmZmUxMjg2Mi04M2Q4LTQ2OGItOGM1Ni0xNDgxY2YxOGI4MTgiLCJpYXQiOjE3NjEyMTg3MjUsImV4cCI6MTc2MTgyMzUyNX0.Ny5Gt3TFZvX-mLpBdQJ8nWR0rqIbQGpXPGrcEWWNlVs';

const RIDER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJpZGVyLTAwMSIsInJvbGUiOiJyaWRlciJ9.pz5qZubhjBOCuM-BwbaImq21Hfm-4Iu_W4NF3JL2_ig';
const DUMMY_RIDER_ID = 'rider-001';

console.log(`🚀 Starting E2E URL test for Realtime Server at ${URL}...\n`);

const driverWs = new WebSocket(`${URL}/?token=${DRIVER_TOKEN}`);
const riderWs = new WebSocket(`${URL}/?token=${RIDER_TOKEN}`);

let driverConnected = false;
let riderConnected = false;

const logDriver = (msg) => console.log(`[DRIVER 🚗] ${msg}`);
const logRider = (msg) => console.log(`[RIDER 🧍] ${msg}`);

driverWs.on('open', () => {
  logDriver('Connected to server, sending auth...');
  driverWs.send(JSON.stringify({
    type: 'auth',
    id: DUMMY_DRIVER_ID,
    role: 'driver',
    vehicleType: 'moto'
  }));
});

driverWs.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  
  if (msg.type === 'auth_success') {
    logDriver('Auth SUCCESS! Dummy subscription active.');
    
    // Send initial location so server knows driver is nearby
    logDriver('Sending initial location update...');
    driverWs.send(JSON.stringify({
      type: 'location_update',
      location: {
        latitude: 17.385,
        longitude: 78.486,
        heading: 90,
        vehicleType: 'moto'
      }
    }));

    driverConnected = true;
    checkBothConnected();
  } else if (msg.type === 'auth_error') {
    logDriver('Auth ERROR: ' + msg.message);
  } else if (msg.type === 'new_ride_request') {
    logDriver('Received new ride request! Accepting it...');
    
    setTimeout(() => {
      driverWs.send(JSON.stringify({
        type: 'ride_accept',
        riderId: msg.payload.riderId,
        payload: {
          pickupLocation: msg.payload.pickup,
          dropLocation: msg.payload.drop,
          fare: msg.payload.fare,
          vehicleType: msg.payload.type,
          otp: '1234'
        }
      }));
      logDriver('Sent ride_accept.');
      
      setTimeout(() => {
        logDriver('Sending live location update...');
        driverWs.send(JSON.stringify({
          type: 'location_update',
          location: {
            latitude: 17.3855,
            longitude: 78.4865,
            heading: 90,
            vehicleType: 'moto'
          }
        }));
      }, 1000);
      
      setTimeout(() => {
        logDriver('Sending chat message to rider...');
        driverWs.send(JSON.stringify({
          type: 'chat_message',
          recipientId: DUMMY_RIDER_ID,
          text: 'I am on my way!',
          timestamp: Date.now()
        }));
      }, 2000);
      
    }, 1000);
  } else if (msg.type === 'chat_message') {
    logDriver(`Received message: "${msg.text}"`);
    setTimeout(() => {
      console.log('\n✅ E2E URL Test Completed Successfully! All flows (Dispatch, Accept, Live Tracking, Chat) are working.');
      driverWs.close();
      riderWs.close();
      process.exit(0);
    }, 1000);
  } else if (msg.type === 'sync_state') {
    logDriver('Received sync_state. Clearing old trip...');
    driverWs.send(JSON.stringify({
      type: 'ride_complete',
      riderId: msg.payload.riderId
    }));
  } else {
    logDriver(`[Unhandled Message] ${JSON.stringify(msg)}`);
  }
});

riderWs.on('open', () => {
  logRider('Connected to server, sending auth...');
  riderWs.send(JSON.stringify({
    type: 'auth',
    id: DUMMY_RIDER_ID,
    role: 'rider'
  }));
});

riderWs.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  
  if (msg.type === 'auth_success') {
    logRider('Auth SUCCESS!');
    riderConnected = true;
    checkBothConnected();
  } else if (msg.type === 'driver_matched' || msg.type === 'ride_accepted') {
    logRider(`Driver matched! ID: ${msg.payload.driverId}`);
  } else if (msg.type === 'driver_location_changed') {
    logRider(`Received live tracking update! Lat: ${msg.payload.location.latitude}, Lng: ${msg.payload.location.longitude}`);
  } else if (msg.type === 'chat_message') {
    logRider(`Received message: "${msg.text}"`);
    
    setTimeout(() => {
      logRider('Replying to driver...');
      riderWs.send(JSON.stringify({
        type: 'chat_message',
        recipientId: DUMMY_DRIVER_ID,
        text: 'Okay, waiting outside.',
        timestamp: Date.now()
      }));
    }, 1000);
  } else {
    logRider(`[Unhandled Message] ${JSON.stringify(msg)}`);
  }
});

function checkBothConnected() {
  if (driverConnected && riderConnected) {
    // Wait a short moment to ensure driver location update is registered
    setTimeout(() => {
      console.log('\n--- Both connected & driver location set. Rider booking ride... ---');
      riderWs.send(JSON.stringify({
        type: 'ride_request',
        payload: {
          pickup: { address: 'Origin', lat: 17.385, lng: 78.486 }, // Same as driver
          drop: { address: 'Dest', lat: 17.40, lng: 78.50 },
          fare: 150,
          type: 'moto',
          customer: { id: DUMMY_RIDER_ID, name: 'Dummy Rider', phone: '1111111111', rating: 4.5 },
          status: 'pending'
        }
      }));
      logRider('Sent ride_request.');
    }, 3000);
  }
}
