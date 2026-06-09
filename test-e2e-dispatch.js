import WebSocket from 'ws';

// ─── Configuration ───────────────────────────────────────────────────────────
const SOCKET_URL = 'wss://real.shelteric.com';

const RIDER_TOKEN  = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJpZGVyLTAwMSIsInJvbGUiOiJyaWRlciJ9.pz5qZubhjBOCuM-BwbaImq21Hfm-4Iu_W4NF3JL2_ig';
const DRIVER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImRyaXZlci0wMDEiLCJyb2xlIjoiZHJpdmVyIn0.LlItUpllcsY5aXlIHt9pH8eI7oRzAc-WJoOcIo0H9NY';

let driverReceivedRide = false;
let testTimeout;

// ─── Helpers ──────────────────────────────────────────────────────────────────
function log(tag, ...args) {
  const ts = new Date().toLocaleTimeString('en-IN');
  console.log(`[${ts}] [${tag}]`, ...args);
}

function fail(msg) {
  console.error(`\n❌ TEST FAILED: ${msg}\n`);
  process.exit(1);
}

function pass() {
  console.log(`\n✅ ALL TESTS PASSED — End-to-end ride dispatch is working!\n`);
  process.exit(0);
}

// ─── Step 1: Connect Driver ──────────────────────────────────────────────────
function connectDriver() {
  return new Promise((resolve, reject) => {
    const url = `${SOCKET_URL}?token=${encodeURIComponent(DRIVER_TOKEN)}`;
    log('DRIVER', `Connecting to ${SOCKET_URL}...`);
    
    const ws = new WebSocket(url);

    ws.on('open', () => {
      log('DRIVER', '✅ WebSocket CONNECTED');
      
      // Send auth
      const authMsg = { type: 'auth', role: 'driver', id: 'driver-001' };
      log('DRIVER', 'Sending auth:', JSON.stringify(authMsg));
      ws.send(JSON.stringify(authMsg));
    });

    ws.on('message', (raw) => {
      const data = JSON.parse(raw.toString());
      
      if (data.type === 'auth_success') {
        log('DRIVER', `✅ AUTH SUCCESS — id: ${data.id}, role: ${data.role}`);
        
        // Go available
        const statusMsg = { type: 'driver_status', status: 'available' };
        log('DRIVER', 'Sending driver_status: available');
        ws.send(JSON.stringify(statusMsg));

        // Send location (Hyderabad)
        const locMsg = { type: 'location_update', location: { lat: 17.385, lng: 78.4867 } };
        log('DRIVER', 'Sending location_update:', JSON.stringify(locMsg.location));
        ws.send(JSON.stringify(locMsg));

        resolve(ws);
      } else if (data.type === 'new_ride_request') {
        log('DRIVER', '🎉 ═══ RECEIVED RIDE REQUEST! ═══');
        log('DRIVER', '   Payload:', JSON.stringify(data.payload, null, 2));
        driverReceivedRide = true;
      } else if (data.type === 'demand_heatmap') {
        // Silently ignore periodic heatmap pushes
      } else {
        log('DRIVER', `📩 Received: ${data.type}`, JSON.stringify(data));
      }
    });

    ws.on('error', (err) => {
      log('DRIVER', `❌ ERROR: ${err.message}`);
      reject(err);
    });

    ws.on('close', (code, reason) => {
      log('DRIVER', `Disconnected (code: ${code})`);
    });
  });
}

// ─── Step 2: Connect Rider ───────────────────────────────────────────────────
function connectRider() {
  return new Promise((resolve, reject) => {
    const url = `${SOCKET_URL}?token=${encodeURIComponent(RIDER_TOKEN)}`;
    log('RIDER', `Connecting to ${SOCKET_URL}...`);
    
    const ws = new WebSocket(url);

    ws.on('open', () => {
      log('RIDER', '✅ WebSocket CONNECTED');
      
      // Send auth
      const authMsg = { type: 'auth', role: 'rider', id: 'rider-001' };
      log('RIDER', 'Sending auth:', JSON.stringify(authMsg));
      ws.send(JSON.stringify(authMsg));
    });

    ws.on('message', (raw) => {
      const data = JSON.parse(raw.toString());
      
      if (data.type === 'auth_success') {
        log('RIDER', `✅ AUTH SUCCESS — id: ${data.id}, role: ${data.role}`);
        resolve(ws);
      } else if (data.type === 'nearby_drivers') {
        // Silently ignore periodic broadcasts
      } else {
        log('RIDER', `📩 Received: ${data.type}`, JSON.stringify(data));
      }
    });

    ws.on('error', (err) => {
      log('RIDER', `❌ ERROR: ${err.message}`);
      reject(err);
    });

    ws.on('close', (code, reason) => {
      log('RIDER', `Disconnected (code: ${code})`);
    });
  });
}

// ─── Step 3: Send Ride Request ───────────────────────────────────────────────
function sendRideRequest(riderWs) {
  const ridePayload = {
    type: 'ride_request',
    payload: {
      riderId: 'rider-001',
      pickupLocation: { lat: 17.385, lng: 78.4867 },
      dropLocation: { lat: 17.4401, lng: 78.3489 },
      fare: 120,
      vehicleType: 'Bike',
      riderName: 'Test Rider',
      pickupAddress: 'Charminar, Hyderabad',
      dropAddress: 'HITEC City, Hyderabad',
      distance: 12.5,
    }
  };

  log('RIDER', '🚀 Sending ride_request:', JSON.stringify(ridePayload.payload.pickupAddress), '→', JSON.stringify(ridePayload.payload.dropAddress));
  riderWs.send(JSON.stringify(ridePayload));
}

// ─── Main Test Flow ──────────────────────────────────────────────────────────
async function main() {
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('  E2E WebSocket Dispatch Test');
  console.log(`  Server: ${SOCKET_URL}`);
  console.log('══════════════════════════════════════════════════════════════\n');

  // Global timeout — fail if test takes too long
  testTimeout = setTimeout(() => {
    if (!driverReceivedRide) {
      fail('Timed out after 15s — driver never received the ride request');
    }
  }, 15000);

  try {
    // 1. Connect driver first
    log('TEST', '── Step 1: Connect DRIVER ──');
    const driverWs = await connectDriver();

    // 2. Wait a moment for driver to register
    await new Promise(r => setTimeout(r, 1500));

    // 3. Connect rider
    log('TEST', '── Step 2: Connect RIDER ──');
    const riderWs = await connectRider();

    // 4. Wait a moment for rider to register
    await new Promise(r => setTimeout(r, 1000));

    // 5. Send ride request
    log('TEST', '── Step 3: Send RIDE REQUEST ──');
    sendRideRequest(riderWs);

    // 6. Wait for driver to receive the ride
    log('TEST', '── Step 4: Waiting for driver to receive ride... ──');
    
    // Poll for result
    const checkInterval = setInterval(() => {
      if (driverReceivedRide) {
        clearInterval(checkInterval);
        clearTimeout(testTimeout);

        // Cleanup
        setTimeout(() => {
          riderWs.close();
          driverWs.close();
          setTimeout(pass, 500);
        }, 1000);
      }
    }, 200);

  } catch (err) {
    fail(`Connection error: ${err.message}`);
  }
}

main();
