/**
 * Full Ride Flow Test v2 — with raw message debugging
 */

import WebSocket from 'ws';
import jwt from 'jsonwebtoken';

const SERVER = 'wss://real.shelteric.com';
const JWT_SECRET = '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';

const RIDER_TOKEN = jwt.sign({ id: 'test-rider-flow', role: 'rider' }, JWT_SECRET, { noTimestamp: true });
const DRIVER_TOKEN = jwt.sign({ id: 'ffe12862-83d8-468b-8c56-1481cf18b818', role: 'driver' }, JWT_SECRET, { noTimestamp: true });

const PASS = '\x1b[32m✅ PASS\x1b[0m';
const FAIL = '\x1b[31m❌ FAIL\x1b[0m';
const INFO = '\x1b[36mℹ️ \x1b[0m';

let testsPassed = 0;
let testsFailed = 0;

function assert(condition, testName) {
  if (condition) { console.log(`  ${PASS} ${testName}`); testsPassed++; }
  else { console.log(`  ${FAIL} ${testName}`); testsFailed++; }
}

function waitForMessage(ws, type, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout waiting for "${type}"`)), timeout);
    const handler = (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.type === type) {
          clearTimeout(timer);
          ws.removeListener('message', handler);
          resolve(msg);
        }
      } catch {}
    };
    ws.on('message', handler);
  });
}

function connect(token, label) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${SERVER}?token=${token}`);
    const allMessages = [];

    ws.on('open', () => {
      console.log(`  ${INFO}${label} connected`);
    });

    // Log ALL incoming messages for debugging
    ws.on('message', (raw) => {
      const str = raw.toString();
      allMessages.push(str);
      try {
        const msg = JSON.parse(str);
        console.log(`  ${INFO}${label} ← ${msg.type}${msg.payload ? ' (has payload)' : ''}`);
      } catch {
        console.log(`  ${INFO}${label} ← raw: ${str.substring(0, 100)}`);
      }
    });

    ws.on('close', (code, reason) => {
      console.log(`  ${INFO}${label} Connection Closed: ${code} ${reason}`);
    });

    ws.on('error', (err) => reject(err));

    // Give it a moment to connect and receive initial messages
    setTimeout(() => resolve(ws), 2000);
  });
}

function send(ws, data) {
  ws.send(JSON.stringify(data));
}

async function runTest() {
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('  🚀 FULL RIDE FLOW TEST v2');
  console.log('══════════════════════════════════════════════════════════════\n');

  let driverWs, riderWs;

  try {
    // ── Step 1: Connect ──
    console.log('── Step 1: Connect Driver & Rider ──');
    driverWs = await connect(DRIVER_TOKEN, 'Driver');
    riderWs = await connect(RIDER_TOKEN, 'Rider');
    
    console.log(`  Driver readyState: ${driverWs.readyState} (OPEN is ${WebSocket.OPEN})`);
    console.log(`  Rider readyState: ${riderWs.readyState} (OPEN is ${WebSocket.OPEN})`);
    
    assert(driverWs.readyState === WebSocket.OPEN, 'Driver WebSocket is OPEN');
    assert(riderWs.readyState === WebSocket.OPEN, 'Rider WebSocket is OPEN');

    // Send auth messages
    console.log('\n── Step 1.5: Authenticate ──');
    send(driverWs, { type: 'auth', role: 'driver', vehicleType: 'auto', id: 'ffe12862-83d8-468b-8c56-1481cf18b818' });
    send(riderWs, { type: 'auth', role: 'rider', id: 'test-rider-flow' });

    const driverAuth = await waitForMessage(driverWs, 'auth_success');
    const riderAuth = await waitForMessage(riderWs, 'auth_success');
    assert(driverAuth.type === 'auth_success', 'Driver auth success');
    assert(riderAuth.type === 'auth_success', 'Rider auth success');

    // ── Step 2: Driver goes online ──
    console.log('\n── Step 2: Driver goes online ──');
    send(driverWs, {
      type: 'driver_status',
      status: 'available',
      lat: 17.3850,
      lng: 78.4867,
      vehicleType: 'auto',
    });
    await new Promise(r => setTimeout(r, 1000));
    assert(true, 'Driver status set to "available"');

    send(driverWs, { type: 'location_update', lat: 17.3850, lng: 78.4867 });
    await new Promise(r => setTimeout(r, 500));
    assert(true, 'Driver location updated (Hyderabad)');

    // ── Step 3: Rider sends ride request ──
    console.log('\n── Step 3: Rider sends ride request ──');
    send(riderWs, {
      type: 'ride_request',
      payload: {
        pickupLocation: { lat: 17.3850, lng: 78.4867 },
        dropLocation: { lat: 17.4400, lng: 78.3489 },
        pickupAddress: 'Charminar, Hyderabad',
        dropAddress: 'Gachibowli, Hyderabad',
        vehicleType: 'auto',
        fare: 150,
        distance: '12.5',
        riderName: 'Test Rider',
      }
    });

    const rideRequest = await waitForMessage(driverWs, 'new_ride_request');
    assert(rideRequest.type === 'new_ride_request', 'Driver received new_ride_request');
    assert(rideRequest.payload?.riderId === 'test-rider-flow', 'Correct riderId');
    assert(rideRequest.payload?.fare === 150, 'Correct fare ₹150');

    // ── Step 4: Driver accepts ──
    console.log('\n── Step 4: Driver accepts ride ──');
    send(driverWs, {
      type: 'ride_accept',
      riderId: 'test-rider-flow',
      payload: {
        pickupLocation: { lat: 17.3850, lng: 78.4867 },
        dropLocation: { lat: 17.4400, lng: 78.3489 },
        fare: 150,
        distance: '12.5',
        vehicleType: 'auto',
      },
    });

    const accepted = await waitForMessage(riderWs, 'ride_accepted');
    assert(accepted.type === 'ride_accepted', 'Rider received ride_accepted');
    assert(accepted.payload.driverId === 'ffe12862-83d8-468b-8c56-1481cf18b818', 'Correct driverId');
    assert(accepted.payload?.status === 'accepted', 'Status is "accepted"');
    assert(typeof accepted.payload?.otp === 'string', `OTP generated: ${accepted.payload?.otp}`);

    // ── Step 5: Driver arrived ──
    console.log('\n── Step 5: Trip → "arrived" ──');
    send(driverWs, {
      type: 'trip_status_update',
      riderId: 'test-rider-flow',
      status: 'arrived',
    });
    const driverArrived = await waitForMessage(riderWs, 'trip_status_changed');
    assert(driverArrived.payload.status === 'arrived', 'Status is arrived');

    // ── Step 6: Trip in progress ──
    console.log('\n── Step 6: Trip → "in_progress" ──');
    send(driverWs, {
      type: 'trip_status_update',
      riderId: 'test-rider-flow',
      status: 'in_progress',
    });
    const inProgress = await waitForMessage(riderWs, 'trip_status_changed');
    assert(inProgress.payload?.status === 'in_progress', 'Trip status = "in_progress"');

    // ── Step 7: Trip completed ──
    console.log('\n── Step 7: Trip → "completed" ──');
    send(driverWs, {
      type: 'trip_status_update',
      riderId: 'test-rider-flow',
      status: 'completed',
    });
    const completed = await waitForMessage(riderWs, 'trip_status_changed');
    assert(completed.payload?.status === 'completed', 'Trip status = "completed"');

    // ── Results ──
    console.log('\n══════════════════════════════════════════════════════════════');
    console.log(`  📊 RESULTS: ${testsPassed} passed, ${testsFailed} failed`);
    if (testsFailed === 0) {
      console.log('  🎉 ALL TESTS PASSED! Full ride flow works perfectly!');
    } else {
      console.log('  ⚠️  Some tests failed.');
    }
    console.log('══════════════════════════════════════════════════════════════\n');

  } catch (err) {
    console.error(`\n  ${FAIL} Error: ${err.message}\n`);
    testsFailed++;
  } finally {
    if (driverWs) driverWs.close();
    if (riderWs) riderWs.close();
    setTimeout(() => process.exit(testsFailed > 0 ? 1 : 0), 500);
  }
}

runTest();
