/**
 * ═══════════════════════════════════════════════════════════════════════════
 *  RideGo — Bike vs Auto Load Test
 *  50 Riders (25 bike + 25 auto) + 50 Drivers (25 bike + 25 auto)
 *  Tests: vehicle type matching, cancellations, concurrent requests
 * ═══════════════════════════════════════════════════════════════════════════
 */

import WebSocket from 'ws';
import jwt from 'jsonwebtoken';

const SERVER_URL = 'wss://real.shelteric.com';
const JWT_SECRET = '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';

const NUM_BIKE_RIDERS = 25;
const NUM_AUTO_RIDERS = 25;
const NUM_BIKE_DRIVERS = 25;
const NUM_AUTO_DRIVERS = 25;
const TOTAL_RIDERS = NUM_BIKE_RIDERS + NUM_AUTO_RIDERS;
const TOTAL_DRIVERS = NUM_BIKE_DRIVERS + NUM_AUTO_DRIVERS;
const RUN_ID = Date.now().toString(36);

// ─── Stats ─────────────────────────────────────────────────────────────────
const stats = {
  connected: { riders: 0, drivers: 0 },
  authSuccess: { riders: 0, drivers: 0 },
  bikeRequests: 0,
  autoRequests: 0,
  bikeAccepted: 0,
  autoAccepted: 0,
  bikeDriverGotAutoRequest: 0,  // SHOULD BE 0
  autoDriverGotBikeRequest: 0,  // SHOULD BE 0
  riderCancels: 0,
  driverCancelNotified: 0,
  locationUpdates: 0,
  chatsSent: 0,
  chatsReceived: 0,
  tripCompleted: 0,
  raceConditionBlocked: 0,
  errors: [],
};

function makeToken(id, role) {
  return jwt.sign({ id, role }, JWT_SECRET, { noTimestamp: true });
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomLat() { return 17.35 + Math.random() * 0.15; }
function randomLng() { return 78.35 + Math.random() * 0.2; }

function log(tag, msg) {
  console.log(`[${new Date().toISOString().slice(11, 23)}] [${tag}] ${msg}`);
}
function logError(tag, msg) {
  console.error(`[${new Date().toISOString().slice(11, 23)}] [${tag}] ❌ ${msg}`);
  stats.errors.push(`[${tag}] ${msg}`);
}

// ─── Client ────────────────────────────────────────────────────────────────

class Client {
  constructor(id, role) {
    this.id = id;
    this.role = role;
    this.token = makeToken(id, role);
    this.ws = null;
    this.isConnected = false;
    this.messages = [];
    this.resolvers = {};
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(`${SERVER_URL}?token=${this.token}`, {
        rejectUnauthorized: false, handshakeTimeout: 20000,
      });
      const t = setTimeout(() => reject(new Error(`Timeout ${this.id}`)), 25000);
      this.ws.on('open', () => { clearTimeout(t); this.isConnected = true; resolve(); });
      this.ws.on('message', raw => {
        try {
          const d = JSON.parse(raw.toString());
          this.messages.push(d);
          if (this.resolvers[d.type]) { this.resolvers[d.type](d); delete this.resolvers[d.type]; }
        } catch {}
      });
      this.ws.on('error', e => { clearTimeout(t); reject(e); });
      this.ws.on('close', () => { this.isConnected = false; });
    });
  }

  send(msg) { if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg)); }
  
  waitFor(type, ms = 20000) {
    const existing = this.messages.find(m => m.type === type);
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { delete this.resolvers[type]; reject(new Error(`Timeout ${type} on ${this.id}`)); }, ms);
      this.resolvers[type] = d => { clearTimeout(t); resolve(d); };
    });
  }

  get(type) { return this.messages.filter(m => m.type === type); }
  close() { if (this.ws) this.ws.close(); }

  async auth(vehicleType) {
    const msg = { type: 'auth', id: this.id, role: this.role };
    if (vehicleType) msg.vehicleType = vehicleType;
    this.send(msg);
    return this.waitFor('auth_success');
  }
}

// ─── Test ──────────────────────────────────────────────────────────────────

async function connectBatch(clients, label) {
  const BATCH = 10;
  for (let i = 0; i < clients.length; i += BATCH) {
    const batch = clients.slice(i, i + BATCH);
    await Promise.allSettled(batch.map(c =>
      c.connect().then(() => {
        if (c.role === 'rider') stats.connected.riders++;
        else stats.connected.drivers++;
      }).catch(e => logError('CONNECT', `${c.id}: ${e.message}`))
    ));
    await sleep(300);
  }
  log(label, `Connected: ${stats.connected.riders} riders, ${stats.connected.drivers} drivers`);
}

async function authBatch(clients, vehicleType, label) {
  const BATCH = 10;
  for (let i = 0; i < clients.length; i += BATCH) {
    const batch = clients.slice(i, i + BATCH);
    await Promise.allSettled(batch.map(c =>
      c.auth(vehicleType).then(() => {
        if (c.role === 'rider') stats.authSuccess.riders++;
        else stats.authSuccess.drivers++;
      }).catch(e => logError('AUTH', `${c.id}: ${e.message}`))
    ));
    await sleep(300);
  }
  log(label, `Auth done for ${clients.length} ${vehicleType || ''} ${clients[0]?.role}s`);
}

async function main() {
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('  RideGo — Bike vs Auto Load Test');
  console.log(`  ${TOTAL_RIDERS} Riders (${NUM_BIKE_RIDERS} bike + ${NUM_AUTO_RIDERS} auto)`);
  console.log(`  ${TOTAL_DRIVERS} Drivers (${NUM_BIKE_DRIVERS} bike + ${NUM_AUTO_DRIVERS} auto)`);
  console.log(`  Run: ${RUN_ID}`);
  console.log('══════════════════════════════════════════════════════════════\n');

  const startTime = Date.now();

  // Create clients
  const bikeRiders = Array.from({ length: NUM_BIKE_RIDERS }, (_, i) => new Client(`br-${RUN_ID}-${i}`, 'rider'));
  const autoRiders = Array.from({ length: NUM_AUTO_RIDERS }, (_, i) => new Client(`ar-${RUN_ID}-${i}`, 'rider'));
  const bikeDrivers = Array.from({ length: NUM_BIKE_DRIVERS }, (_, i) => new Client(`bd-${RUN_ID}-${i}`, 'driver'));
  const autoDrivers = Array.from({ length: NUM_AUTO_DRIVERS }, (_, i) => new Client(`ad-${RUN_ID}-${i}`, 'driver'));

  const allRiders = [...bikeRiders, ...autoRiders];
  const allDrivers = [...bikeDrivers, ...autoDrivers];
  const allClients = [...allRiders, ...allDrivers];

  // ═══ PHASE 1: Connect All ═══
  log('PHASE 1', '═══ Connecting all 100 clients in batches ═══');
  await connectBatch(allClients, 'PHASE 1');

  // ═══ PHASE 2: Auth All ═══
  log('PHASE 2', '═══ Authenticating — Bike riders, Auto riders, Bike drivers, Auto drivers ═══');
  await authBatch(bikeRiders, undefined, 'PHASE 2');
  await authBatch(autoRiders, undefined, 'PHASE 2');
  await authBatch(bikeDrivers, 'bike', 'PHASE 2');
  await authBatch(autoDrivers, 'auto', 'PHASE 2');
  log('PHASE 2', `Total auth: ${stats.authSuccess.riders} riders, ${stats.authSuccess.drivers} drivers`);

  // ═══ PHASE 3: Drivers Go Online ═══
  log('PHASE 3', '═══ All drivers going online with locations ═══');
  for (const d of allDrivers) {
    if (!d.isConnected) continue;
    d.send({ type: 'driver_status', status: 'available' });
    d.send({ type: 'location_update', location: { lat: randomLat(), lng: randomLng(), heading: Math.random() * 360 } });
    stats.locationUpdates++;
  }
  await sleep(2000);
  log('PHASE 3', `${allDrivers.filter(d => d.isConnected).length} drivers online`);

  // ═══ PHASE 4: Bike Ride Requests ═══
  log('PHASE 4', '═══ 25 BIKE riders requesting rides ═══');
  for (const r of bikeRiders) {
    if (!r.isConnected) continue;
    r.send({
      type: 'ride_request',
      payload: {
        pickupLocation: { lat: randomLat(), lng: randomLng() },
        dropLocation: { lat: randomLat(), lng: randomLng() },
        fare: 30 + Math.floor(Math.random() * 100),
        vehicleType: 'bike',
        riderName: `BikeRider_${r.id}`,
        distance: (1.5 + Math.random() * 8).toFixed(1),
        pickupAddress: `Bike Pickup ${r.id}`,
        dropAddress: `Bike Drop ${r.id}`,
      },
    });
    stats.bikeRequests++;
    await sleep(100);
  }
  await sleep(3000);

  // ═══ CHECK: Did auto drivers receive bike requests? (SHOULD NOT) ═══
  log('CHECK 1', '═══ Verifying vehicle type isolation: Auto drivers should NOT get bike requests ═══');
  for (const d of autoDrivers) {
    const bikeReqs = d.get('new_ride_request').filter(m => {
      const vt = (m.payload?.vehicleType || '').toLowerCase();
      return vt === 'bike' || (!vt.includes('auto'));
    });
    stats.autoDriverGotBikeRequest += bikeReqs.length;
  }
  if (stats.autoDriverGotBikeRequest === 0) {
    log('CHECK 1', '✅ PASSED — Auto drivers received 0 bike requests (correct!)');
  } else {
    logError('CHECK 1', `FAILED — Auto drivers received ${stats.autoDriverGotBikeRequest} bike requests!`);
  }

  // Verify bike drivers DID receive bike requests
  let bikeDriversWithRequests = 0;
  for (const d of bikeDrivers) {
    if (d.get('new_ride_request').length > 0) bikeDriversWithRequests++;
  }
  log('CHECK 1', `Bike drivers that received requests: ${bikeDriversWithRequests}/${bikeDrivers.filter(d => d.isConnected).length}`);

  // ═══ PHASE 5: Auto Ride Requests ═══
  log('PHASE 5', '═══ 25 AUTO riders requesting rides ═══');
  for (const r of autoRiders) {
    if (!r.isConnected) continue;
    r.send({
      type: 'ride_request',
      payload: {
        pickupLocation: { lat: randomLat(), lng: randomLng() },
        dropLocation: { lat: randomLat(), lng: randomLng() },
        fare: 50 + Math.floor(Math.random() * 150),
        vehicleType: 'auto',
        riderName: `AutoRider_${r.id}`,
        distance: (2 + Math.random() * 10).toFixed(1),
        pickupAddress: `Auto Pickup ${r.id}`,
        dropAddress: `Auto Drop ${r.id}`,
      },
    });
    stats.autoRequests++;
    await sleep(100);
  }
  await sleep(3000);

  // ═══ CHECK: Did bike drivers receive auto requests? (SHOULD NOT) ═══
  log('CHECK 2', '═══ Verifying vehicle type isolation: Bike drivers should NOT get auto requests ═══');
  for (const d of bikeDrivers) {
    const autoReqs = d.get('new_ride_request').filter(m => {
      const vt = (m.payload?.vehicleType || '').toLowerCase();
      return vt.includes('auto');
    });
    stats.bikeDriverGotAutoRequest += autoReqs.length;
  }
  if (stats.bikeDriverGotAutoRequest === 0) {
    log('CHECK 2', '✅ PASSED — Bike drivers received 0 auto requests (correct!)');
  } else {
    logError('CHECK 2', `FAILED — Bike drivers received ${stats.bikeDriverGotAutoRequest} auto requests!`);
  }

  let autoDriversWithRequests = 0;
  for (const d of autoDrivers) {
    if (d.get('new_ride_request').filter(m => (m.payload?.vehicleType || '').toLowerCase().includes('auto')).length > 0) autoDriversWithRequests++;
  }
  log('CHECK 2', `Auto drivers that received auto requests: ${autoDriversWithRequests}/${autoDrivers.filter(d => d.isConnected).length}`);

  // ═══ PHASE 6: Bike drivers accept bike rides ═══
  log('PHASE 6', '═══ Bike drivers accepting bike rides (1:1 pairing) ═══');
  const bikeAccepted = new Set();
  for (const d of bikeDrivers) {
    const reqs = d.get('new_ride_request');
    for (const req of reqs) {
      const rid = req.payload?.riderId;
      if (rid && !bikeAccepted.has(rid)) {
        bikeAccepted.add(rid);
        d.send({ type: 'ride_accept', riderId: rid, payload: req.payload });
        stats.bikeAccepted++;
        await sleep(150);
        break;
      }
    }
  }
  log('PHASE 6', `Bike rides accepted: ${stats.bikeAccepted}`);

  // ═══ PHASE 7: Auto drivers accept auto rides ═══
  log('PHASE 7', '═══ Auto drivers accepting auto rides (1:1 pairing) ═══');
  const autoAccepted = new Set();
  for (const d of autoDrivers) {
    const reqs = d.get('new_ride_request').filter(m => (m.payload?.vehicleType || '').toLowerCase().includes('auto'));
    for (const req of reqs) {
      const rid = req.payload?.riderId;
      if (rid && !autoAccepted.has(rid)) {
        autoAccepted.add(rid);
        d.send({ type: 'ride_accept', riderId: rid, payload: req.payload });
        stats.autoAccepted++;
        await sleep(150);
        break;
      }
    }
  }
  log('PHASE 7', `Auto rides accepted: ${stats.autoAccepted}`);
  await sleep(2000);

  // Verify riders got ride_accepted
  let bikeConfirmed = 0, autoConfirmed = 0;
  for (const r of bikeRiders) { if (r.get('ride_accepted').length > 0) bikeConfirmed++; }
  for (const r of autoRiders) { if (r.get('ride_accepted').length > 0) autoConfirmed++; }
  log('PHASE 7', `Confirmed on rider side: ${bikeConfirmed} bike, ${autoConfirmed} auto`);

  // ═══ PHASE 8: Rider Cancellations ═══
  log('PHASE 8', '═══ 10 riders cancelling (5 bike + 5 auto) ═══');
  for (let i = 0; i < 5; i++) {
    if (bikeRiders[i]?.isConnected) {
      bikeRiders[i].send({ type: 'ride_cancel' });
      stats.riderCancels++;
    }
    if (autoRiders[i]?.isConnected) {
      autoRiders[i].send({ type: 'ride_cancel' });
      stats.riderCancels++;
    }
  }
  await sleep(2000);

  // Check if paired drivers got cancellation notification
  for (const d of allDrivers) {
    const cancelMsgs = d.get('trip_status_changed').filter(m => m.payload?.status === 'cancelled');
    stats.driverCancelNotified += cancelMsgs.length;
  }
  log('PHASE 8', `Riders cancelled: ${stats.riderCancels}, Drivers notified: ${stats.driverCancelNotified}`);

  // ═══ PHASE 9: Chat between remaining active pairs ═══
  log('PHASE 9', '═══ Chat messages between active ride pairs ═══');
  for (let i = 5; i < 15; i++) {
    const rider = bikeRiders[i];
    const driver = bikeDrivers[i];
    if (rider?.isConnected && driver?.isConnected) {
      rider.send({ type: 'chat_message', to: driver.id, message: `Bike rider ${i} here!` });
      driver.send({ type: 'chat_message', to: rider.id, message: `Bike driver ${i} coming!` });
      stats.chatsSent += 2;
    }
  }
  for (let i = 5; i < 15; i++) {
    const rider = autoRiders[i];
    const driver = autoDrivers[i];
    if (rider?.isConnected && driver?.isConnected) {
      rider.send({ type: 'chat_message', to: driver.id, message: `Auto rider ${i} here!` });
      driver.send({ type: 'chat_message', to: rider.id, message: `Auto driver ${i} on the way!` });
      stats.chatsSent += 2;
    }
  }
  await sleep(2000);

  for (let i = 5; i < 15; i++) {
    stats.chatsReceived += (bikeRiders[i]?.get('CHAT_MESSAGE').length || 0);
    stats.chatsReceived += (bikeDrivers[i]?.get('CHAT_MESSAGE').length || 0);
    stats.chatsReceived += (autoRiders[i]?.get('CHAT_MESSAGE').length || 0);
    stats.chatsReceived += (autoDrivers[i]?.get('CHAT_MESSAGE').length || 0);
  }
  log('PHASE 9', `Chats sent: ${stats.chatsSent}, received: ${stats.chatsReceived}`);

  // ═══ PHASE 10: Trip completions ═══
  log('PHASE 10', '═══ Completing 10 trips (5 bike + 5 auto) ═══');
  for (let i = 10; i < 15; i++) {
    const bd = bikeDrivers[i];
    const bikeReqs = bd?.get('new_ride_request');
    if (bikeReqs?.length > 0) {
      const rid = bikeReqs[0].payload?.riderId;
      if (rid) {
        bd.send({ type: 'trip_status_update', riderId: rid, status: 'arrived' });
        await sleep(200);
        bd.send({ type: 'trip_status_update', riderId: rid, status: 'in_progress' });
        await sleep(200);
        bd.send({ type: 'trip_status_update', riderId: rid, status: 'completed' });
        stats.tripCompleted++;
        await sleep(200);
      }
    }

    const ad = autoDrivers[i];
    const autoReqs = ad?.get('new_ride_request');
    if (autoReqs?.length > 0) {
      const rid = autoReqs[0].payload?.riderId;
      if (rid) {
        ad.send({ type: 'trip_status_update', riderId: rid, status: 'arrived' });
        await sleep(200);
        ad.send({ type: 'trip_status_update', riderId: rid, status: 'in_progress' });
        await sleep(200);
        ad.send({ type: 'trip_status_update', riderId: rid, status: 'completed' });
        stats.tripCompleted++;
        await sleep(200);
      }
    }
  }
  log('PHASE 10', `Trips completed: ${stats.tripCompleted}`);

  // ═══ PHASE 11: Location bursts from all drivers ═══
  log('PHASE 11', '═══ All 50 drivers sending location updates ═══');
  for (let round = 0; round < 3; round++) {
    for (const d of allDrivers) {
      if (!d.isConnected) continue;
      d.send({ type: 'location_update', location: { lat: randomLat(), lng: randomLng(), heading: Math.random() * 360 } });
      stats.locationUpdates++;
    }
    await sleep(300);
  }
  log('PHASE 11', `Total location updates: ${stats.locationUpdates}`);

  // ═══ PHASE 12: Cleanup ═══
  log('PHASE 12', '═══ Graceful disconnect ═══');
  for (const d of allDrivers) { if (d.isConnected) d.send({ type: 'driver_status', status: 'offline' }); }
  await sleep(500);
  for (const c of allClients) c.close();
  await sleep(1000);
  log('PHASE 12', 'All disconnected');

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  // ═══ REPORT ═══
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('  BIKE vs AUTO LOAD TEST RESULTS');
  console.log('══════════════════════════════════════════════════════════════');
  console.log(`  Duration:                   ${duration}s`);
  console.log(`  ─────────────────────────────────────────────`);
  console.log(`  Connections:                ${stats.connected.riders} riders, ${stats.connected.drivers} drivers`);
  console.log(`  Auth Success:               ${stats.authSuccess.riders} riders, ${stats.authSuccess.drivers} drivers`);
  console.log(`  ─────────────────────────────────────────────`);
  console.log(`  BIKE requests sent:         ${stats.bikeRequests}`);
  console.log(`  AUTO requests sent:         ${stats.autoRequests}`);
  console.log(`  BIKE rides accepted:        ${stats.bikeAccepted}`);
  console.log(`  AUTO rides accepted:        ${stats.autoAccepted}`);
  console.log(`  ─────────────────────────────────────────────`);
  console.log(`  🔒 VEHICLE TYPE ISOLATION:`);
  console.log(`    Auto drivers got bike req: ${stats.autoDriverGotBikeRequest} ${stats.autoDriverGotBikeRequest === 0 ? '✅' : '❌ LEAK!'}`);
  console.log(`    Bike drivers got auto req: ${stats.bikeDriverGotAutoRequest} ${stats.bikeDriverGotAutoRequest === 0 ? '✅' : '❌ LEAK!'}`);
  console.log(`  ─────────────────────────────────────────────`);
  console.log(`  Rider cancellations:        ${stats.riderCancels}`);
  console.log(`  Driver cancel notified:     ${stats.driverCancelNotified}`);
  console.log(`  Trips completed:            ${stats.tripCompleted}`);
  console.log(`  Chat sent/received:         ${stats.chatsSent} / ${stats.chatsReceived}`);
  console.log(`  Location updates:           ${stats.locationUpdates}`);
  console.log(`  ─────────────────────────────────────────────`);

  if (stats.errors.length > 0) {
    console.log(`  ❌ ERRORS (${stats.errors.length}):`);
    stats.errors.forEach((e, i) => console.log(`     ${i + 1}. ${e}`));
  } else {
    console.log(`  ✅ ALL TESTS PASSED!`);
  }
  console.log('══════════════════════════════════════════════════════════════\n');

  process.exit(stats.errors.length > 0 ? 1 : 0);
}

main();
