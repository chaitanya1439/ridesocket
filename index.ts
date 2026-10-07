// NOTE: The expo-server-sdk uses `import { fetch } from 'undici'` internally,
// NOT `globalThis.fetch`. The Node built-in dispatcher incompatibility is fixed
// by passing an explicit undici Agent as `httpAgent` in pushService.ts.


import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import cors from 'cors';
import jwt from 'jsonwebtoken';
import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';
import Redis from 'ioredis';
import crypto from 'crypto';

const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:RidegoPassword123!@ridego-db.cmbwkyg28hi2.us-east-1.rds.amazonaws.com:5432/postgres';
const pool = new pg.Pool({ 
  connectionString,
  ssl: { rejectUnauthorized: false },
  max: 20, // Increased pool size for concurrent driver auths
  idleTimeoutMillis: 30000,
});
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter } as any);
const redis = new (Redis as any)(process.env.REDIS_URL || 'redis://localhost:6379');

async function getActiveTrip(riderId: string): Promise<TripRecord | undefined> {
  const data = await redis.get(`trip:${riderId}`);
  return data ? JSON.parse(data) : undefined;
}
async function setActiveTrip(riderId: string, trip: TripRecord) {
  await redis.set(`trip:${riderId}`, JSON.stringify(trip));
  // Reverse index: driver → rider mapping for O(1) lookups
  if (trip.driverId) {
    await redis.set(`drivertrip:${trip.driverId}`, riderId);
  }
}
async function deleteActiveTrip(riderId: string) {
  // Clean up reverse index before deleting the trip
  const tripData = await redis.get(`trip:${riderId}`);
  if (tripData) {
    try {
      const trip = JSON.parse(tripData);
      if (trip.driverId) {
        await redis.del(`drivertrip:${trip.driverId}`);
      }
    } catch { /* ignore parse errors */ }
  }
  await redis.del(`trip:${riderId}`);
}
/** O(1) lookup: find the active trip for a given driver */
async function getDriverActiveTrip(driverId: string): Promise<TripRecord | undefined> {
  const riderId = await redis.get(`drivertrip:${driverId}`);
  if (!riderId) return undefined;
  return getActiveTrip(riderId);
}
async function getAllActiveTrips(): Promise<[string, TripRecord][]> {
  const keys = await redis.keys('trip:*');
  if (!keys.length) return [];
  const vals = await redis.mget(keys);
  return keys.map((k: string, i: number) => [k.replace('trip:', ''), vals[i] ? JSON.parse(vals[i]!) : null]).filter((x: any) => x[1]);
}

async function getPendingRequest(riderId: string): Promise<any> {
  const data = await redis.get(`req:${riderId}`);
  return data ? JSON.parse(data) : undefined;
}
async function setPendingRequest(riderId: string, req: any) {
  await redis.set(`req:${riderId}`, JSON.stringify(req), 'EX', 300);
}
async function deletePendingRequest(riderId: string) {
  await redis.del(`req:${riderId}`);
}
async function getAllPendingRequests(): Promise<[string, any][]> {
  const keys = await redis.keys('req:*');
  if (!keys.length) return [];
  const vals = await redis.mget(keys);
  return keys.map((k: string, i: number) => [k.replace('req:', ''), vals[i] ? JSON.parse(vals[i]!) : null]).filter((x: any) => x[1]);
}


import type {
  ClientInfo,
  DecodedToken,
  DriverStatus,
  TripRecord,
  TripStatus,
  Location,
  Hotspot,
  InboundMessage,
  RideRequestPayload,
  RideRejectMessage,
  ForceLogoutMessage,
} from './types.js';

import {
  registerPushToken,
  unregisterPushToken,
  sendPushNotification,
  notifyDriverOfRideRequest,
  notifyRiderOfAcceptance,
  notifyTripStatusChange,
  getPushToken,
} from './pushService.js';
import { setupOcrRoutes } from './ocrService.js';

// ─── JWT Secret ───────────────────────────────────────────────────────────────

const JWT_SECRET = process.env.JWT_SECRET ?? '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';

// ─── Fixed Tokens (.env lo define chesukoni ikkade generate avutayi) ──────────
// .env lo RIDER_TOKEN_ID, DRIVER_TOKEN_ID set cheyandi — never expire avutayi.
// Same secret + same id = always same token. Server restart chesina same token vastundi.

const RIDER_TOKEN_ID  = process.env.RIDER_TOKEN_ID  ?? 'rider-001';
const DRIVER_TOKEN_ID = process.env.DRIVER_TOKEN_ID ?? 'driver-001';

const FIXED_TOKENS = {
  rider:  jwt.sign({ id: RIDER_TOKEN_ID,  role: 'rider'  }, JWT_SECRET, { noTimestamp: true }),
  driver: jwt.sign({ id: DRIVER_TOKEN_ID, role: 'driver' }, JWT_SECRET, { noTimestamp: true }),
};

const app = express();
app.use(cors());
app.use(express.json());

// Setup OCR endpoints
setupOcrRoutes(app);

// Simple healthcheck
app.get('/', (req, res) => res.send('Realtime Server Active'));

// Subscription Purchase Endpoint
app.post('/buy-subscription', async (req, res) => {
  const { driverId } = req.body;
  if (!driverId) {
    return res.status(400).json({ success: false, error: 'driverId is required' });
  }

  try {
    const expiryDate = new Date();
    expiryDate.setDate(expiryDate.getDate() + 2); // 2 Days Plan

    await prisma.user.update({
      where: { userId: driverId },
      data: {
        subscriptionStatus: 'active',
        subscriptionExpiry: expiryDate
      }
    });
    
    console.log(`[Subscription API] Driver ${driverId} purchased subscription successfully.`);
    return res.json({ success: true, message: 'Subscription updated' });
  } catch (error: any) {
    console.error('[Subscription API] Error updating DB:', error);
    return res.status(500).json({ success: false, error: 'Failed to update subscription in database' });
  }
});

// History API Endpoints
app.get('/api/rider/history/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const trips = await prisma.trip.findMany({
      where: { riderId: userId, status: { in: ['completed', 'cancelled', 'scheduled'] } },
      orderBy: { createdAt: 'desc' },
      take: 20
    });
    
    // Map to the frontend expected format
    const formatted = trips.map((t: any) => {
      const displayDate = t.scheduledTime ? new Date(t.scheduledTime) : new Date(t.createdAt);
      return {
        id: t.id,
        vehicle: t.vehicleType || "Bike",
        status: t.status,
        date: displayDate.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' }),
        time: displayDate.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }),
        pickup: t.pickupAddress || "Pickup Location",
        drop: t.dropAddress || "Drop Location",
        fare: t.fare || 0,
      };
    });
    
    res.json(formatted);
  } catch (error) {
    console.error('[History API] Rider error:', error);
    res.status(500).json([]);
  }
});

app.get('/api/driver/history/:driverId', async (req, res) => {
  try {
    const { driverId } = req.params;
    const trips = await prisma.trip.findMany({
      where: { driverId: driverId, status: { in: ['completed', 'cancelled'] } },
      orderBy: { createdAt: 'desc' },
      take: 20
    });
    
    // Map to the frontend expected format
    const formatted = trips.map((t: any) => ({
      id: t.id,
      date: new Date(t.createdAt).toLocaleString('en-IN', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' }),
      pickup: t.pickupAddress || "Pickup Location",
      drop: t.dropAddress || "Drop Location", 
      fare: t.fare || 0,
      status: t.status,
      timestamp: new Date(t.createdAt).getTime()
    }));
    
    res.json(formatted);
  } catch (error) {
    console.error('[History API] Driver error:', error);
    res.status(500).json([]);
  }
});

app.get('/api/rider/stats/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    const [ridesCount, moneySaved, parcelsCount] = await Promise.all([
      prisma.trip.count({ where: { riderId: userId, status: 'completed' } }),
      prisma.trip.aggregate({ _sum: { fare: true }, where: { riderId: userId, status: 'completed' } }),
      prisma.trip.count({ where: { riderId: userId, vehicleType: 'Parcel' } }) // Assuming Parcel is a vehicleType or similar
    ]);

    // Format money saved logic or apply 10% assumption if you want
    const saved = moneySaved._sum.fare ? Number((moneySaved._sum.fare * 0.1).toFixed(2)) : 0; 
    
    res.json({ success: true, stats: { rides: ridesCount, saved, parcels: parcelsCount } });
  } catch (error: any) {
    console.error('[Stats API] Error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch stats' });
  }
});

app.get('/api/driver/stats/:driverId', async (req, res) => {
  try {
    const { driverId } = req.params;
    
    // Get start of today
    const startOfToday = new Date();
    startOfToday.setHours(0, 0, 0, 0);

    const [driver, totalRides, todayTrips] = await Promise.all([
      prisma.user.findUnique({ where: { userId: driverId } }),
      prisma.trip.count({ where: { driverId, status: 'completed' } }),
      prisma.trip.findMany({
        where: { 
          driverId, 
          status: 'completed',
          createdAt: { gte: startOfToday } 
        },
        select: { fare: true }
      })
    ]);

    const todayEarnings = todayTrips.reduce((sum: number, trip: any) => sum + (trip.fare || 0), 0);

    res.json({
      totalRides,
      todayEarnings,
      subscriptionStatus: driver?.subscriptionStatus || 'Inactive',
      subscriptionExpiry: driver?.subscriptionExpiry || null
    });
  } catch (error) {
    console.error('[Stats API] Driver error:', error);
    res.status(500).json({ error: 'Failed to fetch driver stats' });
  }
});

// --- Background Location Update API ---
app.post('/api/location/update', async (req, res) => {
  try {
    const { driverId, location, riderId } = req.body;
    if (!driverId || !location) {
      return res.status(400).json({ error: 'driverId and location are required' });
    }
    
    // Update Redis
    await redis.geoadd('driver_locations', location.lng, location.lat, driverId);
    
    // Find target rider
    let targetRiderId = riderId;
    if (!targetRiderId) {
      targetRiderId = await redis.get(`drivertrip:${driverId}`);
    }
    
    // Forward to rider via WebSocket
    if (targetRiderId) {
      const targetRider = riders.get(targetRiderId);
      if (targetRider?.ws.readyState === WebSocket.OPEN) {
        targetRider.ws.send(JSON.stringify({
          type: 'driver_location',
          payload: { driverId, location }
        }));
      }
    }
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: 'Failed to update location' });
  }
});

const server = createServer(app);

// 1. Bandwidth Efficiency: Enable per-message deflate compression.
const wss = new WebSocketServer({
  noServer: true, // We manually handle the HTTP upgrade for authentication
  perMessageDeflate: {
    zlibDeflateOptions: { level: 4 }, // Balance CPU vs compression ratio
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    threshold: 256, // Only compress messages larger than 256 bytes
  },
});

// ─── Utility: augment ws instances with clientInfo ────────────────────────────

/** Retrieves the bound ClientInfo from a WebSocket instance, or undefined. */
function getClientInfo(ws: WebSocket): ClientInfo | undefined {
  return (ws as WebSocket & { clientInfo?: ClientInfo }).clientInfo;
}

/** Binds a ClientInfo to a WebSocket instance. */
function setClientInfo(ws: WebSocket, info: ClientInfo): void {
  (ws as WebSocket & { clientInfo?: ClientInfo }).clientInfo = info;
}

// ─── Geospatial Proximity (Haversine) ────────────────────────────────────────

/** Returns the great-circle distance between two coordinates in kilometres. */
function getDistanceInKm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ─── In-memory state ──────────────────────────────────────────────────────────

const riders = new Map<string, ClientInfo>();
const drivers = new Map<string, ClientInfo>();
const activeDevices = new Map<string, ClientInfo>();

/**
 * 2. Active State Recovery (Moved to Redis)
 * `activeTrips` will now use `trip:<riderId>` in Redis.
 */
// const activeTrips = new Map<string, TripRecord>();

/**
 * Stores pending ride requests (Moved to Redis)
 * `pendingRequests` will use `request:<riderId>` in Redis.
 */
// const pendingRequests = new Map<string, RideRequestPayload & { timestamp: number; riderId: string }>();

const MAX_DRIVER_MATCH_DISTANCE_KM = Number(
  process.env.MAX_DRIVER_MATCH_DISTANCE_KM ?? 30,
);

// ─── Heartbeat & Idle Pruning ─────────────────────────────────────────────────

const IDLE_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes

const heartbeatInterval = setInterval(async () => {
  const now = Date.now();

  wss.clients.forEach((ws) => {
    const client = getClientInfo(ws);
    if (!client) {
      // Unauthenticated socket — terminate
      ws.terminate();
      return;
    }

    // 1. Silent-drop detection via ping/pong
    if (!client.isAlive) {
      console.log(`[Heartbeat] Terminating inactive WS for ${client.role} ${client.id}`);
      if (client.role === 'rider') {
        riders.delete(client.id);
      }
      ws.terminate();
      return;
    }

    // 2. Idle pruning: disconnect riders idle for 15+ minutes to save memory
    //    Drivers are kept alive while 'available' or 'busy'.
    if (client.role === 'rider' && now - client.lastActivity > IDLE_TIMEOUT_MS) {
      console.log(`[Idle Prune] Disconnecting idle rider ${client.id}`);
      riders.delete(client.id);
      ws.close(1000, 'Idle timeout');
      return;
    }

    client.isAlive = false;
    ws.ping();
  });

  // Prune expired pending requests (older than 60 seconds)
  for (const [riderId, req] of await getAllPendingRequests()) {
    if (now - req.timestamp > 60_000) {
      await deletePendingRequest(riderId);
    }
  }

  // 3. Garbage collect disconnected drivers who haven't been active for 12 hours
  const TWELVE_HOURS_MS = 12 * 60 * 60 * 1000;
  drivers.forEach((driver, id) => {
    if (driver.ws.readyState === WebSocket.CLOSED && (now - driver.lastActivity > TWELVE_HOURS_MS)) {
      console.log(`[Heartbeat] Pruning stale offline driver ${id}`);
      drivers.delete(id);
    }
  });
}, 30_000);

// ─── Demand Heatmap Push ─────────────────────────────────────────────────────

/**
 * 3. Fleet Optimization
 * Push surge/hotspot data to all online drivers every minute instead of
 * having thousands of drivers HTTP-poll on a timer.
 */
const MOCK_HOTSPOTS: Hotspot[] = [
  { lat: 17.4401, lng: 78.3489, intensity: 0.9, surge: 1.5 }, // HITEC City
  { lat: 17.385, lng: 78.4867, intensity: 0.6, surge: 1.2 },
];

const heatmapInterval = setInterval(() => {
  const payload = JSON.stringify({ type: 'demand_heatmap', payload: MOCK_HOTSPOTS });
  let pushedCount = 0;

  drivers.forEach((driver) => {
    if (driver.ws.readyState === WebSocket.OPEN) {
      driver.ws.send(payload);
      pushedCount++;
    }
  });

  if (pushedCount > 0) {
    console.log(`[Fleet Optimization] Pushed demand heatmap to ${pushedCount} drivers`);
  }
}, 60_000);

wss.on('close', () => {
  clearInterval(heartbeatInterval);
  clearInterval(heatmapInterval);
});

// ─── HTTP Upgrade Authentication ──────────────────────────────────────────────

/**
 * 5. Connection Security
 * Reject unauthenticated requests before they become WebSocket connections.
 */
server.on('upgrade', (request, socket, head) => {
  const clientIp = request.socket.remoteAddress;
  console.log(`\n[Auth] ═══ WebSocket Upgrade Request ═══`);
  console.log(`[Auth] Client IP: ${clientIp}`);
  console.log(`[Auth] URL: ${request.url}`);

  try {
    const url = new URL(request.url ?? '', `http://${request.headers.host}`);
    const token = url.searchParams.get('token');

    if (!token) {
      console.log(`[Auth] ✗ REJECTED — No token provided`);
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    console.log(`[Auth] Token received: ${token.substring(0, 30)}...`);

    let decoded: DecodedToken;
    try {
      decoded = jwt.verify(token, JWT_SECRET, { ignoreExpiration: true }) as DecodedToken;
      console.log(`[Auth] ✓ Token VERIFIED — id: ${decoded.id ?? decoded.userId}, role: ${decoded.role}`);
    } catch (err: any) {
      console.log(`[Auth] ✗ REJECTED — Token verification failed: ${err.message}`);
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request, decoded);
    });
  } catch (err: any) {
    console.log(`[Auth] ✗ REJECTED — Unexpected error: ${err.message}`);
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
  }
});

// ─── WebSocket Connection Handler ────────────────────────────────────────────

wss.on('connection', (ws: WebSocket, _request: unknown, decodedToken: DecodedToken) => {
  // Support both `id` and `userId` JWT schemas
  const tokenUserId = decodedToken?.id ?? decodedToken?.userId;
  console.log(`[Auth] ✓ WebSocket CONNECTED — tokenUserId: ${tokenUserId}, role: ${decodedToken?.role}`);

  // React Native automatically replies to native ping frames with a pong.
  ws.on('pong', () => {
    const client = getClientInfo(ws);
    if (client) client.isAlive = true;
  });

  ws.on('message', async (raw: Buffer | string) => {
    let data: InboundMessage;

    try {
      data = JSON.parse(raw.toString()) as InboundMessage;
    } catch (err) {
      console.error('[WS] Failed to parse message:', err);
      return;
    }

    // Update activity timestamp on every message
    const client = getClientInfo(ws);
    if (client) {
      client.lastActivity = Date.now();
      client.isAlive = true;
    }

    switch (data.type) {
      // ── Auth ───────────────────────────────────────────────────────────────
      case 'auth': {
        const clientId = data.id ?? tokenUserId ?? '';
        const existingDriver = data.role === 'driver' ? drivers.get(clientId) : undefined;

        console.log(`[Auth] 'auth' message received — role: ${data.role}, clientId: ${clientId}, tokenUserId: ${tokenUserId}`);

        const newClient: ClientInfo = {
          ws,
          role: data.role as 'rider' | 'driver',
          id: clientId,
          isAlive: true,
          lastActivity: Date.now(),
          ...(data.deviceId ? { deviceId: data.deviceId } : {}),
          ...(data.vehicleType ? { vehicleType: data.vehicleType } : {}),
        };

        if (data.deviceId) {
          const existingDevice = activeDevices.get(data.deviceId);
          if (existingDevice && existingDevice.ws !== ws) {
            console.log(`[Auth] Device Exclusivity: Forcing logout for existing session on device ${data.deviceId}`);
            const logoutMsg: ForceLogoutMessage = { type: 'force_logout', reason: 'logged_in_elsewhere' };
            try {
              existingDevice.ws.send(JSON.stringify(logoutMsg));
              // Give it a moment to send the message before closing
              setTimeout(() => {
                 if (existingDevice.ws.readyState === WebSocket.OPEN) {
                   existingDevice.ws.close();
                 }
              }, 100);
            } catch (e) {
              // Ignore send errors on dead sockets
            }
          }
          activeDevices.set(data.deviceId, newClient);
        }

        if (data.role === 'driver') {
          // Register driver in memory FIRST + send auth_success immediately
          // so the client doesn't timeout while we do async DB/Redis work.
          newClient.status = existingDriver?.status ?? 'available';
          if (existingDriver?.lastLocation) {
            newClient.lastLocation = existingDriver.lastLocation;
          }

          setClientInfo(ws, newClient);
          drivers.set(newClient.id, newClient);
          console.log(`[Auth] ✓ Driver REGISTERED in memory — id: ${newClient.id}, status: ${newClient.status}`);
          console.log(`[Auth]   Total drivers online: ${drivers.size}`);

          // Send auth_success immediately (non-blocking)
          ws.send(JSON.stringify({ type: 'auth_success', id: newClient.id, role: data.role }));

          // Background: DB subscription check + trip sync (non-blocking)
          (async () => {
            try {
              // DB subscription & gender check (fire-and-forget, don't block auth)
              if (clientId !== 'ffe12862-83d8-468b-8c56-1481cf18b818') {
                prisma.user.findUnique({
                  where: { userId: clientId }
                }).then((user: any) => {
                  if (user && user.gender) {
                    newClient.gender = user.gender.toLowerCase();
                    console.log(`[Auth] Set gender ${newClient.gender} for driver ${newClient.id}`);
                  }
                }).catch((e: any) => console.error(`[Auth] DB lookup failed for ${clientId}:`, e));
              }

              // Offline Recovery Sync — O(1) using reverse index instead of scanning all trips
              const currentTrip = await getDriverActiveTrip(newClient.id);
              if (currentTrip && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'sync_state', payload: currentTrip }));
                console.log(`Synced active state to reconnecting driver ${newClient.id}`);
              }

              // Deliver pending ride requests to reconnecting/newly-online drivers
              if ((newClient as any).status !== 'offline') {
                for (const [riderId, req] of await getAllPendingRequests()) {
                  if (Date.now() - req.timestamp <= 60_000 && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                      type: 'new_ride_request',
                      payload: { ...req, riderId }
                    }));
                  }
                }
              }

            } catch (err) {
              console.error(`[Auth] Background sync error for ${clientId}:`, err);
            }
          })();
          
          break; // Exit the switch statement for driver
        }

        // ---- Rider Registration Flow ----
        setClientInfo(ws, newClient);
        riders.set(newClient.id, newClient);
        console.log(`[Auth] ✓ Rider REGISTERED in memory — id: ${newClient.id}`);
        console.log(`[Auth]   Total riders online: ${riders.size}`);

        ws.send(JSON.stringify({ type: 'auth_success', id: newClient.id, role: data.role }));

        // Offline Recovery Sync for Rider
        const currentTrip = (await getActiveTrip(newClient.id));
        if (currentTrip) {
          ws.send(JSON.stringify({ type: 'sync_state', payload: currentTrip }));
          console.log(`Synced active state to reconnecting rider ${newClient.id}`);
        }

        break;
      }

      // ── Driver status ──────────────────────────────────────────────────────
      case 'driver_status': {
        if (client?.role === 'driver') {
          const statusMap: Record<string, DriverStatus> = {
            available: 'available',
            busy: 'busy',
            offline: 'offline',
          };
          client.status = statusMap[data.status] ?? 'offline';
          console.log(`Driver ${client.id} is now ${client.status}`);

          if (client.status === 'available') {
            // Send any pending ride requests to newly available drivers
            for (const [riderId, req] of await getAllPendingRequests()) {
              if (Date.now() - req.timestamp <= 60_000) {
                ws.send(JSON.stringify({
                  type: 'new_ride_request',
                  payload: { ...req, riderId }
                }));
              }
            }
          }
        }
        break;
      }

      // ── Ride request ───────────────────────────────────────────────────────
      case 'ride_request': {
        if (!client) break;

        // Support both nested `payload` and legacy flat fields.
        // With exactOptionalPropertyTypes:true, we must not set a key to `undefined` —
        // only include fields that are actually present in the message.
        const ridePayload: RideRequestPayload = data.payload ?? (() => {
          const p: RideRequestPayload = {};
          if (data.pickupLocation     !== undefined) p.pickupLocation     = data.pickupLocation;
          if (data.dropLocation       !== undefined) p.dropLocation       = data.dropLocation;
          if (data.destinationLocation !== undefined) p.destinationLocation = data.destinationLocation;
          if (data.fare               !== undefined) p.fare               = data.fare;
          if (data.vehicle            !== undefined) p.vehicle            = data.vehicle;
          if (data.vehicleType        !== undefined) p.vehicleType        = data.vehicleType;
          if (data.distance           !== undefined) p.distance           = data.distance;
          if (data.riderName          !== undefined) p.riderName          = data.riderName;
          if (data.parcelDetails      !== undefined) p.parcelDetails      = data.parcelDetails;
          if (data.pickupAddress      !== undefined) p.pickupAddress      = data.pickupAddress;
          if (data.dropAddress        !== undefined) p.dropAddress        = data.dropAddress;
          return p;
        })();

        console.log(`Ride request from rider ${client.id}:`, ridePayload);

        // Store pending request for offline/backgrounded drivers
        await setPendingRequest(client.id, { ...ridePayload, riderId: client.id, timestamp: Date.now() });

        const pickupLoc: Location | undefined = ridePayload.pickupLocation;
        let matchedCount = 0;

        drivers.forEach((driver) => {
          if (driver.status !== 'available' || driver.ws.readyState !== WebSocket.OPEN) {
            console.log(`[Dispatch] Skipped Driver ${driver.id} - status: ${driver.status}`);
            return;
          }

          // Vehicle Type matching: Only dispatch if driver's vehicleType category matches requested vehicle
          const reqType = (ridePayload.vehicleType ?? ridePayload.vehicle ?? 'bike').toLowerCase();
          const drvType = (driver.vehicleType ?? 'bike').toLowerCase();
          const isRequestAuto = reqType.includes('auto');
          const isDriverAuto = drvType.includes('auto');
          
          if (isRequestAuto !== isDriverAuto) {
            console.log(`[Dispatch] Skipped Driver ${driver.id} - vehicle type mismatch (Driver: ${drvType}, Req: ${reqType})`);
            return;
          }

          // Gender validation for She-Bike
          if (reqType.includes('she-bike')) {
            if (driver.gender !== 'female') {
              console.log(`[Dispatch] Skipped Driver ${driver.id} - she-bike requested but driver gender is ${driver.gender}`);
              return;
            }
          }

          // Geospatial filtering: skip drivers outside the match radius
          if (pickupLoc) {
            if (!driver.lastLocation) {
              // Driver location unknown — include them anyway (can't compute distance)
              console.log(`[Dispatch] Including Driver ${driver.id} - no lastLocation known (broadcasting to all available)`);
              // Fall through to send the request
            } else {
              const dist = getDistanceInKm(
                pickupLoc.lat,
                pickupLoc.lng,
                driver.lastLocation.lat,
                driver.lastLocation.lng,
              );
              if (dist > MAX_DRIVER_MATCH_DISTANCE_KM) {
                console.log(`[Dispatch] Skipped Driver ${driver.id} - distance ${dist.toFixed(2)}km > ${MAX_DRIVER_MATCH_DISTANCE_KM}km max`);
                return;
              }
            }
          }

          matchedCount++;
          driver.ws.send(
            JSON.stringify({
              type: 'new_ride_request',
              payload: { riderId: client.id, ...ridePayload },
            }),
          );
        });

        console.log(
          `Broadcasted ride request to ${matchedCount} nearby drivers within ${MAX_DRIVER_MATCH_DISTANCE_KM} km`,
        );

        // ── Push Notification Fallback ──────────────────────────────────────
        // Also send push notifications to matched drivers who have registered
        // push tokens. This ensures drivers receive the request even if the
        // app is backgrounded or the WebSocket connection is temporarily lost.
        drivers.forEach((driver) => {
          if (driver.status !== 'available') {
            console.log(`[Push Fallback] Skipped Driver ${driver.id} - status: ${driver.status}`);
            return;
          }
          if (pickupLoc) {
            if (!driver.lastLocation) {
              console.log(`[Push Fallback] Including Driver ${driver.id} - no lastLocation known (sending push anyway)`);
              // Fall through to send the push notification
            } else {
              const dist = getDistanceInKm(
                pickupLoc.lat, pickupLoc.lng,
                driver.lastLocation.lat, driver.lastLocation.lng,
              );
              if (dist > MAX_DRIVER_MATCH_DISTANCE_KM) {
                console.log(`[Push Fallback] Skipped Driver ${driver.id} - distance ${dist.toFixed(2)}km > ${MAX_DRIVER_MATCH_DISTANCE_KM}km max`);
                return;
              }
            }
          }
          notifyDriverOfRideRequest(driver.id, {
            riderId: client.id,
            riderName: ridePayload.riderName,
            pickupAddress: ridePayload.pickupAddress ?? (pickupLoc ? `${pickupLoc.lat.toFixed(4)}, ${pickupLoc.lng.toFixed(4)}` : undefined),
            dropAddress: ridePayload.dropAddress,
            fare: ridePayload.fare,
            distance: ridePayload.distance ? parseFloat(String(ridePayload.distance)) : undefined,
            vehicleType: ridePayload.vehicleType ?? ridePayload.vehicle,
            pickupLat: pickupLoc?.lat,
            pickupLng: pickupLoc?.lng,
          }).catch((err) => console.error(`[Push] Error notifying driver ${driver.id}:`, err));
        });
        break;
      }

      
      // ── Tatkal Ride Start (QR Scan) -> Ask Rider Consent ──
      case 'tatkal_ride_start': {
        if (!client || client.role !== 'driver') break;
        const { bookingId, riderId, driverName, code, pickup, drop, vehicle, fare } = data as any;
        if (!riderId) break;

        // Notify Driver that we are waiting for rider consent
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'tatkal_consent_pending' }));
        }

        // Send consent request to Rider
        const riderToNotify = riders.get(riderId);
        if (riderToNotify?.ws.readyState === WebSocket.OPEN) {
          riderToNotify.ws.send(JSON.stringify({
            type: 'tatkal_consent_request',
            payload: {
              bookingId, riderId, driverId: client.id, driverName: driverName || client.id,
              code, pickup, drop, vehicle, fare, type: 'tatkal_ride'
            }
          }));
        }
        break;
      }

      // ── Rider Accepts Tatkal Consent ──
      case 'tatkal_consent_accept': {
        if (!client || client.role !== 'rider') break;
        const payload = data.payload;
        if (!payload || !payload.driverId) break;
        
        const driverClient = drivers.get(payload.driverId);
        if (driverClient) driverClient.status = 'busy';

        const tripRecord: TripRecord = {
          ...payload,
          status: 'accepted',
          otp: payload.code || Math.floor(1000 + Math.random() * 9000).toString(),
        };

        (async () => {
          try {
            const dbTrip = await prisma.trip.create({
              data: {
                riderId: payload.riderId,
                driverId: payload.driverId,
                status: 'accepted',
                otp: tripRecord.otp ?? null,
                vehicleType: payload.vehicle ?? null,
                fare: payload.fare ? parseFloat(String(payload.fare)) : null,
              }
            });
            tripRecord.id = dbTrip.id;
          } catch (e) {
            console.error('[Prisma] Error creating tatkal trip:', e);
          }
        })();

        await setActiveTrip(payload.riderId, tripRecord);
        await deletePendingRequest(payload.riderId);

        console.log(`[tatkal_ride_started] Driver ${payload.driverId} started tatkal ride for rider ${payload.riderId}`);

        // Notify Rider
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'tatkal_ride_started', payload: tripRecord }));
        }

        // Notify Driver
        if (driverClient?.ws.readyState === WebSocket.OPEN) {
          driverClient.ws.send(JSON.stringify({ type: 'tatkal_ride_started', payload: tripRecord }));
          driverClient.ws.send(JSON.stringify({ type: 'sync_state', payload: tripRecord }));
        }
        break;
      }

      // ── Rider Rejects Tatkal Consent ──
      case 'tatkal_consent_reject': {
        if (!client || client.role !== 'rider') break;
        const driverId = data.payload?.driverId;
        if (!driverId) break;
        
        const driverClient = drivers.get(driverId);
        if (driverClient?.ws.readyState === WebSocket.OPEN) {
          driverClient.ws.send(JSON.stringify({ type: 'tatkal_consent_rejected' }));
        }
        break;
      }

      // ── Chat Message ──
      case 'chat_message': {
        if (!client) break;

        const targetId = data.to ?? data.toId ?? data.recipientId;
        const textMsg = data.message ?? data.text ?? '';

        if (targetId) {
          const tripForChat = (await getActiveTrip(client.role === 'rider' ? client.id : targetId));
          if (tripForChat && tripForChat.id) {
            prisma.chatMessage.create({
              data: {
                tripId: tripForChat.id,
                senderId: client.id,
                text: textMsg
              }
            }).catch((e: any) => console.error('[Prisma] Error saving chat message:', e));
          }
          const recipient =
            client.role === 'rider' ? drivers.get(targetId) : riders.get(targetId);

          if (recipient?.ws.readyState === WebSocket.OPEN) {
            const timestamp = new Date().toISOString();
            recipient.ws.send(
              JSON.stringify({
                type: 'chat_message',
                from: client.id,
                message: textMsg,
                timestamp,
                payload: { fromId: client.id, senderId: client.id, text: textMsg, timestamp },
              }),
            );
          }
        }
        break;
      }

      // ── Feedback ──────────────────────────────────────────────────────────
      case 'submit_feedback': {
        if (!client) break;
        if (!data.tripId || !data.toUserId || !data.rating) break;

        (async () => {
          try {
            await prisma.feedback.create({
              data: {
                tripId: data.tripId,
                fromUserId: client.id,
                toUserId: data.toUserId,
                rating: Number(data.rating),
                comments: data.comments ?? null
              }
            });
            console.log(`[Feedback] ${client.id} rated ${data.toUserId} with ${data.rating} stars`);
          } catch (e) {
            console.error('[Prisma] Error saving feedback:', e);
          }
        })();
        break;
      }

      // ── On-demand heatmap ──────────────────────────────────────────────────
      case 'get_demand_heatmap': {
        if (client?.role === 'driver') {
          ws.send(JSON.stringify({ type: 'demand_heatmap', payload: MOCK_HOTSPOTS }));
        }
        break;
      }

      // ── Push Token Registration ────────────────────────────────────────────
      case 'register_push_token': {
        if (!client) break;
        const success = registerPushToken(client.id, data.pushToken);
        ws.send(JSON.stringify({ type: 'push_token_registered', success }));
        break;
      }

      // ── Push Token Unregistration ──────────────────────────────────────────
      case 'unregister_push_token': {
        if (!client) break;
        unregisterPushToken(client.id);
        break;
      }

      // ── Application Level Ping ─────────────────────────────────────────────
      case 'ping': {
        // Silently handled to keep connection alive
        break;
      }

      default: {
        // Runtime safety net for messages not covered by InboundMessage
        const unknownType = (data as { type?: unknown }).type;
        console.log('[WS] Unknown message type:', unknownType);
      }
    }
  });

  ws.on('close', () => {
    const client = getClientInfo(ws);
    if (!client) {
      console.log('Unauthenticated client disconnected');
      return;
    }
    if (client.role === 'rider') {
      riders.delete(client.id);
      console.log(`Rider disconnected: ${client.id}`);
    } else {
      // Do NOT delete the driver from the `drivers` map here!
      // We keep their state (status, lastLocation) in memory so they can 
      // still receive Push Notifications while backgrounded/offline.
      console.log(`Driver WS closed: ${client.id} (Kept in memory for Push)`);
    }
  });
});

// ─── HTTP Routes ──────────────────────────────────────────────────────────────

app.get('/', (_req, res) => {
  res.send('Realtime WebSocket Server is running');
});

/**
 * GET /api/vehicle-types
 * Dynamically fetch allowed vehicle types.
 */
app.get('/api/vehicle-types', (_req, res) => {
  res.json({
    types: [
      { id: 'bike', name: 'Bike' },
      { id: 'auto', name: 'Auto' },
      { id: 'she-bike', name: 'She Bike' },
      { id: 'parcel-bike', name: 'Parcel Bike' },
      { id: 'mini', name: 'Mini Cab' },
      { id: 'sedan', name: 'Sedan' },
      { id: 'suv', name: 'SUV' },
      { id: 'premium', name: 'Premium' },
    ]
  });
});

// ─── REST: Push-Triggered Ride Request ────────────────────────────────────────

/**
 * POST /api/request-ride
 *
 * The ride-booker calls this endpoint to securely trigger a Push Notification
 * to nearby drivers. This is the asynchronous complement to the WebSocket
 * ride_request message — useful when the rider wants to ensure the driver
 * gets notified even if their app is backgrounded.
 *
 * Body: {
 *   riderId: string,
 *   pickupLocation: { lat: number, lng: number },
 *   dropLocation?: { lat: number, lng: number },
 *   fare?: number,
 *   vehicleType?: string,
 *   riderName?: string,
 *   distance?: number,
 *   pickupAddress?: string,
 *   dropAddress?: string
 * }
 *
 * Auth: Bearer token in Authorization header
 */

/**
 * Background Location Update API
 * Receives REST POST with location data from background drivers.
 */
app.post('/api/location', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization header required' });
    return;
  }
  const token = authHeader.split(' ')[1];
  if (!token) {
    res.status(401).json({ error: 'Token missing' });
    return;
  }
  try {
    const decoded = jwt.verify(token, JWT_SECRET as string) as any;
    if (!decoded.id || !decoded.role || decoded.role !== 'driver') {
      res.status(401).json({ error: 'Invalid driver token' });
      return;
    }
    
    const { location, riderId } = req.body;
    if (!location || typeof location.lat !== 'number' || typeof location.lng !== 'number') {
      res.status(400).json({ error: 'Invalid location payload' });
      return;
    }

    // Update Redis
    await redis.geoadd('driver_locations', location.lng, location.lat, decoded.id);
    
    // Broadcast to rider
    let targetRiderId: string | undefined = riderId;
    if (!targetRiderId) {
      const rId = await redis.get(`drivertrip:${decoded.id}`);
      if (rId) targetRiderId = rId;
    }

    if (targetRiderId) {
      const targetRider = riders.get(targetRiderId);
      if (targetRider?.ws.readyState === WebSocket.OPEN) {
        targetRider.ws.send(JSON.stringify({
          type: 'driver_location',
          payload: { driverId: decoded.id, location }
        }));
      }
    }

    res.json({ success: true });
  } catch (err) {
    console.error('[API] Location Update error:', err);
    res.status(401).json({ error: 'Unauthorized' });
  }
});

app.post('/api/request-ride', async (req, res) => {
  // Authenticate the request
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization header required' });
    return;
  }

  let decoded: { id?: string; userId?: string; role?: string };
  try {
    decoded = jwt.verify(authHeader.slice(7), JWT_SECRET, { ignoreExpiration: true }) as typeof decoded;
  } catch {
    res.status(401).json({ error: 'Invalid or expired token' });
    return;
  }

  const body = req.body as {
    riderId?: string;
    pickupLocation?: Location;
    dropLocation?: Location;
    fare?: number;
    vehicleType?: string;
    riderName?: string;
    distance?: number;
    pickupAddress?: string;
    dropAddress?: string;
  };

  const riderId = body.riderId ?? decoded.id ?? decoded.userId;
  if (!riderId) {
    res.status(400).json({ error: 'riderId is required' });
    return;
  }

  const pickupLoc = body.pickupLocation;

  // Store in pending requests for reconnecting background drivers
  const newRequest: any = { riderId, timestamp: Date.now() };
  if (body.pickupLocation !== undefined) newRequest.pickupLocation = body.pickupLocation;
  if (body.dropLocation !== undefined) newRequest.dropLocation = body.dropLocation;
  if (body.fare !== undefined) newRequest.fare = body.fare;
  if (body.vehicleType !== undefined) newRequest.vehicleType = body.vehicleType;
  if (body.riderName !== undefined) newRequest.riderName = body.riderName;
  if (body.distance !== undefined) newRequest.distance = body.distance;
  if (body.pickupAddress !== undefined) newRequest.pickupAddress = body.pickupAddress;
  if (body.dropAddress !== undefined) newRequest.dropAddress = body.dropAddress;
  await setPendingRequest(riderId, newRequest);

  // Find nearby available drivers and send push notifications
  let notifiedCount = 0;
  const notificationPromises: Promise<unknown>[] = [];

  drivers.forEach((driver) => {
    if (driver.status !== 'available') return;

    // Geospatial filtering
    if (pickupLoc && driver.lastLocation) {
      const dist = getDistanceInKm(
        pickupLoc.lat, pickupLoc.lng,
        driver.lastLocation.lat, driver.lastLocation.lng,
      );
      if (dist > MAX_DRIVER_MATCH_DISTANCE_KM) return;
    }

    notifiedCount++;

    // Send push notification to this driver
    const pushPromise = notifyDriverOfRideRequest(driver.id, {
      riderId,
      riderName: body.riderName,
      pickupAddress: body.pickupAddress ?? (pickupLoc ? `${pickupLoc.lat.toFixed(4)}, ${pickupLoc.lng.toFixed(4)}` : undefined),
      dropAddress: body.dropAddress,
      fare: body.fare,
      distance: body.distance,
      vehicleType: body.vehicleType,
      pickupLat: pickupLoc?.lat,
      pickupLng: pickupLoc?.lng,
      dropLat: body.dropLocation?.lat,
      dropLng: body.dropLocation?.lng,
    });

    notificationPromises.push(pushPromise);

    // Also send via WebSocket if they're connected
    if (driver.ws.readyState === WebSocket.OPEN) {
      driver.ws.send(JSON.stringify({
        type: 'new_ride_request',
        payload: {
          riderId,
          pickupLocation: body.pickupLocation,
          dropLocation: body.dropLocation,
          fare: body.fare,
          vehicleType: body.vehicleType,
          riderName: body.riderName,
          distance: body.distance,
          pickupAddress: body.pickupAddress,
          dropAddress: body.dropAddress,
        },
      }));
    }
  });

  // Wait for all notifications to be sent before responding
  Promise.allSettled(notificationPromises)
    .then(() => {
      res.json({
        success: true,
        driversNotified: notifiedCount,
        message: `Ride request sent to ${notifiedCount} nearby driver(s)`,
      });
    })
    .catch(() => {
      res.status(500).json({ error: 'Failed to send some notifications' });
    });
});


// ─── REST: Schedule Ride ──────────────────────────────────────────────────────

app.post('/api/schedule-ride', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization header required' });
    return;
  }

  let decoded: any;
  try {
    decoded = jwt.verify(authHeader.slice(7), JWT_SECRET, { ignoreExpiration: true });
  } catch {
    res.status(401).json({ error: 'Invalid or expired token' });
    return;
  }

  const riderId = req.body.riderId ?? decoded.id ?? decoded.userId;
  if (!riderId) {
    res.status(400).json({ error: 'riderId is required' });
    return;
  }

  const { scheduledTime, pickupLocation, dropLocation, fare, vehicleType, riderName, distance, pickupAddress, dropAddress } = req.body;
  if (!scheduledTime || new Date(scheduledTime).getTime() <= Date.now()) {
    res.status(400).json({ error: 'Valid future scheduledTime is required' });
    return;
  }

  try {
    const dbTrip = await prisma.trip.create({
      data: {
        riderId,
        status: 'scheduled',
        isScheduled: true,
        scheduledTime: new Date(scheduledTime),
        pickupLat: pickupLocation?.lat,
        pickupLng: pickupLocation?.lng,
        dropLat: dropLocation?.lat,
        dropLng: dropLocation?.lng,
        fare: fare ? parseFloat(String(fare)) : null,
        vehicleType: vehicleType,
        pickupAddress,
        dropAddress,
        distance: distance ? parseFloat(String(distance)) : null,
      }
    });

    res.json({
      success: true,
      tripId: dbTrip.id,
      message: `Ride scheduled successfully for ${new Date(scheduledTime).toLocaleString()}`
    });
  } catch (err) {
    console.error('[API] Error scheduling ride:', err);
    res.status(500).json({ error: 'Failed to schedule ride' });
  }
});

// ─── Cron: Process Scheduled Rides ─────────────────────────────────────────────

setInterval(async () => {
  try {
    // Find scheduled rides that are <= 15 mins away and still in 'scheduled' status
    const fifteenMinsFromNow = new Date(Date.now() + 15 * 60000);
    const trips = await prisma.trip.findMany({
      where: {
        status: 'scheduled',
        isScheduled: true,
        scheduledTime: { lte: fifteenMinsFromNow }
      }
    });

    for (const trip of trips) {
      // Mark as pending
      await prisma.trip.update({
        where: { id: trip.id },
        data: { status: 'pending' }
      });

      // Prepare payload
      const payload = {
        riderId: trip.riderId,
        pickupLocation: { lat: trip.pickupLat!, lng: trip.pickupLng! },
        dropLocation: { lat: trip.dropLat!, lng: trip.dropLng! },
        fare: trip.fare,
        vehicleType: trip.vehicleType,
        riderName: 'Rider', // Could fetch from User table
        distance: trip.distance,
        pickupAddress: trip.pickupAddress,
        dropAddress: trip.dropAddress,
      };

      // Store in Redis for reconnecting drivers
      await setPendingRequest(trip.riderId, { ...payload, timestamp: Date.now() });

      // Notify nearby available drivers
      let notifiedCount = 0;
      drivers.forEach((driver) => {
        if (driver.status !== 'available') return;
        
        if (driver.lastLocation && trip.pickupLat && trip.pickupLng) {
          const dist = getDistanceInKm(trip.pickupLat, trip.pickupLng, driver.lastLocation.lat, driver.lastLocation.lng);
          if (dist > MAX_DRIVER_MATCH_DISTANCE_KM) return;
        }

        notifiedCount++;
        notifyDriverOfRideRequest(driver.id, {
          ...payload,
          pickupAddress: payload.pickupAddress ?? undefined,
          dropAddress: payload.dropAddress ?? undefined,
          fare: payload.fare ?? undefined,
          distance: payload.distance ?? undefined,
          vehicleType: payload.vehicleType ?? undefined,
          pickupLat: trip.pickupLat!,
          pickupLng: trip.pickupLng!,
          dropLat: trip.dropLat!,
          dropLng: trip.dropLng!,
        }).catch((e: any) => console.error('[Cron] Push err:', e));

        if (driver.ws.readyState === WebSocket.OPEN) {
          driver.ws.send(JSON.stringify({ type: 'new_ride_request', payload }));
        }
      });
      console.log(`[Cron] Dispatched scheduled ride ${trip.id} to ${notifiedCount} drivers.`);
    }
  } catch (err) {
    console.error('[Cron] Error processing scheduled rides:', err);
  }
}, 60000); // run every 1 minute


// ─── REST: Register Push Token ────────────────────────────────────────────────

/**
 * POST /api/register-push-token
 *
 * Alternative to the WebSocket message for registering push tokens.
 * Useful if the app needs to register before establishing a WebSocket connection.
 *
 * Body: { userId: string, pushToken: string }
 * Auth: Bearer token in Authorization header
 */
app.post('/api/register-push-token', (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization header required' });
    return;
  }

  try {
    jwt.verify(authHeader.slice(7), JWT_SECRET, { ignoreExpiration: true });
  } catch {
    res.status(401).json({ error: 'Invalid or expired token' });
    return;
  }

  const { userId, pushToken } = req.body as { userId?: string; pushToken?: string };

  if (!userId || !pushToken) {
    res.status(400).json({ error: 'userId and pushToken are required' });
    return;
  }

  const success = registerPushToken(userId, pushToken);
  res.json({ success, message: success ? 'Push token registered' : 'Invalid push token format' });
});

// ─── REST: Send Test Notification ─────────────────────────────────────────────

/**
 * POST /api/test-notification
 * Dev-only endpoint to verify push notification setup.
 *
 * Body: { userId: string, title?: string, body?: string }
 */
app.post('/api/test-notification', async (req, res) => {
  const { userId, title, body: bodyText } = req.body as {
    userId?: string;
    title?: string;
    body?: string;
  };

  if (!userId) {
    res.status(400).json({ error: 'userId is required' });
    return;
  }

  const token = getPushToken(userId);
  if (!token) {
    res.status(404).json({ error: `No push token registered for ${userId}` });
    return;
  }

  const ticket = await sendPushNotification(
    userId,
    title ?? '🔔 Test Notification',
    bodyText ?? 'Push notifications are working!',
    { type: 'test' },
  );

  res.json({ success: !!ticket, ticket });
});

/**
 * POST /auth/login
 * Expo app calls this first → gets a JWT → uses it for WebSocket connection.
 *
 * Body: { id: string, role: 'rider' | 'driver' }
 *
 * Production lo: ikkade DB check, password verify chesukoni token issue cheyyali.
 * Ippudu: id + role isthe chalu, token vastundi.
 */
app.post('/auth/login', async (req, res) => {
  const { token: firebaseToken, role } = req.body as { token?: string; role?: string };

  if (!firebaseToken || !role || (role !== 'rider' && role !== 'driver')) {
    res.status(400).json({ error: 'Firebase token and role (rider | driver) required' });
    return;
  }

  let decodedFirebaseToken: any;
  try {
    // DEV MODE ONLY: Decoding without verification because we lack serviceAccountKey.json
    // In production, use firebase-admin.auth().verifyIdToken(firebaseToken)
    decodedFirebaseToken = jwt.decode(firebaseToken);
    if (!decodedFirebaseToken || !decodedFirebaseToken.phone_number) {
      throw new Error('Invalid Firebase token or missing phone number');
    }
  } catch (err: any) {
    console.error('Firebase Token Decode Error:', err);
    res.status(401).json({ error: 'Invalid Firebase token' });
    return;
  }

  const phone = decodedFirebaseToken.phone_number;
  const uid = decodedFirebaseToken.user_id || phone;
  
  console.log(`[Auth Login] Checking DB for phone: ${phone} (UID: ${uid})`);

  try {
    // Check if user exists in DB
    const existingUser = await prisma.user.findUnique({
      where: { userId: uid }
    });

    let isNewUser = false;
    let userData = existingUser;
    if (!existingUser) {
      isNewUser = true;
      console.log(`[Auth Login] User not found in DB. Creating new record for ${uid}`);
      userData = await prisma.user.create({
        data: {
          userId: uid,
          phone: phone,
          role: role
        }
      });
    } else {
      console.log(`[Auth Login] User ${uid} found in DB.`);
      // If the user hasn't completed their profile yet, consider them new
      if (!existingUser.name) {
        isNewUser = true;
      }
    }

    // Issue internal JWT for WebSocket Authentication
    const internalToken = jwt.sign(
      { id: uid, role },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    res.json({ token: internalToken, id: uid, role, isNewUser, user: userData });
  } catch (dbErr: any) {
    console.error('[Auth Login] DB Error:', dbErr);
    res.status(500).json({ error: 'Database operation failed' });
  }
});

/**
 * POST /auth/update-profile
 * Completes the user profile with Name, Email, and Gender.
 */
app.post('/auth/update-profile', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization header required' });
    return;
  }

  const token = authHeader.slice(7);
  try {
    const decoded = jwt.verify(token, JWT_SECRET) as any;
    const uid = decoded.id || decoded.userId;

    const { 
      name, email, gender, dob, languages, 
      vehicleType, vehicleNumber, 
      subscriptionStatus, subscriptionExpiry, subscriptionExpiryDate,
      subscriptionPlanId, subscriptionEarningLimit 
    } = req.body;

    const updateData: any = {};
    if (name !== undefined) updateData.name = name;
    if (email !== undefined) updateData.email = email;
    if (gender !== undefined) updateData.gender = gender;
    if (dob !== undefined) updateData.dob = dob;
    if (languages !== undefined) updateData.languages = languages;
    if (vehicleType !== undefined) updateData.vehicleType = vehicleType;
    if (vehicleNumber !== undefined) updateData.vehicleNumber = vehicleNumber;
    if (subscriptionStatus !== undefined) updateData.subscriptionStatus = subscriptionStatus;
    const expiry = subscriptionExpiry || subscriptionExpiryDate;
    if (expiry !== undefined) updateData.subscriptionExpiry = new Date(expiry);
    if (subscriptionPlanId !== undefined) updateData.subscriptionPlanId = subscriptionPlanId;
    if (subscriptionEarningLimit !== undefined) updateData.subscriptionEarningLimit = subscriptionEarningLimit;

    const updatedUser = await prisma.user.update({
      where: { userId: uid },
      data: updateData
    });

    res.json({
      message: 'Profile updated successfully',
      user: { name, email, gender }
    });
  } catch (error) {
    console.error('[Auth Update Profile] Error:', error);
    res.status(500).json({ error: 'Failed to update profile' });
  }
});

/**
 * POST /auth/refresh
 * Refresh the JWT token so the user stays logged in.
 */
app.post('/auth/refresh', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization header required' });
    return;
  }

  const oldToken = authHeader.slice(7);
  try {
    // We ignore expiration to allow slightly expired tokens to be refreshed
    const decoded = jwt.verify(oldToken, JWT_SECRET, { ignoreExpiration: true }) as any;
    
    if (!decoded || (!decoded.id && !decoded.userId)) {
      throw new Error("Invalid token payload");
    }

    const uid = decoded.id || decoded.userId;
    const role = decoded.role;

    // Issue a new 7-day token
    const newToken = jwt.sign(
      { id: uid, role },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    res.json({ token: newToken });
  } catch (err: any) {
    console.error('[Auth Refresh] Error:', err);
    res.status(401).json({ error: 'Invalid token' });
  }
});


// ─── Nearby Drivers Broadcast ─────────────────────────────────────────────────

/**
 * 6. Idle Rider UX
 * Show roaming car icons to idle riders to improve conversion rate.
 * Pushed every 5 s instead of relying on client HTTP polling.
 */
const broadcastNearbyDrivers = setInterval(() => {
  const availableDrivers = Array.from(drivers.entries())
    .filter(([, d]) => d.status === 'available' && d.lastLocation != null)
    .map(([id, d]) => ({ 
      id, 
      lat: d.lastLocation!.lat, 
      lng: d.lastLocation!.lng,
      vehicleType: d.vehicleType || 'bike'
    }));

  if (availableDrivers.length === 0) return;

  const payload = JSON.stringify({ type: 'nearby_drivers', payload: availableDrivers });

  riders.forEach(async (rider, riderId) => {
    // Only send to idle riders not currently in an active trip
    if (!(!!(await getActiveTrip(riderId))) && rider.ws.readyState === WebSocket.OPEN) {
      rider.ws.send(payload);
    }
  });
}, 5_000);

// ─── Start Server ─────────────────────────────────────────────────────────────

const PORT = process.env.PORT ?? 8080;
server.listen(PORT, () => {
  console.log(`\n══════════════════════════════════════════════════════════════`);
  console.log(`  Realtime Server listening on port ${PORT}`);
  console.log(`  Redis: connected | PostgreSQL: connected`);
  console.log(`══════════════════════════════════════════════════════════════\n`);
});

// ─── Graceful Shutdown ────────────────────────────────────────────────────────

/**
 * On SIGINT / SIGTERM, close all WebSocket connections with code 1001 (Going Away)
 * so clients trigger their exponential-backoff reconnect immediately rather than
 * waiting for a TCP timeout.
 */
const gracefulShutdown = (): void => {
  console.log('Server shutting down, disconnecting clients gracefully…');
  clearInterval(heartbeatInterval);
  clearInterval(heatmapInterval);
  clearInterval(broadcastNearbyDrivers);

  wss.clients.forEach((ws) => ws.close(1001, 'Server shutting down'));

  server.close(() => {
    console.log('Server closed.');
    process.exit(0);
  });
};

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);