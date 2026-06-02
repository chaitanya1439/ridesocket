import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import cors from 'cors';
import jwt from 'jsonwebtoken';

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
} from './types.js';

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

/**
 * 2. Active State Recovery
 * Persists trip state in memory so reconnecting mobile clients can be
 * instantly synced without hitting a database.
 * Key: riderId
 */
const activeTrips = new Map<string, TripRecord>();

const MAX_DRIVER_MATCH_DISTANCE_KM = Number(
  process.env.MAX_DRIVER_MATCH_DISTANCE_KM ?? 15,
);

// ─── Heartbeat & Idle Pruning ─────────────────────────────────────────────────

const IDLE_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes

const heartbeatInterval = setInterval(() => {
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
      console.log(`[Heartbeat] Terminating inactive ${client.role} ${client.id}`);
      if (client.role === 'rider') riders.delete(client.id);
      else drivers.delete(client.id);
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
  try {
    const url = new URL(request.url ?? '', `http://${request.headers.host}`);
    const token = url.searchParams.get('token');

    if (!token) {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    let decoded: DecodedToken;
    try {
      decoded = jwt.verify(token, JWT_SECRET) as DecodedToken;
    } catch {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request, decoded);
    });
  } catch {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
  }
});

// ─── WebSocket Connection Handler ────────────────────────────────────────────

wss.on('connection', (ws: WebSocket, _request: unknown, decodedToken: DecodedToken) => {
  console.log('New authenticated client connected');

  // Support both `id` and `userId` JWT schemas
  const tokenUserId = decodedToken?.id ?? decodedToken?.userId;

  // React Native automatically replies to native ping frames with a pong.
  ws.on('pong', () => {
    const client = getClientInfo(ws);
    if (client) client.isAlive = true;
  });

  ws.on('message', (raw: Buffer | string) => {
    let data: InboundMessage;

    try {
      data = JSON.parse(raw.toString()) as InboundMessage;
    } catch (err) {
      console.error('[WS] Failed to parse message:', err);
      return;
    }

    // Update activity timestamp on every message
    const client = getClientInfo(ws);
    if (client) client.lastActivity = Date.now();

    switch (data.type) {
      // ── Auth ───────────────────────────────────────────────────────────────
      case 'auth': {
        const newClient: ClientInfo = {
          ws,
          role: data.role,
          id: data.id ?? tokenUserId ?? '',
          isAlive: true,
          lastActivity: Date.now(),
          ...(data.role === 'driver' ? { status: 'offline' as DriverStatus } : {}),
        };

        setClientInfo(ws, newClient);

        if (data.role === 'rider') {
          riders.set(newClient.id, newClient);
          console.log(`Rider authorised in memory: ${newClient.id}`);
        } else {
          drivers.set(newClient.id, newClient);
          console.log(`Driver authorised in memory: ${newClient.id}`);
        }

        ws.send(JSON.stringify({ type: 'auth_success', id: newClient.id, role: data.role }));

        // Offline Recovery Sync — instantly restore in-progress trips on reconnect
        let currentTrip: TripRecord | undefined;
        if (data.role === 'rider') {
          currentTrip = activeTrips.get(newClient.id);
        } else {
          for (const trip of activeTrips.values()) {
            if (trip.driverId === newClient.id) {
              currentTrip = trip;
              break;
            }
          }
        }

        if (currentTrip) {
          ws.send(JSON.stringify({ type: 'sync_state', payload: currentTrip }));
          console.log(`Synced active state to reconnecting ${data.role} ${newClient.id}`);
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
          return p;
        })();

        console.log(`Ride request from rider ${client.id}:`, ridePayload);

        const pickupLoc: Location | undefined = ridePayload.pickupLocation;
        let matchedCount = 0;

        drivers.forEach((driver) => {
          if (driver.status !== 'available' || driver.ws.readyState !== WebSocket.OPEN) return;

          // Geospatial filtering: skip drivers outside the match radius
          if (pickupLoc && driver.lastLocation) {
            const dist = getDistanceInKm(
              pickupLoc.lat,
              pickupLoc.lng,
              driver.lastLocation.lat,
              driver.lastLocation.lng,
            );
            if (dist > MAX_DRIVER_MATCH_DISTANCE_KM) return;
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
        break;
      }

      // ── Ride accept ────────────────────────────────────────────────────────
      case 'ride_accept': {
        if (!client || client.role !== 'driver') break;

        client.status = 'busy';

        const tripRecord: TripRecord = {
          riderId: data.riderId,
          driverId: client.id,
          status: 'accepted',
          ...data.payload,
        };
        activeTrips.set(data.riderId, tripRecord);

        const riderToNotify = riders.get(data.riderId);
        if (riderToNotify?.ws.readyState === WebSocket.OPEN) {
          riderToNotify.ws.send(JSON.stringify({ type: 'ride_accepted', payload: tripRecord }));
        }
        break;
      }

      // ── Location update ────────────────────────────────────────────────────
      case 'location_update': {
        if (!client || client.role !== 'driver') break;

        if (data.location) {
          client.lastLocation = data.location;
        }

        // Derive the paired rider from server memory (never trust client-provided riderId blindly)
        let targetRiderId: string | undefined = data.riderId;
        if (!targetRiderId) {
          for (const [riderId, trip] of activeTrips.entries()) {
            if (trip.driverId === client.id) {
              targetRiderId = riderId;
              break;
            }
          }
        }

        if (targetRiderId) {
          const targetRider = riders.get(targetRiderId);
          if (targetRider?.ws.readyState === WebSocket.OPEN) {
            targetRider.ws.send(
              JSON.stringify({
                type: 'driver_location',
                payload: { driverId: client.id, location: data.location },
              }),
            );
          }
        }
        break;
      }

      // ── Trip status update ─────────────────────────────────────────────────
      case 'trip_status_update': {
        if (!client || client.role !== 'driver') break;

        console.log(`Trip status update from ${client.id} for ${data.riderId}: ${data.status}`);

        const trip = activeTrips.get(data.riderId);
        if (trip) trip.status = data.status as TripStatus;

        const targetRider = riders.get(data.riderId);
        if (targetRider?.ws.readyState === WebSocket.OPEN) {
          targetRider.ws.send(
            JSON.stringify({
              type: 'trip_status_changed',
              payload: { driverId: client.id, status: data.status },
            }),
          );
        }

        if (data.status === 'completed' || data.status === 'cancelled') {
          client.status = 'available';
          activeTrips.delete(data.riderId);
        }
        break;
      }

      // ── Chat message ───────────────────────────────────────────────────────
      case 'CHAT_MESSAGE':
      case 'chat_message': {
        if (!client) break;

        const targetId = data.to ?? data.toId;
        const textMsg = data.message ?? data.text ?? '';

        if (targetId) {
          const recipient =
            client.role === 'rider' ? drivers.get(targetId) : riders.get(targetId);

          if (recipient?.ws.readyState === WebSocket.OPEN) {
            const timestamp = new Date().toISOString();
            recipient.ws.send(
              JSON.stringify({
                type: 'CHAT_MESSAGE',
                from: client.id,
                message: textMsg,
                timestamp,
                payload: { fromId: client.id, text: textMsg, timestamp },
              }),
            );
          }
        }
        break;
      }

      // ── On-demand heatmap ──────────────────────────────────────────────────
      case 'get_demand_heatmap': {
        if (client?.role === 'driver') {
          ws.send(JSON.stringify({ type: 'demand_heatmap', payload: MOCK_HOTSPOTS }));
        }
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
      drivers.delete(client.id);
      console.log(`Driver disconnected: ${client.id}`);
    }
  });
});

// ─── HTTP Routes ──────────────────────────────────────────────────────────────

app.get('/', (_req, res) => {
  res.send('Realtime WebSocket Server is running');
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
app.post('/auth/login', (req, res) => {
  const { id, role } = req.body as { id?: string; role?: string };

  if (!id || !role || (role !== 'rider' && role !== 'driver')) {
    res.status(400).json({ error: 'id and role (rider | driver) required' });
    return;
  }

  const token = jwt.sign(
    { id, role },
    JWT_SECRET,
    { expiresIn: '7d' }
  );

  res.json({ token, id, role });
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
    .map(([id, d]) => ({ id, lat: d.lastLocation!.lat, lng: d.lastLocation!.lng }));

  if (availableDrivers.length === 0) return;

  const payload = JSON.stringify({ type: 'nearby_drivers', payload: availableDrivers });

  riders.forEach((rider, riderId) => {
    // Only send to idle riders not currently in an active trip
    if (!activeTrips.has(riderId) && rider.ws.readyState === WebSocket.OPEN) {
      rider.ws.send(payload);
    }
  });
}, 5_000);

// ─── Start Server ─────────────────────────────────────────────────────────────

const PORT = process.env.PORT ?? 8080;
server.listen(PORT, () => {
  console.log(`Realtime Server listening on port ${PORT}`);
  console.log('\n─── DEV TOKENS (Expo app lo copy-paste cheyandi) ───────────────');
  console.log(`RIDER  TOKEN: ${FIXED_TOKENS.rider}`);
  console.log(`DRIVER TOKEN: ${FIXED_TOKENS.driver}`);
  console.log('────────────────────────────────────────────────────────────────\n');
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