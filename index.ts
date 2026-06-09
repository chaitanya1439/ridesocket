// NOTE: The expo-server-sdk uses `import { fetch } from 'undici'` internally,
// NOT `globalThis.fetch`. The Node built-in dispatcher incompatibility is fixed
// by passing an explicit undici Agent as `httpAgent` in pushService.ts.


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
  RideRejectMessage,
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

/**
 * Stores pending ride requests so drivers reconnecting from background
 * can receive missed broadcasts. Key: riderId
 */
const pendingRequests = new Map<string, RideRequestPayload & { timestamp: number; riderId: string }>();

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
  for (const [riderId, req] of pendingRequests.entries()) {
    if (now - req.timestamp > 60_000) {
      pendingRequests.delete(riderId);
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
      decoded = jwt.verify(token, JWT_SECRET) as DecodedToken;
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
        };

        if (data.role === 'driver') {
          // Default to 'available' for new drivers so they can receive rides immediately.
          // Previously defaulted to 'offline', which silently blocked all dispatches.
          newClient.status = existingDriver?.status ?? 'available';
          if (existingDriver?.lastLocation) {
            newClient.lastLocation = existingDriver.lastLocation;
          }
        }

        setClientInfo(ws, newClient);

        if (data.role === 'rider') {
          riders.set(newClient.id, newClient);
          console.log(`[Auth] ✓ Rider REGISTERED in memory — id: ${newClient.id}`);
          console.log(`[Auth]   Total riders online: ${riders.size}`);
        } else {
          drivers.set(newClient.id, newClient);
          console.log(`[Auth] ✓ Driver REGISTERED in memory — id: ${newClient.id}, status: ${newClient.status}`);
          console.log(`[Auth]   Total drivers online: ${drivers.size}`);
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

        // Deliver pending ride requests to reconnecting/newly-online drivers
        if (data.role === 'driver' && (newClient as any).status !== 'offline') {
          for (const [riderId, req] of pendingRequests.entries()) {
            if (Date.now() - req.timestamp <= 60_000) {
              ws.send(JSON.stringify({
                type: 'new_ride_request',
                payload: { ...req, riderId }
              }));
            }
          }
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
            for (const [riderId, req] of pendingRequests.entries()) {
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
          return p;
        })();

        console.log(`Ride request from rider ${client.id}:`, ridePayload);

        // Store pending request for offline/backgrounded drivers
        pendingRequests.set(client.id, { ...ridePayload, riderId: client.id, timestamp: Date.now() });

        const pickupLoc: Location | undefined = ridePayload.pickupLocation;
        let matchedCount = 0;

        drivers.forEach((driver) => {
          if (driver.status !== 'available' || driver.ws.readyState !== WebSocket.OPEN) {
            console.log(`[Dispatch] Skipped Driver ${driver.id} - status: ${driver.status}, ws.readyState: ${driver.ws.readyState === WebSocket.OPEN ? 'OPEN' : driver.ws.readyState}`);
            return;
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
            pickupAddress: pickupLoc ? `${pickupLoc.lat.toFixed(4)}, ${pickupLoc.lng.toFixed(4)}` : undefined,
            fare: ridePayload.fare,
            distance: ridePayload.distance ? parseFloat(String(ridePayload.distance)) : undefined,
            vehicleType: ridePayload.vehicleType ?? ridePayload.vehicle,
            pickupLat: pickupLoc?.lat,
            pickupLng: pickupLoc?.lng,
          }).catch((err) => console.error(`[Push] Error notifying driver ${driver.id}:`, err));
        });
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
        pendingRequests.delete(data.riderId);

        const riderToNotify = riders.get(data.riderId);
        if (riderToNotify?.ws.readyState === WebSocket.OPEN) {
          riderToNotify.ws.send(JSON.stringify({ type: 'ride_accepted', payload: tripRecord }));
        }

        // Push notification to rider: "Your ride has been accepted!"
        notifyRiderOfAcceptance(data.riderId, {
          driverId: client.id,
        }).catch((err) => console.error(`[Push] Error notifying rider ${data.riderId}:`, err));

        // Notify OTHER drivers to remove this request (since it's been accepted)
        drivers.forEach((otherDriver) => {
          if (otherDriver.id !== client.id && otherDriver.ws.readyState === WebSocket.OPEN) {
            otherDriver.ws.send(JSON.stringify({
              type: 'ride_request_cancelled',
              payload: { riderId: data.riderId, reason: 'accepted_by_another' },
            }));
          }
        });
        break;
      }

      // ── Ride reject ─────────────────────────────────────────────────────────
      case 'ride_reject': {
        if (!client || client.role !== 'driver') break;

        console.log(`Driver ${client.id} rejected ride from rider ${data.riderId}`);

        // Notify the rider that this specific driver rejected
        const riderForReject = riders.get(data.riderId);
        if (riderForReject?.ws.readyState === WebSocket.OPEN) {
          riderForReject.ws.send(JSON.stringify({
            type: 'ride_rejected',
            payload: { driverId: client.id },
          }));
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

        // Push notification to rider about trip status changes
        const statusMessages: Record<string, string> = {
          arrived: 'Your driver has arrived at the pickup point!',
          in_progress: 'Your ride has started. Enjoy the journey!',
          completed: 'Ride completed! Thank you for riding with us.',
          cancelled: 'Your ride has been cancelled.',
        };
        const msg = statusMessages[data.status];
        if (msg) {
          notifyTripStatusChange(data.riderId, {
            status: data.status,
            message: msg,
          }).catch((err) => console.error(`[Push] Error notifying trip status:`, err));
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
app.post('/api/request-ride', (req, res) => {
  // Authenticate the request
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization header required' });
    return;
  }

  let decoded: { id?: string; userId?: string; role?: string };
  try {
    decoded = jwt.verify(authHeader.slice(7), JWT_SECRET) as typeof decoded;
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
  pendingRequests.set(riderId, newRequest);

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
    jwt.verify(authHeader.slice(7), JWT_SECRET);
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
  console.log(`\n══════════════════════════════════════════════════════════════`);
  console.log(`  Realtime Server listening on port ${PORT}`);
  console.log(`══════════════════════════════════════════════════════════════`);
  console.log(`\n─── JWT SECRET ─────────────────────────────────────────────────`);
  console.log(`  ${JWT_SECRET}`);
  console.log(`\n─── DEV TOKENS (Expo app lo copy-paste cheyandi) ───────────────`);
  console.log(`  RIDER  TOKEN: ${FIXED_TOKENS.rider}`);
  console.log(`  DRIVER TOKEN: ${FIXED_TOKENS.driver}`);
  console.log(`\n─── VERIFY: Both tokens should decode correctly ────────────────`);
  try {
    const rVerify = jwt.verify(FIXED_TOKENS.rider, JWT_SECRET) as any;
    console.log(`  Rider  token decoded: { id: '${rVerify.id}', role: '${rVerify.role}' } ✓`);
  } catch (e: any) {
    console.log(`  Rider  token verification FAILED: ${e.message} ✗`);
  }
  try {
    const dVerify = jwt.verify(FIXED_TOKENS.driver, JWT_SECRET) as any;
    console.log(`  Driver token decoded: { id: '${dVerify.id}', role: '${dVerify.role}' } ✓`);
  } catch (e: any) {
    console.log(`  Driver token verification FAILED: ${e.message} ✗`);
  }
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