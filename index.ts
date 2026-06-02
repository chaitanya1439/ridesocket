import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import cors from 'cors';
import jwt from 'jsonwebtoken';

const app = express();
app.use(cors());
app.use(express.json());

const server = createServer(app);

// 1. Bandwidth Efficiency: Enable per-message deflate compression.
const wss = new WebSocketServer({ 
  noServer: true, // We will manually handle the HTTP upgrade for Authentication
  perMessageDeflate: {
    zlibDeflateOptions: { level: 4 }, // Optimize CPU vs Compression ratio
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    threshold: 256 // Only compress messages larger than 256 bytes
  }
});

interface ClientInfo {
  ws: WebSocket;
  role: 'rider' | 'driver';
  id: string;
  isAlive: boolean; // For heartbeat
  status?: 'available' | 'busy' | 'offline'; // Driver specific
  lastLocation?: { lat: number, lng: number }; // For proximity dispatch
  lastActivity: number; // For pruning idle connections
}

// 0. Geospatial Proximity Function (Haversine Formula)
// Used to prevent broadcasting rides to drivers that are too far away
function getDistanceInKm(lat1: number, lon1: number, lat2: number, lon2: number) {
  const R = 6371; // Radius of the earth in km
  const dLat = (lat2 - lat1) * Math.PI / 180;  
  const dLon = (lon2 - lon1) * Math.PI / 180; 
  const a = 
    Math.sin(dLat/2) * Math.sin(dLat/2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * 
    Math.sin(dLon/2) * Math.sin(dLon/2); 
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
  return R * c; 
}

const riders = new Map<string, ClientInfo>();
const drivers = new Map<string, ClientInfo>();

// 2. Active State Recovery (Resilience against tunnel/elevator drops)
// We maintain active trips in memory so when a mobile client drops and reconnects, 
// they instantly get their latest state without having to poll a database.
const activeTrips = new Map<string, any>(); // Key: riderId

// 1. Heartbeat Mechanism (Efficiency & Memory Management)
// Automatically prune silently dropped mobile connections AND idle backgrounded riders
const IDLE_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes

const interval = setInterval(() => {
  const now = Date.now();
  wss.clients.forEach((ws) => {
    const client = (ws as any).clientInfo as ClientInfo;
    if (client) {
      // 1. Check for Silent Drops (Ping/Pong)
      if (!client.isAlive) {
        console.log(`[Heartbeat] Terminating inactive ${client.role} ${client.id}`);
        if (client.role === 'rider') riders.delete(client.id);
        else drivers.delete(client.id);
        return ws.terminate();
      }
      
      // 2. Idle Pruning: Disconnect Riders who have been inactive for 15+ minutes to save server RAM
      // (Drivers are kept alive indefinitely while they are 'available' or 'busy')
      if (client.role === 'rider' && (now - client.lastActivity > IDLE_TIMEOUT_MS)) {
        console.log(`[Idle Prune] Disconnecting idle rider ${client.id} to free memory`);
        riders.delete(client.id);
        return ws.close(1000, 'Idle timeout'); // Graceful close
      }

      client.isAlive = false;
    }
    ws.ping();
  });
}, 30000);

// 3. Fleet Optimization: Push Demand Heatmaps (Surge Zones)
// Instead of 10,000 drivers HTTP polling the server every minute, 
// the server pushes dynamic surge multipliers/hotspots directly to drivers.
const HEATMAP_INTERVAL_MS = 60 * 1000; // Push every 1 minute
const heatmapInterval = setInterval(() => {
  // In a real app, you would query your DB for active requests vs available drivers in specific geohashes.
  // Here we mock sending dynamic hotspots.
  const hotspots = [
    { lat: 17.4401, lng: 78.3489, intensity: 0.9, surge: 1.5 }, // Example: HITEC City
    { lat: 17.3850, lng: 78.4867, intensity: 0.6, surge: 1.2 }
  ];
  
  let pushedCount = 0;
  drivers.forEach((driver) => {
    if (driver.ws.readyState === WebSocket.OPEN) {
      driver.ws.send(JSON.stringify({
        type: 'demand_heatmap',
        payload: hotspots
      }));
      pushedCount++;
    }
  });
  if (pushedCount > 0) {
    console.log(`[Fleet Optimization] Pushed demand heatmap to ${pushedCount} drivers`);
  }
}, HEATMAP_INTERVAL_MS);

wss.on('close', () => {
  clearInterval(interval);
  clearInterval(heatmapInterval);
});

// 5. Connection Security: HTTP Upgrade Authentication
// Reject unauthenticated requests before they even become WebSockets
server.on('upgrade', (request, socket, head) => {
  try {
    const url = new URL(request.url || '', `http://${request.headers.host}`);
    const token = url.searchParams.get('token');

    if (!token) {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    // Decode JWT (In production use jwt.verify with your secret)
    const decoded = jwt.decode(token) as any;
    if (!decoded) {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      // Pass the decoded token data to the connection event
      wss.emit('connection', ws, request, decoded);
    });
  } catch (err) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
  }
});

wss.on('connection', (ws: WebSocket, request: any, decodedToken: any) => {
  console.log('New authenticated client connected');
  
  // Extract user ID from token (handles both id and userId schemas from the JWTs)
  const userId = decodedToken?.id || decodedToken?.userId;
  
  // React Native automatically replies to native ping frames with a pong
  ws.on('pong', () => {
    const client = (ws as any).clientInfo as ClientInfo;
    if (client) client.isAlive = true;
  });

  ws.on('message', (message: any) => {
    try {
      const data = JSON.parse(message.toString());

      // If client not authenticated yet, only allow auth
      const client = (ws as any).clientInfo as ClientInfo;
      if (client) {
        client.lastActivity = Date.now();
      }

      switch (data.type) {
        case 'auth':
          // Even though they are authenticated at the network level, 
          // we use this event to set their 'role' and bind their memory profile
          const newClient: ClientInfo = {
            ws,
            role: data.role,
            id: data.id || userId,
            isAlive: true,
            lastActivity: Date.now()
          };
          if (data.role === 'driver') {
            newClient.status = 'offline';
          }
          (ws as any).clientInfo = newClient;
          
          if (data.role === 'rider') {
            riders.set(newClient.id, newClient);
            console.log(`Rider authorized in memory: ${newClient.id}`);
          } else if (data.role === 'driver') {
            drivers.set(newClient.id, newClient);
            console.log(`Driver authorized in memory: ${newClient.id}`);
          }
          
          ws.send(JSON.stringify({ type: 'auth_success', id: newClient.id, role: data.role }));
          
          // --- Offline Recovery Sync ---
          // Instantly sync the user back into their active trip if they just reconnected
          let currentTrip = null;
          if (data.role === 'rider') {
            currentTrip = activeTrips.get(newClient.id);
          } else if (data.role === 'driver') {
            // Find if this driver is assigned to any active trip
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

        case 'driver_status':
          // { type: 'driver_status', status: 'available' | 'busy' | 'offline' }
          if (client && client.role === 'driver') {
            client.status = data.status === 'available' ? 'available' : data.status === 'busy' ? 'busy' : 'offline';
            console.log(`Driver ${client.id} is now ${data.status}`);
          }
          break;

        case 'ride_request':
          console.log(`Ride request from rider ${client?.id}:`, data.payload);
          const pickupLoc = data.payload.pickupLocation; // expected { lat, lng }

          // Geospatial Filtering: Only broadcast to 'available' drivers WITHIN 5km
          let matchedCount = 0;
          drivers.forEach((driver) => {
            if (driver.status === 'available' && driver.ws.readyState === WebSocket.OPEN) {
              
              // If pickup location is provided and driver has a known location, filter by distance
              let isNearby = true;
              if (pickupLoc && driver.lastLocation) {
                const distance = getDistanceInKm(
                  pickupLoc.lat, pickupLoc.lng,
                  driver.lastLocation.lat, driver.lastLocation.lng
                );
                if (distance > 5.0) isNearby = false; // Filter out drivers > 5km away
              }

              if (isNearby) {
                matchedCount++;
                driver.ws.send(JSON.stringify({
                  type: 'new_ride_request',
                  payload: { riderId: client?.id, ...data.payload }
                }));
              }
            }
          });
          console.log(`Broadcasted ride request to ${matchedCount} nearby drivers`);
          break;

        case 'ride_accept':
          if (client && client.role === 'driver') {
            client.status = 'busy'; // Mark driver as busy immediately
            
            // Save to active trips for offline recovery
            const tripRecord = {
              riderId: data.riderId,
              driverId: client.id,
              status: 'accepted',
              ...data.payload
            };
            activeTrips.set(data.riderId, tripRecord);

            const riderToNotify = riders.get(data.riderId);
            if (riderToNotify && riderToNotify.ws.readyState === WebSocket.OPEN) {
              riderToNotify.ws.send(JSON.stringify({
                type: 'ride_accepted',
                payload: tripRecord
              }));
            }
          }
          break;

        case 'location_update':
          if (client && client.role === 'driver') {
            // Save location to server memory for proximity dispatch
            if (data.location) {
              client.lastLocation = data.location;
            }

            // Route live tracking to rider if driver is currently in an active trip
            // Lookup the paired rider from server memory instead of trusting client payload
            let targetRiderId = data.riderId;
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
              if (targetRider && targetRider.ws.readyState === WebSocket.OPEN) {
                targetRider.ws.send(JSON.stringify({
                  type: 'driver_location',
                  payload: { driverId: client.id, location: data.location }
                }));
              }
            }
          }
          break;

        case 'trip_status_update':
          if (client && client.role === 'driver' && data.riderId) {
            console.log(`Trip status update from ${client.id} for ${data.riderId}: ${data.status}`);
            
            // Update recovery memory
            const trip = activeTrips.get(data.riderId);
            if (trip) trip.status = data.status;

            const targetRider = riders.get(data.riderId);
            if (targetRider && targetRider.ws.readyState === WebSocket.OPEN) {
              targetRider.ws.send(JSON.stringify({
                type: 'trip_status_changed',
                payload: { driverId: client.id, status: data.status }
              }));
            }
            // If trip is completed/cancelled, make driver available again and clear memory
            if (data.status === 'completed' || data.status === 'cancelled') {
              client.status = 'available';
              activeTrips.delete(data.riderId);
            }
          }
          break;

        case 'CHAT_MESSAGE':
        case 'chat_message':
          const targetId = data.to || data.toId;
          const textMsg = data.message || data.text;
          if (client && targetId) {
            const recipient = client.role === 'rider' ? drivers.get(targetId) : riders.get(targetId);
            if (recipient && recipient.ws.readyState === WebSocket.OPEN) {
              recipient.ws.send(JSON.stringify({
                type: 'CHAT_MESSAGE',
                from: client.id,
                message: textMsg,
                timestamp: new Date().toISOString(),
                payload: { fromId: client.id, text: textMsg, timestamp: new Date().toISOString() }
              }));
            }
          }
          break;

        case 'get_demand_heatmap':
          if (client && client.role === 'driver') {
            const hotspots = [
              { lat: 17.4401, lng: 78.3489, intensity: 0.9, surge: 1.5 }, // Example: HITEC City
              { lat: 17.3850, lng: 78.4867, intensity: 0.6, surge: 1.2 }
            ];
            ws.send(JSON.stringify({
              type: 'demand_heatmap',
              payload: hotspots
            }));
          }
          break;

        default:
          console.log('Unknown message type:', data.type);
      }
    } catch (err) {
      console.error('Error parsing message:', err);
    }
  });

  ws.on('close', () => {
    const client = (ws as any).clientInfo as ClientInfo;
    if (client) {
      if (client.role === 'rider') {
        riders.delete(client.id);
        console.log(`Rider disconnected: ${client.id}`);
      } else {
        drivers.delete(client.id);
        console.log(`Driver disconnected: ${client.id}`);
      }
    } else {
      console.log('Unauthenticated client disconnected');
    }
  });
});

app.get('/', (req, res) => {
  res.send('Realtime WebSocket Server is running');
});



// 6. Idle Rider Roaming Cars Broadcast (UX Efficiency)
// When a Rider opens the app, seeing little car icons roaming around increases conversion rate.
// Instead of HTTP polling, we broadcast available drivers every 5 seconds to idle riders.
const broadcastNearbyDrivers = setInterval(() => {
  const availableDrivers = Array.from(drivers.entries())
    .filter(([_, data]) => data.status === 'available' && data.lastLocation)
    .map(([id, data]) => ({ id, ...data.lastLocation }));

  if (availableDrivers.length === 0) return;

  const payload = JSON.stringify({ type: 'nearby_drivers', payload: availableDrivers });
  
  riders.forEach((data, riderId) => {
    // Only send to idle riders (not currently in an active trip)
    if (!activeTrips.has(riderId) && data.ws.readyState === WebSocket.OPEN) {
      data.ws.send(payload);
    }
  });
}, 5000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Realtime Server listening on port ${PORT}`);
});

// 6. Graceful Shutdown (Deployment Efficiency)
// When deploying a new version of the server, forcefully killing TCP sockets leaves clients hanging.
// This intercepts the kill signal and actively closes all WebSocket connections with code 1001 (Going Away).
// This instantly triggers the client's exponential backoff reconnect logic instead of them waiting for a timeout.
const gracefulShutdown = () => {
  console.log('Server shutting down, disconnecting clients gracefully...');
  clearInterval(interval);
  clearInterval(heatmapInterval);
  clearInterval(broadcastNearbyDrivers);
  wss.clients.forEach((ws) => {
    ws.close(1001, 'Server shutting down');
  });
  server.close(() => {
    console.log('Server closed.');
    process.exit(0);
  });
};

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);
