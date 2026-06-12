import type { WebSocket } from 'ws';

// ─── Location ────────────────────────────────────────────────────────────────

export interface Location {
  lat: number;
  lng: number;
}

// ─── Client roles & statuses ─────────────────────────────────────────────────

export type ClientRole = 'rider' | 'driver';
export type DriverStatus = 'available' | 'busy' | 'offline';
export type TripStatus = 'accepted' | 'arrived' | 'in_progress' | 'completed' | 'cancelled';

// ─── In-memory client record ──────────────────────────────────────────────────

export interface ClientInfo {
  ws: WebSocket;
  role: ClientRole;
  id: string;
  /** Tracks ping/pong liveness for heartbeat pruning */
  isAlive: boolean;
  /** Epoch ms of last received message — used for idle-rider pruning */
  lastActivity: number;
  /** Driver-only: current operational status */
  status?: DriverStatus;
  /** Driver-only: most-recent GPS fix for proximity dispatch */
  lastLocation?: Location;
  /** Driver-only: vehicle type for dispatch filtering */
  vehicleType?: string;
}

// ─── Active trip record ───────────────────────────────────────────────────────

export interface TripRecord {
  riderId: string;
  driverId: string;
  status: TripStatus;
  pickupLocation?: Location;
  dropLocation?: Location;
  destinationLocation?: Location;
  fare?: number;
  vehicle?: string;
  vehicleType?: string;
  distance?: number;
  riderName?: string;
  otp?: string;
  [key: string]: unknown; // allow spread of additional ride-request fields
}

// ─── Decoded JWT payload ──────────────────────────────────────────────────────

export interface DecodedToken {
  /** Prefer `id`; fall back to `userId` for legacy tokens */
  id?: string;
  userId?: string;
  role?: ClientRole;
  iat?: number;
  exp?: number;
  [key: string]: unknown;
}

// ─── Demand heatmap ───────────────────────────────────────────────────────────

export interface Hotspot {
  lat: number;
  lng: number;
  intensity: number;
  surge: number;
}

// ─── WebSocket message payloads (client → server) ────────────────────────────

export interface RideRequestPayload {
  pickupLocation?: Location;
  dropLocation?: Location;
  destinationLocation?: Location;
  fare?: number;
  vehicle?: string;
  vehicleType?: string;
  distance?: number;
  riderName?: string;
  parcelDetails?: any;
}

export interface AuthMessage {
  type: 'auth';
  role: ClientRole;
  id?: string;
  vehicleType?: string;
}

export interface DriverStatusMessage {
  type: 'driver_status';
  status: DriverStatus;
}

export interface RideRequestMessage {
  type: 'ride_request';
  payload?: RideRequestPayload;
  // Legacy flat fields (kept for backward compat)
  pickupLocation?: Location;
  dropLocation?: Location;
  destinationLocation?: Location;
  fare?: number;
  vehicle?: string;
  vehicleType?: string;
  distance?: number;
  riderName?: string;
  parcelDetails?: any;
}

export interface RideAcceptMessage {
  type: 'ride_accept';
  riderId: string;
  payload?: Partial<TripRecord>;
}

export interface RideRejectMessage {
  type: 'ride_reject';
  riderId: string;
}

export interface RideCancelMessage {
  type: 'ride_cancel';
  riderId?: string; // The rider whose ride is being cancelled. Optional for riders (can use their own ID).
  reason?: string;
}

export interface LocationUpdateMessage {
  type: 'location_update';
  location: Location;
  /** Optional — server will look it up from activeTrips when omitted */
  riderId?: string;
}

export interface TripStatusUpdateMessage {
  type: 'trip_status_update';
  riderId: string;
  status: TripStatus;
}

export interface ChatMessage {
  type: 'CHAT_MESSAGE' | 'chat_message';
  /** Recipient ID */
  to?: string;
  toId?: string;
  message?: string;
  text?: string;
}

export interface GetDemandHeatmapMessage {
  type: 'get_demand_heatmap';
}

/**
 * Client sends its Expo Push Token after acquiring notification permissions.
 * The server stores it in the push token registry for future push dispatches.
 */
export interface RegisterPushTokenMessage {
  type: 'register_push_token';
  pushToken: string;
}

/**
 * Client requests its push token be removed (e.g., on logout).
 */
export interface UnregisterPushTokenMessage {
  type: 'unregister_push_token';
}

export interface PingMessage {
  type: 'ping';
}

export type InboundMessage =
  | AuthMessage
  | DriverStatusMessage
  | RideRequestMessage
  | RideAcceptMessage
  | RideRejectMessage
  | RideCancelMessage
  | LocationUpdateMessage
  | TripStatusUpdateMessage
  | ChatMessage
  | GetDemandHeatmapMessage
  | RegisterPushTokenMessage
  | UnregisterPushTokenMessage
  | PingMessage;

// ─── WebSocket message payloads (server → client) ────────────────────────────

export interface AuthSuccessMessage {
  type: 'auth_success';
  id: string;
  role: ClientRole;
}

export interface SyncStateMessage {
  type: 'sync_state';
  payload: TripRecord;
}

export interface NewRideRequestMessage {
  type: 'new_ride_request';
  payload: RideRequestPayload & { riderId: string };
}

export interface RideAcceptedMessage {
  type: 'ride_accepted';
  payload: TripRecord;
}

export interface DriverLocationMessage {
  type: 'driver_location';
  payload: { driverId: string; location: Location };
}

export interface TripStatusChangedMessage {
  type: 'trip_status_changed';
  payload: { driverId: string; status: TripStatus };
}

export interface DemandHeatmapMessage {
  type: 'demand_heatmap';
  payload: Hotspot[];
}

export interface NearbyDriversMessage {
  type: 'nearby_drivers';
  payload: Array<{ id: string } & Location>;
}

export interface OutboundChatMessage {
  type: 'CHAT_MESSAGE';
  from: string;
  message: string;
  timestamp: string;
  payload: { fromId: string; text: string; timestamp: string };
}

export interface PushTokenAckMessage {
  type: 'push_token_registered';
  success: boolean;
}

export type OutboundMessage =
  | AuthSuccessMessage
  | SyncStateMessage
  | NewRideRequestMessage
  | RideAcceptedMessage
  | DriverLocationMessage
  | TripStatusChangedMessage
  | DemandHeatmapMessage
  | NearbyDriversMessage
  | OutboundChatMessage
  | PushTokenAckMessage;