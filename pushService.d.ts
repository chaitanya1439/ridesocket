import type { ExpoPushMessage, ExpoPushTicket } from 'expo-server-sdk';
/**
 * Register or update the Expo Push Token for a given user.
 * Called when a client sends their push token after notification permission grant.
 */
export declare function registerPushToken(userId: string, pushToken: string): boolean;
/**
 * Remove the push token when a user logs out or unregisters.
 */
export declare function unregisterPushToken(userId: string): void;
/**
 * Retrieve the push token for a specific user.
 */
export declare function getPushToken(userId: string): string | undefined;
/**
 * Retrieve all registered push tokens (useful for broadcast scenarios).
 */
export declare function getAllPushTokens(): Map<string, string>;
export interface RideRequestNotification {
    riderId: string;
    riderName?: string | undefined;
    pickupAddress?: string | undefined;
    dropAddress?: string | undefined;
    fare?: number | undefined;
    distance?: number | undefined;
    vehicleType?: string | undefined;
    pickupLat?: number | undefined;
    pickupLng?: number | undefined;
    dropLat?: number | undefined;
    dropLng?: number | undefined;
}
export interface RideAcceptedNotification {
    driverId: string;
    driverName?: string | undefined;
    vehicleInfo?: string | undefined;
    estimatedArrival?: number | undefined;
}
export interface TripStatusNotification {
    tripId?: string | undefined;
    status: string;
    message: string;
}
/**
 * Send a push notification to a specific user by userId.
 *
 * @param targetUserId - The user to send the notification to
 * @param title        - Notification title
 * @param body         - Notification body text
 * @param data         - Custom data payload (received in the app's notification handler)
 * @param options      - Optional overrides (sound, badge, priority, etc.)
 *
 * @returns The push ticket from Expo, or null if the user has no registered token.
 */
export declare function sendPushNotification(targetUserId: string, title: string, body: string, data?: Record<string, unknown>, options?: Partial<ExpoPushMessage>): Promise<ExpoPushTicket | null>;
/**
 * Send push notifications to multiple users at once.
 * Expo SDK handles batching internally (up to 100 per chunk).
 */
export declare function sendBatchNotifications(notifications: Array<{
    userId: string;
    title: string;
    body: string;
    data?: Record<string, unknown>;
}>): Promise<Map<string, ExpoPushTicket | null>>;
/**
 * Notify a driver about a new ride request.
 */
export declare function notifyDriverOfRideRequest(driverId: string, rideInfo: RideRequestNotification): Promise<ExpoPushTicket | null>;
/**
 * Notify a rider that a driver has accepted their ride.
 */
export declare function notifyRiderOfAcceptance(riderId: string, acceptInfo: RideAcceptedNotification): Promise<ExpoPushTicket | null>;
/**
 * Notify a user about a trip status change (arrived, started, completed, etc.)
 */
export declare function notifyTripStatusChange(userId: string, statusInfo: TripStatusNotification): Promise<ExpoPushTicket | null>;
//# sourceMappingURL=pushService.d.ts.map