import { Expo } from 'expo-server-sdk';
import { Agent } from 'undici';
// ─── Expo Server SDK Instance ────────────────────────────────────────────────
// The Expo instance handles batching, retries, and rate-limiting automatically.
// We pass an explicit undici Agent as httpAgent so the SDK's fetch uses npm
// undici's own dispatcher instead of Node's built-in one (which has an
// incompatible onRequestStart interface on Node 22–25).
const expo = new Expo({
    accessToken: 'XEgdE51O6Llc2f4WeU1n6Gzo4LI2hkFSRXnbr1n6',
    httpAgent: new Agent(),
});
// ─── In-Memory Push Token Registry ───────────────────────────────────────────
// Maps userId → Expo Push Token (e.g., "ExponentPushToken[xxxx]")
// Production: Replace this with a database-backed store (PostgreSQL, Redis, etc.)
const pushTokenRegistry = new Map();
// ─── Token Management ────────────────────────────────────────────────────────
/**
 * Register or update the Expo Push Token for a given user.
 * Called when a client sends their push token after notification permission grant.
 */
export function registerPushToken(userId, pushToken) {
    if (!Expo.isExpoPushToken(pushToken)) {
        console.error(`[Push] Invalid Expo Push Token for ${userId}: ${pushToken}`);
        return false;
    }
    pushTokenRegistry.set(userId, pushToken);
    console.log(`[Push] Registered token for ${userId}: ${pushToken}`);
    return true;
}
/**
 * Remove the push token when a user logs out or unregisters.
 */
export function unregisterPushToken(userId) {
    pushTokenRegistry.delete(userId);
    console.log(`[Push] Unregistered token for ${userId}`);
}
/**
 * Retrieve the push token for a specific user.
 */
export function getPushToken(userId) {
    return pushTokenRegistry.get(userId);
}
/**
 * Retrieve all registered push tokens (useful for broadcast scenarios).
 */
export function getAllPushTokens() {
    return pushTokenRegistry;
}
// ─── Send Push Notification ──────────────────────────────────────────────────
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
export async function sendPushNotification(targetUserId, title, body, data = {}, options = {}) {
    const pushToken = pushTokenRegistry.get(targetUserId);
    if (!pushToken) {
        console.warn(`[Push] No push token registered for user ${targetUserId}`);
        return null;
    }
    if (!Expo.isExpoPushToken(pushToken)) {
        console.error(`[Push] Token for ${targetUserId} is no longer valid: ${pushToken}`);
        pushTokenRegistry.delete(targetUserId);
        return null;
    }
    const message = {
        to: pushToken,
        title,
        body,
        data: data,
        sound: 'default',
        priority: 'high',
        // Android-specific: use a custom channel for ride alerts
        channelId: 'ride-alerts',
        ...options,
    };
    try {
        const chunks = expo.chunkPushNotifications([message]);
        const tickets = [];
        for (const chunk of chunks) {
            const ticketChunk = await expo.sendPushNotificationsAsync(chunk);
            tickets.push(...ticketChunk);
        }
        const ticket = tickets[0];
        if (!ticket) {
            console.warn(`[Push] No ticket returned for ${targetUserId}`);
            return null;
        }
        if (ticket.status === 'ok') {
            console.log(`[Push] ✓ Notification sent to ${targetUserId}`);
        }
        else {
            console.error(`[Push] ✗ Failed for ${targetUserId}:`, ticket.message);
            // Auto-cleanup invalid tokens
            if (ticket.details?.error === 'DeviceNotRegistered' ||
                ticket.details?.error === 'InvalidCredentials') {
                pushTokenRegistry.delete(targetUserId);
                console.log(`[Push] Removed stale token for ${targetUserId}`);
            }
        }
        return ticket;
    }
    catch (error) {
        console.error(`[Push] Network error sending to ${targetUserId}:`, error);
        return null;
    }
}
// ─── Batch Notifications ─────────────────────────────────────────────────────
/**
 * Send push notifications to multiple users at once.
 * Expo SDK handles batching internally (up to 100 per chunk).
 */
export async function sendBatchNotifications(notifications) {
    const results = new Map();
    const messages = [];
    const userIdOrder = [];
    for (const notif of notifications) {
        const pushToken = pushTokenRegistry.get(notif.userId);
        if (!pushToken || !Expo.isExpoPushToken(pushToken)) {
            results.set(notif.userId, null);
            continue;
        }
        messages.push({
            to: pushToken,
            title: notif.title,
            body: notif.body,
            data: notif.data ?? {},
            sound: 'default',
            priority: 'high',
            channelId: 'ride-alerts',
        });
        userIdOrder.push(notif.userId);
    }
    if (messages.length === 0)
        return results;
    try {
        const chunks = expo.chunkPushNotifications(messages);
        let ticketIndex = 0;
        for (const chunk of chunks) {
            const ticketChunk = await expo.sendPushNotificationsAsync(chunk);
            for (const ticket of ticketChunk) {
                const userId = userIdOrder[ticketIndex];
                if (userId !== undefined) {
                    results.set(userId, ticket);
                }
                ticketIndex++;
            }
        }
        console.log(`[Push] Batch sent: ${messages.length} notifications`);
    }
    catch (error) {
        console.error('[Push] Batch send failed:', error);
    }
    return results;
}
// ─── Convenience Methods ─────────────────────────────────────────────────────
/**
 * Notify a driver about a new ride request.
 */
export async function notifyDriverOfRideRequest(driverId, rideInfo) {
    const fare = rideInfo.fare != null ? `₹${rideInfo.fare}` : 'N/A';
    const pickup = rideInfo.pickupAddress ?? 'Nearby location';
    return sendPushNotification(driverId, '🚗 New Ride Request!', `Pickup: ${pickup} • Fare: ${fare}`, {
        type: 'new_ride_request',
        riderId: rideInfo.riderId,
        ...(rideInfo.riderName != null ? { riderName: rideInfo.riderName } : {}),
        ...(rideInfo.pickupAddress != null ? { pickupAddress: rideInfo.pickupAddress } : {}),
        ...(rideInfo.fare != null ? { fare: rideInfo.fare } : {}),
        ...(rideInfo.distance != null ? { distance: rideInfo.distance } : {}),
        ...(rideInfo.vehicleType != null ? { vehicleType: rideInfo.vehicleType } : {}),
        ...(rideInfo.pickupLat != null ? { pickupLat: rideInfo.pickupLat } : {}),
        ...(rideInfo.pickupLng != null ? { pickupLng: rideInfo.pickupLng } : {}),
        ...(rideInfo.dropLat != null ? { dropLat: rideInfo.dropLat } : {}),
        ...(rideInfo.dropLng != null ? { dropLng: rideInfo.dropLng } : {}),
        ...(rideInfo.dropAddress != null ? { dropAddress: rideInfo.dropAddress } : {}),
    });
}
/**
 * Notify a rider that a driver has accepted their ride.
 */
export async function notifyRiderOfAcceptance(riderId, acceptInfo) {
    const eta = acceptInfo.estimatedArrival != null
        ? `Arriving in ~${acceptInfo.estimatedArrival} min`
        : 'On the way!';
    return sendPushNotification(riderId, '✅ Ride Accepted!', `Your driver is ${eta}`, {
        type: 'ride_accepted',
        driverId: acceptInfo.driverId,
        ...(acceptInfo.driverName != null ? { driverName: acceptInfo.driverName } : {}),
        ...(acceptInfo.vehicleInfo != null ? { vehicleInfo: acceptInfo.vehicleInfo } : {}),
        ...(acceptInfo.estimatedArrival != null ? { estimatedArrival: acceptInfo.estimatedArrival } : {}),
    });
}
/**
 * Notify a user about a trip status change (arrived, started, completed, etc.)
 */
export async function notifyTripStatusChange(userId, statusInfo) {
    const statusEmoji = {
        arrived: '📍',
        in_progress: '🚗',
        completed: '🎉',
        cancelled: '❌',
    };
    const emoji = statusEmoji[statusInfo.status] ?? '📱';
    return sendPushNotification(userId, `${emoji} Trip Update`, statusInfo.message, {
        type: 'trip_status_change',
        status: statusInfo.status,
        message: statusInfo.message,
        ...(statusInfo.tripId != null ? { tripId: statusInfo.tripId } : {}),
    });
}
//# sourceMappingURL=pushService.js.map