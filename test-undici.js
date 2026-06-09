import { Expo } from 'expo-server-sdk';
const expo = new Expo();
expo.sendPushNotificationsAsync([{
  to: 'ExponentPushToken[xxxxxxxxxxxxxxxxxxxxxx]',
  title: 'Test',
  body: 'Testing undici bug'
}]).then(console.log).catch(console.error);
