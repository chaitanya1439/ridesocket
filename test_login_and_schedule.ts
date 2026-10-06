import jwt from 'jsonwebtoken';

async function test() {
  // 1. Create fake Firebase token
  const fakeFirebaseToken = jwt.sign(
    { phone_number: '+919999999999', user_id: 'test-user-123' },
    'fake-secret' // Secret doesn't matter because the server uses jwt.decode!
  );

  // 2. Login to get internal JWT
  console.log("Logging in...");
  const loginRes = await fetch('https://real.shelteric.com/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token: fakeFirebaseToken, role: 'rider' })
  });
  const loginData = await loginRes.json();
  
  if (!loginData.token) {
    console.error("Login failed:", loginData);
    return;
  }
  console.log("Login successful! Token:", loginData.token);

  // 3. Test Schedule Ride
  console.log("Scheduling ride...");
  const scheduleRes = await fetch('https://real.shelteric.com/api/schedule-ride', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${loginData.token}`
    },
    body: JSON.stringify({
      scheduledTime: new Date(Date.now() + 60 * 60 * 1000).toISOString(), // 1 hour from now
      pickupLocation: { lat: 17.3850, lng: 78.4867 },
      dropLocation: { lat: 17.4260, lng: 78.4601 },
      fare: 150,
      vehicleType: 'bike',
      riderName: 'Test Rider',
      distance: 5,
      pickupAddress: 'Charminar',
      dropAddress: 'Hussain Sagar'
    })
  });
  
  const scheduleData = await scheduleRes.json();
  console.log("Schedule Response:", scheduleData);
}

test();
