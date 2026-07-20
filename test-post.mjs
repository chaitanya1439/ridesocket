import fetch from 'node-fetch';

const RIDER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJpZGVyLTAwMSIsInJvbGUiOiJyaWRlciJ9.pz5qZubhjBOCuM-BwbaImq21Hfm-4Iu_W4NF3JL2_ig';

async function test() {
  try {
    const res = await fetch('https://real.shelteric.com/api/request-ride', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${RIDER_TOKEN}`
      },
      body: JSON.stringify({
        riderId: 'rider-001',
        pickupLocation: { lat: 17.385, lng: 78.4867 },
        dropLocation: { lat: 17.4, lng: 78.5 },
        fare: 150,
        vehicleType: 'Bike'
      })
    });
    console.log(res.status, await res.text());
  } catch (err) {
    console.error(err);
  }
}

test();
