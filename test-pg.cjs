const { Client } = require('pg');
const client = new Client({
  connectionString: 'postgresql://postgres:RidegoPassword123!@ridego-db.cmbwkyg28hi2.us-east-1.rds.amazonaws.com:5432/postgres'
});
client.connect()
  .then(() => {
    console.log("Connected successfully");
    client.end();
  })
  .catch(err => console.error("Connection error:", err));
