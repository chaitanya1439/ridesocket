import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';
import Redis from 'ioredis';

const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:RidegoPassword123!@ridego-db.cmbwkyg28hi2.us-east-1.rds.amazonaws.com:5432/postgres';
const pool = new pg.Pool({ connectionString, ssl: { rejectUnauthorized: false } });
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

async function clean() {
  console.log('Cleaning Postgres database...');
  // Find all trips where riderId or driverId looks like a test ID
  const result = await prisma.trip.deleteMany({
    where: {
      OR: [
        { riderId: { startsWith: 'test-' } },
        { riderId: { startsWith: 'lt-' } },
        { riderId: { startsWith: 'br-' } },
        { riderId: { startsWith: 'ar-' } },
        { driverId: { startsWith: 'test-' } },
        { driverId: { startsWith: 'lt-' } },
        { driverId: { startsWith: 'bd-' } },
        { driverId: { startsWith: 'ad-' } },
      ]
    }
  });
  console.log(`Deleted ${result.count} test trips from Postgres.`);

  console.log('Cleaning Redis...');
  const keys = await redis.keys('*');
  let redisDeleted = 0;
  for (const key of keys) {
    if (
      key.includes('test-') ||
      key.includes('lt-') ||
      key.includes('br-') ||
      key.includes('ar-') ||
      key.includes('bd-') ||
      key.includes('ad-') ||
      key.includes('msts') // The run ID used in the last test
    ) {
      await redis.del(key);
      redisDeleted++;
    }
  }
  
  // Also clear the driver_locations geo index just in case test drivers are in there
  const drivers = await redis.zrange('driver_locations', 0, -1);
  let geoDeleted = 0;
  for (const d of drivers) {
    if (d.startsWith('test-') || d.startsWith('lt-') || d.startsWith('bd-') || d.startsWith('ad-')) {
      await redis.zrem('driver_locations', d);
      geoDeleted++;
    }
  }

  console.log(`Deleted ${redisDeleted} test keys and ${geoDeleted} test locations from Redis.`);
  
  await prisma.$disconnect();
  await pool.end();
  await redis.quit();
}

clean().catch(console.error);
