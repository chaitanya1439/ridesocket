import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';

const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:RidegoPassword123!@ridego-db.cmbwkyg28hi2.us-east-1.rds.amazonaws.com:5432/postgres';
const pool = new pg.Pool({ connectionString, ssl: { rejectUnauthorized: false } });
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter } as any);

async function main() {
  const trips = await prisma.trip.findMany({ take: 10, orderBy: { createdAt: 'desc' } });
  console.log("Recent Trips:");
  console.dir(trips, { depth: null });
}
main().then(() => prisma.$disconnect()).catch(e => { console.error(e); prisma.$disconnect(); });
