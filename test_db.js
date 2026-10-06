import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';
const connectionString = 'postgresql://postgres:RidegoPassword123!@ridego-db.cmbwkyg28hi2.us-east-1.rds.amazonaws.com:5432/postgres';
const pool = new pg.Pool({ connectionString, ssl: { rejectUnauthorized: false } });
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });
async function test() {
    try {
        const user = await prisma.user.findFirst();
        if (!user) {
            console.log("No users found");
            return;
        }
        const dbTrip = await prisma.trip.create({
            data: {
                riderId: user.userId,
                status: 'scheduled',
                isScheduled: true,
                scheduledTime: new Date(Date.now() + 3600000),
                pickupLat: 17.3850,
                pickupLng: 78.4867,
            }
        });
        console.log("Success! Trip ID:", dbTrip.id);
    }
    catch (err) {
        console.error("Prisma error:", err);
    }
    finally {
        await prisma.$disconnect();
    }
}
test();
//# sourceMappingURL=test_db.js.map