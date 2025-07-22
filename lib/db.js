import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Connection pool settings
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

export async function query(text, params) {
  const start = Date.now();
  const client = await pool.connect();

  try {
    const result = await client.query(text, params);
    const duration = Date.now() - start;
    console.log("Executed query", { text, duration, rows: result.rowCount });
    return result;
  } catch (error) {
    console.error("Database query error:", error);
    throw error;
  } finally {
    client.release();
  }
}

// For getting database statistics
export async function getStats() {
  try {
    const result = await query(`
      SELECT 
        (SELECT COUNT(*) FROM trips_raw) as total_raw_trips,
        (SELECT COUNT(*) FROM stations) as total_stations,
        (SELECT COUNT(*) FROM station_days) as total_station_days,
        (SELECT COUNT(DISTINCT community_area) FROM stations WHERE community_area IS NOT NULL) as unique_community_areas,
        (SELECT MIN(year * 10000 + month * 100 + day) FROM station_days) as earliest_day,
        (SELECT MAX(year * 10000 + month * 100 + day) FROM station_days) as latest_day
    `);
    return result.rows[0];
  } catch (error) {
    console.error("Error getting stats:", error);
    return null;
  }
}

export default pool;
