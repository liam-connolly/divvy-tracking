import { Pool, PoolClient, QueryResult, QueryResultRow } from "pg";
import { DatabaseStats, CommunityAreaStats, StationWithStats } from "./types";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

export async function query<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const start = Date.now();
  const client: PoolClient = await pool.connect();

  try {
    const result = await client.query<T>(text, params);
    const duration = Date.now() - start;

    if (process.env.NODE_ENV === "development") {
      console.log("Executed query", {
        text: text.substring(0, 100) + "...",
        duration,
        rows: result.rowCount,
      });
    }

    return result;
  } catch (error) {
    console.error("Database query error:", error);
    throw error;
  } finally {
    client.release();
  }
}

export async function getStats(): Promise<DatabaseStats | null> {
  try {
    const result = await query<DatabaseStats>(`
      SELECT 
        (SELECT COUNT(*) FROM trips_raw) as total_raw_trips,
        (SELECT COUNT(*) FROM stations) as total_stations,
        (SELECT COUNT(*) FROM station_days) as total_station_days,
        (SELECT COUNT(DISTINCT community_area) FROM stations WHERE community_area IS NOT NULL) as unique_community_areas,
        (SELECT MIN(year * 10000 + month * 100 + day) FROM station_days) as earliest_day,
        (SELECT MAX(year * 10000 + month * 100 + day) FROM station_days) as latest_day
    `);

    return result.rows[0] || null;
  } catch (error) {
    console.error("Error getting stats:", error);
    return null;
  }
}

export async function getCommunityAreas(): Promise<CommunityAreaStats[]> {
  try {
    const result = await query<CommunityAreaStats>(`
      SELECT 
        s.community_area,
        s.community_area_name,
        COUNT(DISTINCT s.id) as station_count,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart), 0) as total_departures,
        COALESCE(SUM(sd.acoustic_arrive + sd.electric_arrive), 0) as total_arrivals
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE s.community_area IS NOT NULL
      GROUP BY s.community_area, s.community_area_name
      ORDER BY total_departures DESC
    `);

    return result.rows;
  } catch (error) {
    console.error("Error getting community areas:", error);
    return [];
  }
}

export async function getStationsByCommunityArea(
  communityArea: number,
  year?: number,
  month?: number
): Promise<StationWithStats[]> {
  try {
    let queryText = `
      SELECT 
        s.id,
        s.station_id,
        s.name,
        s.latitude,
        s.longitude,
        s.community_area,
        s.community_area_name,
        s.created_at,
        s.updated_at,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart), 0) as total_departures,
        COALESCE(SUM(sd.acoustic_arrive + sd.electric_arrive), 0) as total_arrivals,
        COALESCE(SUM(sd.electric_depart + sd.electric_arrive), 0) as electric_total,
        COALESCE(SUM(sd.acoustic_depart + sd.acoustic_arrive), 0) as acoustic_total
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE s.community_area = $1
    `;

    const params: any[] = [communityArea];

    if (year) {
      queryText += ` AND sd.year = $${params.length + 1}`;
      params.push(year);
    }

    if (month) {
      queryText += ` AND sd.month = $${params.length + 1}`;
      params.push(month);
    }

    queryText += `
      GROUP BY s.id, s.station_id, s.name, s.latitude, s.longitude, 
               s.community_area, s.community_area_name, s.created_at, s.updated_at
      ORDER BY total_departures DESC
    `;

    const result = await query<StationWithStats>(queryText, params);
    return result.rows;
  } catch (error) {
    console.error("Error getting stations by community area:", error);
    return [];
  }
}

// Test connection function
export async function testConnection(): Promise<boolean> {
  try {
    const client = await pool.connect();
    const result = await client.query("SELECT NOW()");
    console.log("✅ Database connection successful!");
    console.log("Current time from DB:", result.rows[0].now);

    // Test tables exist
    const tableCheck = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name IN ('stations', 'station_days', 'trips_raw')
      ORDER BY table_name
    `);

    if (tableCheck.rows.length === 3) {
      console.log(
        "✅ All tables exist:",
        tableCheck.rows.map((r) => r.table_name)
      );
    } else {
      console.log(
        "❌ Missing tables. Found:",
        tableCheck.rows.map((r) => r.table_name)
      );
    }

    client.release();
    return true;
  } catch (err) {
    console.error("❌ Database connection error:", err);
    return false;
  }
}

export default pool;
