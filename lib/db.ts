import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import { DatabaseStats, CommunityAreaStats, StationWithStats } from './types';

const pool = new Pool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: { rejectUnauthorized: false },
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

    if (process.env.NODE_ENV === 'development') {
      console.log('Executed query', {
        text: text.substring(0, 100) + '...',
        duration,
        rows: result.rowCount,
      });
    }

    return result;
  } catch (error) {
    console.error('Database query error:', error);
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
    console.error('Error getting stats:', error);
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
    console.error('Error getting community areas:', error);
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
    console.error('Error getting stations by community area:', error);
    return [];
  }
}

// Test connection function
export async function testConnection(): Promise<boolean> {
  try {
    const client = await pool.connect();
    const result = await client.query('SELECT NOW()');
    console.log('✅ Database connection successful!');
    console.log('Current time from DB:', result.rows[0].now);

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
        '✅ All tables exist:',
        tableCheck.rows.map((r) => r.table_name)
      );
    } else {
      console.log(
        '❌ Missing tables. Found:',
        tableCheck.rows.map((r) => r.table_name)
      );
    }

    client.release();
    return true;
  } catch (err) {
    console.error('❌ Database connection error:', err);
    return false;
  }
}

// Get all stations grouped by name (handles duplicates)
export async function getAllStationsGrouped(): Promise<StationWithStats[]> {
  try {
    const result = await query<StationWithStats>(`
      SELECT 
        MIN(s.id) as id,
        MIN(s.station_id) as station_id,
        s.name,
        AVG(s.latitude) as latitude,
        AVG(s.longitude) as longitude,
        MIN(s.community_area) as community_area,
        MIN(s.community_area_name) as community_area_name,
        MIN(s.created_at) as created_at,
        MAX(s.updated_at) as updated_at,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart), 0) as total_departures,
        COALESCE(SUM(sd.acoustic_arrive + sd.electric_arrive), 0) as total_arrivals,
        COALESCE(SUM(sd.electric_depart + sd.electric_arrive), 0) as electric_total,
        COALESCE(SUM(sd.acoustic_depart + sd.acoustic_arrive), 0) as acoustic_total
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      GROUP BY s.name
      ORDER BY total_departures DESC
    `);

    return result.rows;
  } catch (error) {
    console.error('Error getting all grouped stations:', error);
    return [];
  }
}

// Get stations by community area (grouped)
export async function getStationsByCommunityAreaGrouped(
  communityArea: number,
  year?: number,
  month?: number
): Promise<StationWithStats[]> {
  try {
    let queryText = `
      SELECT 
        MIN(s.id) as id,
        MIN(s.station_id) as station_id,
        s.name,
        AVG(s.latitude) as latitude,
        AVG(s.longitude) as longitude,
        MIN(s.community_area) as community_area,
        MIN(s.community_area_name) as community_area_name,
        MIN(s.created_at) as created_at,
        MAX(s.updated_at) as updated_at,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart), 0) as total_departures,
        COALESCE(SUM(sd.acoustic_arrive + sd.electric_arrive), 0) as total_arrivals,
        COALESCE(SUM(sd.electric_depart + sd.electric_arrive), 0) as electric_total,
        COALESCE(SUM(sd.acoustic_depart + sd.acoustic_arrive), 0) as acoustic_total
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE MIN(s.community_area) = $1
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
      GROUP BY s.name
      HAVING MIN(s.community_area) = $1
      ORDER BY total_departures DESC
    `;

    const result = await query<StationWithStats>(queryText, params);
    return result.rows;
  } catch (error) {
    console.error('Error getting stations by community area (grouped):', error);
    return [];
  }
}

// Search stations by name (grouped)
export async function searchStationsGrouped(
  searchTerm: string
): Promise<StationWithStats[]> {
  try {
    const result = await query<StationWithStats>(
      `
      SELECT 
        MIN(s.id) as id,
        MIN(s.station_id) as station_id,
        s.name,
        AVG(s.latitude) as latitude,
        AVG(s.longitude) as longitude,
        MIN(s.community_area) as community_area,
        MIN(s.community_area_name) as community_area_name,
        MIN(s.created_at) as created_at,
        MAX(s.updated_at) as updated_at,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart), 0) as total_departures,
        COALESCE(SUM(sd.acoustic_arrive + sd.electric_arrive), 0) as total_arrivals,
        COALESCE(SUM(sd.electric_depart + sd.electric_arrive), 0) as electric_total,
        COALESCE(SUM(sd.acoustic_depart + sd.acoustic_arrive), 0) as acoustic_total
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE s.name ILIKE $1
      GROUP BY s.name
      ORDER BY total_departures DESC
    `,
      [`%${searchTerm}%`]
    );

    return result.rows;
  } catch (error) {
    console.error('Error searching stations (grouped):', error);
    return [];
  }
}

// Get activity data for a specific station (by name) over time
export async function getStationActivityOverTime(
  stationName: string,
  startYear?: number,
  endYear?: number
): Promise<any[]> {
  try {
    let queryText = `
      SELECT 
        sd.year,
        sd.month,
        SUM(sd.acoustic_depart + sd.electric_depart) as total_departures,
        SUM(sd.acoustic_arrive + sd.electric_arrive) as total_arrivals,
        SUM(sd.electric_depart + sd.electric_arrive) as electric_total,
        SUM(sd.acoustic_depart + sd.acoustic_arrive) as acoustic_total
      FROM stations s
      JOIN station_days sd ON s.id = sd.station_id
      WHERE s.name = $1
    `;

    const params: any[] = [stationName];

    if (startYear) {
      queryText += ` AND sd.year >= $${params.length + 1}`;
      params.push(startYear);
    }

    if (endYear) {
      queryText += ` AND sd.year <= $${params.length + 1}`;
      params.push(endYear);
    }

    queryText += `
      GROUP BY sd.year, sd.month
      ORDER BY sd.year, sd.month
    `;

    const result = await query(queryText, params);
    return result.rows;
  } catch (error) {
    console.error('Error getting station activity over time:', error);
    return [];
  }
}

// Get city-wide trip summary
export async function getCityTripSummary(): Promise<{
  total_trips: number;
  total_stations: number;
  unique_community_areas: number;
} | null> {
  try {
    const result = await query(`
      SELECT 
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart + sd.acoustic_arrive + sd.electric_arrive), 0) as total_trips,
        (SELECT COUNT(DISTINCT s.id) FROM stations s WHERE s.community_area IS NOT NULL) as total_stations,
        (SELECT COUNT(DISTINCT s.community_area) FROM stations s WHERE s.community_area IS NOT NULL) as unique_community_areas
      FROM station_days sd
      JOIN stations s ON sd.station_id = s.id
      WHERE s.community_area IS NOT NULL
    `);

    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting city trip summary:', error);
    return null;
  }
}

// Get community areas with total trip counts
export async function getCommunityAreasWithTripCounts(): Promise<Array<{
  community_area: number;
  community_area_name: string;
  station_count: number;
  total_trips: number;
}>> {
  try {
    const result = await query(`
      SELECT 
        s.community_area,
        s.community_area_name,
        COUNT(DISTINCT s.id) as station_count,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart + sd.acoustic_arrive + sd.electric_arrive), 0) as total_trips
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE s.community_area IS NOT NULL
      GROUP BY s.community_area, s.community_area_name
      ORDER BY total_trips DESC
    `);

    return result.rows;
  } catch (error) {
    console.error('Error getting community areas with trip counts:', error);
    return [];
  }
}

// Get stations with total trip counts
export async function getStationsWithTripCounts(): Promise<Array<{
  id: number;
  name: string;
  community_area_name: string | null;
  total_trips: number;
}>> {
  try {
    const result = await query(`
      SELECT 
        MIN(s.id) as id,
        s.name,
        MIN(s.community_area_name) as community_area_name,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart + sd.acoustic_arrive + sd.electric_arrive), 0) as total_trips
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE s.community_area IS NOT NULL
      GROUP BY s.name
      ORDER BY total_trips DESC
    `);

    return result.rows;
  } catch (error) {
    console.error('Error getting stations with trip counts:', error);
    return [];
  }
}

// Get simple list of community areas for filtering
export async function getCommunityAreasForFilter(): Promise<Array<{
  community_area: number;
  community_area_name: string;
}>> {
  try {
    const result = await query(`
      SELECT DISTINCT
        community_area,
        community_area_name
      FROM stations 
      WHERE community_area IS NOT NULL
      AND community_area_name IS NOT NULL
      ORDER BY community_area_name
    `);

    return result.rows;
  } catch (error) {
    console.error('Error getting community areas for filter:', error);
    return [];
  }
}

// Get simple list of stations for filtering
export async function getStationsForFilter(): Promise<Array<{
  id: number;
  name: string;
  community_area: number | null;
  community_area_name: string | null;
}>> {
  try {
    const result = await query(`
      SELECT DISTINCT
        MIN(id) as id,
        name,
        MIN(community_area) as community_area,
        MIN(community_area_name) as community_area_name
      FROM stations 
      WHERE community_area IS NOT NULL
      GROUP BY name
      ORDER BY name
    `);

    return result.rows;
  } catch (error) {
    console.error('Error getting stations for filter:', error);
    return [];
  }
}

// Get trip data filtered by station
export async function getTripDataByStation(stationName: string): Promise<{
  total_trips: number;
  acoustic_trips: number;
  electric_trips: number;
  total_departures: number;
  total_arrivals: number;
  station_info: {
    name: string;
    community_area_name: string | null;
    community_area: number | null;
  } | null;
} | null> {
  try {
    const result = await query(`
      SELECT 
        s.name,
        MIN(s.community_area_name) as community_area_name,
        MIN(s.community_area) as community_area,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart + sd.acoustic_arrive + sd.electric_arrive), 0) as total_trips,
        COALESCE(SUM(sd.acoustic_depart + sd.acoustic_arrive), 0) as acoustic_trips,
        COALESCE(SUM(sd.electric_depart + sd.electric_arrive), 0) as electric_trips,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart), 0) as total_departures,
        COALESCE(SUM(sd.acoustic_arrive + sd.electric_arrive), 0) as total_arrivals
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE s.name = $1
      GROUP BY s.name
    `, [stationName]);

    if (result.rows.length === 0) return null;

    const row = result.rows[0];
    return {
      total_trips: parseInt(row.total_trips),
      acoustic_trips: parseInt(row.acoustic_trips),
      electric_trips: parseInt(row.electric_trips),
      total_departures: parseInt(row.total_departures),
      total_arrivals: parseInt(row.total_arrivals),
      station_info: {
        name: row.name,
        community_area_name: row.community_area_name,
        community_area: row.community_area,
      }
    };
  } catch (error) {
    console.error('Error getting trip data by station:', error);
    return null;
  }
}

// Get trip data filtered by community area
export async function getTripDataByCommunityArea(communityArea: number): Promise<{
  total_trips: number;
  acoustic_trips: number;
  electric_trips: number;
  total_departures: number;
  total_arrivals: number;
  station_count: number;
  community_area_info: {
    community_area: number;
    community_area_name: string;
  } | null;
} | null> {
  try {
    const result = await query(`
      SELECT 
        MIN(s.community_area) as community_area,
        MIN(s.community_area_name) as community_area_name,
        COUNT(DISTINCT s.id) as station_count,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart + sd.acoustic_arrive + sd.electric_arrive), 0) as total_trips,
        COALESCE(SUM(sd.acoustic_depart + sd.acoustic_arrive), 0) as acoustic_trips,
        COALESCE(SUM(sd.electric_depart + sd.electric_arrive), 0) as electric_trips,
        COALESCE(SUM(sd.acoustic_depart + sd.electric_depart), 0) as total_departures,
        COALESCE(SUM(sd.acoustic_arrive + sd.electric_arrive), 0) as total_arrivals
      FROM stations s
      LEFT JOIN station_days sd ON s.id = sd.station_id
      WHERE s.community_area = $1
      GROUP BY s.community_area
    `, [communityArea]);

    if (result.rows.length === 0) return null;

    const row = result.rows[0];
    return {
      total_trips: parseInt(row.total_trips),
      acoustic_trips: parseInt(row.acoustic_trips),
      electric_trips: parseInt(row.electric_trips),
      total_departures: parseInt(row.total_departures),
      total_arrivals: parseInt(row.total_arrivals),
      station_count: parseInt(row.station_count),
      community_area_info: {
        community_area: row.community_area,
        community_area_name: row.community_area_name,
      }
    };
  } catch (error) {
    console.error('Error getting trip data by community area:', error);
    return null;
  }
}

export default pool;
