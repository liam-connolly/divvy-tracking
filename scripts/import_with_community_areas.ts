interface CommunityArea {
  area_num_1: string;
  community: string;
  geometry: {
    type: "Polygon" | "MultiPolygon";
    coordinates: number[][][] | number[][][][];
  };
}
import { Pool } from "pg";
import * as fs from "fs";
import * as path from "path";
import { parse } from "papaparse";
import dotenv from "dotenv";

dotenv.config({ path: path.resolve(process.cwd(), ".env") });

interface GeoJSONFeature {
  properties: {
    area_num_1: string;
    community: string;
  };
  geometry: {
    type: "Polygon" | "MultiPolygon";
    coordinates: number[][][] | number[][][][];
  };
}

interface GeoJSONResponse {
  features: GeoJSONFeature[];
}

let communityAreas: CommunityArea[] = [];

// Simple point-in-polygon check
function isPointInPolygon(
  lat: number,
  lng: number,
  polygon: number[][]
): boolean {
  let inside = false;
  const x = lng,
    y = lat;

  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i][0],
      yi = polygon[i][1];
    const xj = polygon[j][0],
      yj = polygon[j][1];

    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

// Find which community area a lat/lng belongs to
function findCommunityArea(
  lat: number,
  lng: number
): [number | null, string | null] {
  if (!lat || !lng) return [null, null];

  for (const area of communityAreas) {
    if (area.geometry.type === "Polygon") {
      const coords = area.geometry.coordinates as number[][][];
      if (isPointInPolygon(lat, lng, coords[0])) {
        return [parseInt(area.area_num_1), area.community];
      }
    } else if (area.geometry.type === "MultiPolygon") {
      const coords = area.geometry.coordinates as number[][][][];
      for (const polygon of coords) {
        if (isPointInPolygon(lat, lng, polygon[0])) {
          return [parseInt(area.area_num_1), area.community];
        }
      }
    }
  }
  return [null, null];
}

// Load community areas from Chicago API
async function loadCommunityAreas(): Promise<void> {
  const cacheFile = path.join(process.cwd(), "community_areas.json");

  try {
    if (fs.existsSync(cacheFile)) {
      console.log("📁 Loading community areas from cache...");
      communityAreas = JSON.parse(fs.readFileSync(cacheFile, "utf8"));
    } else {
      console.log("🌐 Downloading community areas from Chicago Data Portal...");
      const fetch = (await import("node-fetch")).default;
      const response = await fetch(
        "https://data.cityofchicago.org/resource/igwz-8jzy.geojson"
      );
      const data = (await response.json()) as GeoJSONResponse;

      communityAreas = data.features.map((f: GeoJSONFeature) => ({
        area_num_1: f.properties.area_num_1,
        community: f.properties.community,
        geometry: f.geometry,
      }));

      fs.writeFileSync(cacheFile, JSON.stringify(communityAreas, null, 2));
      console.log("💾 Cached community areas");
    }
    console.log(`✅ Loaded ${communityAreas.length} community areas`);
  } catch (error) {
    console.error("❌ Failed to load community areas:", error);
    throw error;
  }
}

// Process a single station
async function processStation(
  pool: Pool,
  stationId: string,
  name: string,
  lat?: number,
  lng?: number
): Promise<void> {
  const [communityArea, communityAreaName] = findCommunityArea(
    lat || 0,
    lng || 0
  );

  await pool.query(
    `
    INSERT INTO stations (station_id, name, latitude, longitude, community_area, community_area_name)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (station_id) DO UPDATE SET
      name = EXCLUDED.name,
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude,
      community_area = EXCLUDED.community_area,
      community_area_name = EXCLUDED.community_area_name,
      updated_at = NOW()
  `,
    [stationId, name, lat, lng, communityArea, communityAreaName]
  );
}

// Import trips from one CSV file
async function importCSV(
  pool: Pool,
  filePath: string
): Promise<{ stations: number; trips: number }> {
  const fileName = path.basename(filePath);
  console.log(`📄 Processing ${fileName}...`);

  const csvContent = fs.readFileSync(filePath, "utf8");
  const parseResult = parse(csvContent, { header: true, skipEmptyLines: true });

  if (parseResult.errors.length > 0) {
    console.log(
      `⚠️  ${parseResult.errors.length} parsing errors in ${fileName}`
    );
    console.log("First few errors:", parseResult.errors.slice(0, 3));
  }

  const data = parseResult.data as any[];
  console.log(`  📊 ${data.length} rows found`);

  // Debug: Check first row structure
  if (data.length > 0) {
    console.log("  🔍 First row columns:", Object.keys(data[0]));
    console.log("  🔍 Sample data:", {
      ride_id: data[0].ride_id || data[0].trip_id,
      start_station: data[0].start_station_id || data[0].from_station_id,
      start_name: data[0].start_station_name || data[0].from_station_name,
    });
  }

  // Track processed stations to avoid duplicates
  const processedStations = new Set<string>();
  let stationCount = 0;
  let tripCount = 0;

  // Process all stations first
  console.log("  🏗️  Processing stations...");
  for (const row of data) {
    // Start station
    const startId = row.start_station_id || row.from_station_id;
    const startName = row.start_station_name || row.from_station_name;

    if (startId && startName && !processedStations.has(startId)) {
      try {
        await processStation(
          pool,
          startId,
          startName,
          parseFloat(row.start_lat),
          parseFloat(row.start_lng)
        );
        processedStations.add(startId);
        stationCount++;
      } catch (error) {
        console.log(
          `    ❌ Error processing start station ${startId}: ${error}`
        );
      }
    }

    // End station
    const endId = row.end_station_id || row.to_station_id;
    const endName = row.end_station_name || row.to_station_name;

    if (endId && endName && !processedStations.has(endId)) {
      try {
        await processStation(
          pool,
          endId,
          endName,
          parseFloat(row.end_lat),
          parseFloat(row.end_lng)
        );
        processedStations.add(endId);
        stationCount++;
      } catch (error) {
        console.log(`    ❌ Error processing end station ${endId}: ${error}`);
      }
    }
  }

  console.log(`  ✅ Processed ${stationCount} stations`);

  // Process trips in batches
  console.log("  🚲 Processing trips...");
  const batchSize = 1000;

  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    let batchTripCount = 0;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      for (const row of batch) {
        const rideId = row.ride_id || row.trip_id;
        if (!rideId) continue;

        try {
          const result = await client.query(
            `
            INSERT INTO trips_raw (
              ride_id, rideable_type, started_at, ended_at,
              start_station_name, start_station_id, end_station_name, end_station_id,
              start_lat, start_lng, end_lat, end_lng, member_casual
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (ride_id) DO NOTHING
          `,
            [
              rideId,
              row.rideable_type || "classic_bike",
              row.started_at || row.starttime,
              row.ended_at || row.stoptime,
              row.start_station_name || row.from_station_name,
              row.start_station_id || row.from_station_id,
              row.end_station_name || row.to_station_name,
              row.end_station_id || row.to_station_id,
              parseFloat(row.start_lat) || null,
              parseFloat(row.start_lng) || null,
              parseFloat(row.end_lat) || null,
              parseFloat(row.end_lng) || null,
              row.member_casual || row.usertype || "casual",
            ]
          );

          batchTripCount += result.rowCount || 0;
        } catch (error) {
          console.log(`    ⚠️  Error inserting trip ${rideId}: ${error}`);
        }
      }

      await client.query("COMMIT");
      tripCount += batchTripCount;
    } catch (error) {
      await client.query("ROLLBACK");
      console.log(`    ❌ Batch error: ${error}`);
    } finally {
      client.release();
    }

    if ((Math.floor(i / batchSize) + 1) % 10 === 0) {
      console.log(
        `    Batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
          data.length / batchSize
        )} - ${batchTripCount} trips inserted`
      );
    }
  }

  console.log(`  ✅ ${stationCount} new stations, ${tripCount} trips inserted`);
  return { stations: stationCount, trips: tripCount };
}

// Aggregate daily data
async function aggregateDaily(pool: Pool): Promise<void> {
  console.log("\n📊 Aggregating daily data...");

  // Departures
  await pool.query(`
    INSERT INTO station_days (station_id, day, month, year, acoustic_depart, electric_depart)
    SELECT 
      s.id,
      EXTRACT(day FROM t.started_at)::int,
      EXTRACT(month FROM t.started_at)::int,
      EXTRACT(year FROM t.started_at)::int,
      COUNT(CASE WHEN t.rideable_type IN ('classic_bike', 'docked_bike') THEN 1 END)::int,
      COUNT(CASE WHEN t.rideable_type = 'electric_bike' THEN 1 END)::int
    FROM trips_raw t
    JOIN stations s ON t.start_station_id = s.station_id
    WHERE t.started_at IS NOT NULL
    GROUP BY s.id, EXTRACT(day FROM t.started_at), EXTRACT(month FROM t.started_at), EXTRACT(year FROM t.started_at)
    ON CONFLICT (day, month, year, station_id) DO UPDATE SET
      acoustic_depart = station_days.acoustic_depart + EXCLUDED.acoustic_depart,
      electric_depart = station_days.electric_depart + EXCLUDED.electric_depart
  `);

  // Arrivals
  await pool.query(`
    INSERT INTO station_days (station_id, day, month, year, acoustic_arrive, electric_arrive)
    SELECT 
      s.id,
      EXTRACT(day FROM t.ended_at)::int,
      EXTRACT(month FROM t.ended_at)::int,
      EXTRACT(year FROM t.ended_at)::int,
      COUNT(CASE WHEN t.rideable_type IN ('classic_bike', 'docked_bike') THEN 1 END)::int,
      COUNT(CASE WHEN t.rideable_type = 'electric_bike' THEN 1 END)::int
    FROM trips_raw t
    JOIN stations s ON t.end_station_id = s.station_id
    WHERE t.ended_at IS NOT NULL
    GROUP BY s.id, EXTRACT(day FROM t.ended_at), EXTRACT(month FROM t.ended_at), EXTRACT(year FROM t.ended_at)
    ON CONFLICT (day, month, year, station_id) DO UPDATE SET
      acoustic_arrive = COALESCE(station_days.acoustic_arrive, 0) + EXCLUDED.acoustic_arrive,
      electric_arrive = COALESCE(station_days.electric_arrive, 0) + EXCLUDED.electric_arrive
  `);

  console.log("✅ Daily aggregation complete");
}

// Main import function
async function main(): Promise<void> {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL not found in environment");
  }

  // Load community areas
  await loadCommunityAreas();

  // Setup database
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  // Find CSV files
  const dataDir = path.join(process.cwd(), "divvy_data");
  const csvFiles = fs
    .readdirSync(dataDir)
    .filter((f) => f.endsWith(".csv"))
    .sort();

  console.log(`🚀 Starting import of ${csvFiles.length} files\n`);

  let totalStations = 0;
  let totalTrips = 0;

  // Process each CSV
  for (let i = 0; i < csvFiles.length; i++) {
    const filePath = path.join(dataDir, csvFiles[i]);
    const result = await importCSV(pool, filePath);
    totalStations += result.stations;
    totalTrips += result.trips;

    console.log(`  Progress: ${i + 1}/${csvFiles.length}\n`);
  }

  // Aggregate daily data
  await aggregateDaily(pool);

  // Summary
  console.log(`🎉 Import complete!`);
  console.log(
    `📊 Total: ${totalStations} stations, ${totalTrips.toLocaleString()} trips`
  );

  await pool.end();
}

if (require.main === module) {
  main().catch((error) => {
    console.error("❌ Import failed:", error);
    process.exit(1);
  });
}
