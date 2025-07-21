import { PrismaClient } from '../app/generated/prisma';
import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse/sync';

const prisma = new PrismaClient();

interface DivvyTrip {
  ride_id: string;
  rideable_type: string;
  started_at: string;
  ended_at: string;
  start_station_name: string;
  start_station_id: string;
  end_station_name: string;
  end_station_id: string;
  start_lat: string;
  start_lng: string;
  end_lat: string;
  end_lng: string;
  member_casual: string;
}

interface StationData {
  name: string;
  latitude: string;
  longitude: string;
}

interface StationDayData {
  stationId: number;
  day: number;
  month: number;
  year: number;
  acousticArrive: number;
  acousticDepart: number;
  electricArrive: number;
  electricDepart: number;
}

class DivvySeeder {
  private stationCache = new Map<string, number>();
  private stationDayStats = new Map<string, StationDayData>();

  async seedFromCSV(csvPath: string) {
    console.log(`🌱 Starting seeding process from ${csvPath}`);

    if (!fs.existsSync(csvPath)) {
      throw new Error(`CSV file not found: ${csvPath}`);
    }

    // Read and parse CSV
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const trips: DivvyTrip[] = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });

    console.log(`📊 Found ${trips.length} trips to process`);

    // Process trips in batches
    const batchSize = 1000;
    for (let i = 0; i < trips.length; i += batchSize) {
      const batch = trips.slice(i, i + batchSize);
      await this.processBatch(batch, i, trips.length);
    }

    // Save aggregated station day data
    await this.saveStationDayData();

    console.log('✅ Seeding completed successfully!');
  }

  private async processBatch(
    trips: DivvyTrip[],
    currentIndex: number,
    total: number
  ) {
    console.log(
      `📦 Processing batch ${Math.floor(currentIndex / 1000) + 1} (${
        currentIndex + 1
      }-${Math.min(currentIndex + trips.length, total)} of ${total})`
    );

    for (const trip of trips) {
      await this.processTrip(trip);
    }
  }

  private async processTrip(trip: DivvyTrip) {
    // Skip trips without station data
    if (!trip.start_station_name || !trip.end_station_name) {
      return;
    }

    // Extract date info
    const startDate = new Date(trip.started_at);
    const endDate = new Date(trip.ended_at);

    // Determine bike type
    const isElectric = trip.rideable_type === 'electric_bike';
    const isAcoustic =
      trip.rideable_type === 'classic_bike' ||
      trip.rideable_type === 'docked_bike';

    // Process start station
    const startStationId = await this.getOrCreateStation({
      name: trip.start_station_name,
      latitude: trip.start_lat,
      longitude: trip.start_lng,
    });

    // Process end station
    const endStationId = await this.getOrCreateStation({
      name: trip.end_station_name,
      latitude: trip.end_lat,
      longitude: trip.end_lng,
    });

    // Update departure stats for start station
    this.updateStationDayStats(
      startStationId,
      startDate,
      isElectric,
      isAcoustic,
      'depart'
    );

    // Update arrival stats for end station
    this.updateStationDayStats(
      endStationId,
      endDate,
      isElectric,
      isAcoustic,
      'arrive'
    );
  }

  private async getOrCreateStation(stationData: StationData): Promise<number> {
    // Check cache first
    if (this.stationCache.has(stationData.name)) {
      return this.stationCache.get(stationData.name)!;
    }

    // Check if station exists in database
    let station = await prisma.station.findUnique({
      where: { name: stationData.name },
    });

    if (!station) {
      // Create new station
      station = await prisma.station.create({
        data: {
          name: stationData.name,
          latitude: stationData.latitude,
          longitude: stationData.longitude,
        },
      });
      console.log(`🆕 Created station: ${stationData.name}`);
    }

    // Cache the station ID
    this.stationCache.set(stationData.name, station.id);
    return station.id;
  }

  private updateStationDayStats(
    stationId: number,
    date: Date,
    isElectric: boolean,
    isAcoustic: boolean,
    type: 'arrive' | 'depart'
  ) {
    const day = date.getDate();
    const month = date.getMonth() + 1;
    const year = date.getFullYear();

    const key = `${stationId}-${day}-${month}-${year}`;

    if (!this.stationDayStats.has(key)) {
      this.stationDayStats.set(key, {
        stationId,
        day,
        month,
        year,
        acousticArrive: 0,
        acousticDepart: 0,
        electricArrive: 0,
        electricDepart: 0,
      });
    }

    const stats = this.stationDayStats.get(key)!;

    if (type === 'arrive') {
      if (isElectric) stats.electricArrive++;
      if (isAcoustic) stats.acousticArrive++;
    } else {
      if (isElectric) stats.electricDepart++;
      if (isAcoustic) stats.acousticDepart++;
    }
  }

  private async saveStationDayData() {
    console.log(
      `💾 Saving ${this.stationDayStats.size} station day records...`
    );

    const stationDayData = Array.from(this.stationDayStats.values());

    // Process in batches to avoid memory issues
    const batchSize = 100;
    for (let i = 0; i < stationDayData.length; i += batchSize) {
      const batch = stationDayData.slice(i, i + batchSize);

      await prisma.stationDay.createMany({
        data: batch,
        skipDuplicates: true,
      });

      console.log(
        `💾 Saved batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
          stationDayData.length / batchSize
        )}`
      );
    }
  }

  async cleanup() {
    await prisma.$disconnect();
  }
}

async function main() {
  const csvPath = process.argv[2] || 'data/csv/202501-divvy-tripdata.csv';

  console.log('🚀 Starting Divvy data seeding...');
  console.log(`📁 CSV file: ${csvPath}`);

  const seeder = new DivvySeeder();

  try {
    await seeder.seedFromCSV(csvPath);
  } catch (error) {
    console.error('❌ Seeding failed:', error);
    process.exit(1);
  } finally {
    await seeder.cleanup();
  }
}

main().catch((error) => {
  console.error('❌ Unexpected error:', error);
  process.exit(1);
});
