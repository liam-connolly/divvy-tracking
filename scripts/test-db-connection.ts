import dotenv from "dotenv";
import path from "path";
import { testConnection, getStats } from "../lib/db";

// Load environment variables with absolute path
const envPath = path.resolve(process.cwd(), ".env");
const result = dotenv.config({ path: envPath });

async function main() {
  console.log("Testing database connection...");
  console.log("Environment file path:", envPath);
  console.log("Environment loaded:", !result.error);

  if (result.error) {
    console.error("Failed to load .env.local:", result.error);
  }

  // Debug environment variables
  console.log("DATABASE_URL exists:", !!process.env.DATABASE_URL);
  if (process.env.DATABASE_URL) {
    const dbUrl = process.env.DATABASE_URL;
    const maskedUrl = dbUrl.replace(/:([^:@]+)@/, ":***@"); // Hide password
    console.log("DATABASE_URL:", maskedUrl);
  } else {
    console.log("❌ DATABASE_URL not found in environment");
    return;
  }

  const connectionSuccessful = await testConnection();

  if (connectionSuccessful) {
    console.log("\nFetching database statistics...");
    const stats = await getStats();

    if (stats) {
      console.log("📊 Database Stats:", {
        "Total Raw Trips": stats.total_raw_trips?.toLocaleString() || 0,
        "Total Stations": stats.total_stations?.toLocaleString() || 0,
        "Total Station Days": stats.total_station_days?.toLocaleString() || 0,
        "Community Areas": stats.unique_community_areas || 0,
        "Date Range": `${stats.earliest_day || "N/A"} - ${
          stats.latest_day || "N/A"
        }`,
      });
    }
  }

  process.exit(connectionSuccessful ? 0 : 1);
}

main().catch(console.error);
