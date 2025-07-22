require("dotenv").config({ path: ".env.local" });
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function testConnection() {
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
  } catch (err) {
    console.error("❌ Database connection error:", err.message);
  } finally {
    await pool.end();
  }
}

testConnection();
