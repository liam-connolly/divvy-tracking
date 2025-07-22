const { Pool } = require("pg");

const pool = new Pool({
  connectionString: "postgresql://divvy_user:password@localhost:5432/divvy_db",
});

async function test() {
  try {
    const client = await pool.connect();
    console.log("✅ Manual connection successful!");
    client.release();
  } catch (err) {
    console.error("❌ Manual connection failed:", err.message);
  }
  process.exit();
}

test();
