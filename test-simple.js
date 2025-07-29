const { Pool } = require('pg');

const pool = new Pool({
  host: 'divvy-db.ckz624cow6v0.us-east-1.rds.amazonaws.com',
  port: 5432,
  database: 'divvy_db',
  user: 'postgres',
  password: 'your-actual-password', // Replace with your real password
  ssl: { rejectUnauthorized: false }
});

async function test() {
  try {
    const client = await pool.connect();
    console.log('✅ Connection successful!');
    const result = await client.query('SELECT NOW()');
    console.log('Time:', result.rows[0].now);
    client.release();
  } catch (err) {
    console.error('❌ Connection failed:', err.message);
  }
  await pool.end();
}

test();