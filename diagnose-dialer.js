require('dotenv').config();
const mysql = require('mysql2/promise');

async function run() {
  let pool;
  try {
    // Use DATABASE_URL if available
    const dbUrl = process.env.DATABASE_URL;
    if (!dbUrl) {
      throw new Error('DATABASE_URL not found in environment');
    }
    
    pool = mysql.createPool(dbUrl);

    console.log('\n=== DIALER FAILURE DIAGNOSTIC ===\n');

    // Test connection
    await pool.query('SELECT 1');
    console.log('✓ Database connected\n');

    // Query 1
    console.log('1. LEAD STATUS COUNTS:');
    const [leads] = await pool.query(`
      SELECT status, COUNT(*) as count 
      FROM dialer_leads 
      WHERE campaign_id IN (SELECT id FROM dialer_campaigns WHERE name IN ('test', 'Trump', 'John')) 
      GROUP BY status
    `);
    console.table(leads);

    // Query 2
    console.log('\n2. CALL LOG STATUS/RESULT:');
    const [calls] = await pool.query(`
      SELECT status, result, COUNT(*) as count 
      FROM dialer_call_logs 
      WHERE campaign_id IN (SELECT id FROM dialer_campaigns WHERE name IN ('test', 'Trump', 'John')) 
      GROUP BY status, result
    `);
    console.table(calls);

    // Query 3
    console.log('\n3. ERROR NOTES:');
    const [notes] = await pool.query(`
      SELECT SUBSTRING(notes, 1, 100) as error_note, COUNT(*) as count 
      FROM dialer_call_logs 
      WHERE campaign_id IN (SELECT id FROM dialer_campaigns WHERE name IN ('test', 'Trump', 'John')) 
      AND notes IS NOT NULL 
      GROUP BY error_note 
      LIMIT 10
    `);
    if (notes.length > 0) {
      console.table(notes);
    } else {
      console.log('  No error notes found');
    }

  } catch (err) {
    console.error('\n✗ ERROR:', err.message);
    console.error('Stack:', err.stack);
  } finally {
    if (pool) await pool.end();
  }
}

run();
