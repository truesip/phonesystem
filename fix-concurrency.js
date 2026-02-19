require('dotenv').config();
const mysql = require('mysql2/promise');

async function run() {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  try {
    console.log('\n=== REDUCING DIALER CONCURRENCY ===\n');
    
    // Set max concurrency to 3 to avoid hitting Phone.System limits
    const [result] = await pool.query(`
      UPDATE dialer_campaigns 
      SET concurrency_limit = 3 
      WHERE concurrency_limit > 3
    `);
    
    console.log(`✓ Updated ${result.affectedRows} campaigns to concurrency limit of 3`);
    
    // Show current campaign settings
    const [campaigns] = await pool.query(`
      SELECT name, status, concurrency_limit 
      FROM dialer_campaigns 
      WHERE status != 'deleted'
      ORDER BY id DESC
    `);
    
    console.log('\nCurrent campaign settings:');
    console.table(campaigns);
    
    console.log('\n✓ Done! Your campaigns will now use max 3 concurrent calls.');
    console.log('This should prevent "Service maximum instances reached" errors.\n');
    
  } catch (err) {
    console.error('✗ Error:', err.message);
  } finally {
    await pool.end();
  }
}

run();
