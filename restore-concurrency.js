require('dotenv').config();
const mysql = require('mysql2/promise');

async function run() {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  try {
    console.log('\n=== RESTORING HIGHER CONCURRENCY ===\n');
    
    // Restore to 30 (safer than 50 but much higher than 3)
    const [result] = await pool.query(`
      UPDATE dialer_campaigns 
      SET concurrency_limit = 30 
      WHERE status IN ('running', 'paused', 'draft')
    `);
    
    console.log(`✓ Updated ${result.affectedRows} campaigns to concurrency limit of 30`);
    
    // Show current campaign settings
    const [campaigns] = await pool.query(`
      SELECT name, status, concurrency_limit 
      FROM dialer_campaigns 
      WHERE status != 'deleted'
      ORDER BY id DESC
    `);
    
    console.log('\nCurrent campaign settings:');
    console.table(campaigns);
    
    console.log('\n✓ Done! Campaigns restored to 30 concurrent calls.');
    console.log('Now let\'s investigate the Phone.System server bottleneck...\n');
    
  } catch (err) {
    console.error('✗ Error:', err.message);
  } finally {
    await pool.end();
  }
}

run();
