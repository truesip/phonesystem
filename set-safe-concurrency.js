require('dotenv').config();
const mysql = require('mysql2/promise');

async function run() {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  try {
    console.log('\n=== SETTING SAFE CONCURRENCY FOR PIPECAT LIMIT ===\n');
    console.log('Pipecat account limit: ~10 concurrent instances');
    console.log('Safe per-campaign limit: 5 (allows 2 campaigns to run simultaneously)\n');
    
    // Set to 5 per campaign (safe for 10-instance limit)
    const [result] = await pool.query(`
      UPDATE dialer_campaigns 
      SET concurrency_limit = 5 
      WHERE status IN ('running', 'paused', 'draft')
    `);
    
    console.log(`✓ Updated ${result.affectedRows} campaigns to concurrency limit of 5`);
    
    // Show current campaign settings
    const [campaigns] = await pool.query(`
      SELECT name, status, concurrency_limit 
      FROM dialer_campaigns 
      WHERE status != 'deleted'
      ORDER BY id DESC
    `);
    
    console.log('\nCurrent campaign settings:');
    console.table(campaigns);
    
    const [running] = await pool.query(`
      SELECT COUNT(*) as cnt, SUM(concurrency_limit) as total_limit
      FROM dialer_campaigns 
      WHERE status = 'running'
    `);
    
    const runningCount = running[0]?.cnt || 0;
    const totalLimit = running[0]?.total_limit || 0;
    
    console.log(`\n✓ Running campaigns: ${runningCount}`);
    console.log(`✓ Total concurrent capacity: ${totalLimit} calls`);
    console.log(`✓ Pipecat limit: ~10 instances`);
    
    if (totalLimit > 10) {
      console.log('\n⚠️  WARNING: Total is still above Pipecat limit!');
      console.log('   You may need to pause some campaigns or contact Pipecat for higher quota.');
    } else {
      console.log('\n✅ Configuration is safe for current Pipecat limits!');
    }
    
  } catch (err) {
    console.error('✗ Error:', err.message);
  } finally {
    await pool.end();
  }
}

run();
