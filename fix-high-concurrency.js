const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.log('\n=== Fixing High Concurrency Limits ===\n');
  
  // Check campaigns with high concurrency
  const [high] = await pool.query(
    `SELECT id, name, status, concurrency_limit, user_id
     FROM dialer_campaigns
     WHERE concurrency_limit > 8
     ORDER BY concurrency_limit DESC`
  );
  
  console.log(`Found ${high.length} campaigns with concurrency > 8:\n`);
  console.table(high);
  
  if (high.length > 0) {
    console.log('\n⚠️  Setting all to max safe limit of 8...\n');
    
    const [result] = await pool.execute(
      `UPDATE dialer_campaigns 
       SET concurrency_limit = 8 
       WHERE concurrency_limit > 8`
    );
    
    console.log(`✓ Updated ${result.affectedRows} campaigns\n`);
    
    // Show updated list
    const [updated] = await pool.query(
      `SELECT id, name, status, concurrency_limit
       FROM dialer_campaigns
       WHERE id IN (${high.map(c => c.id).join(',')})
       ORDER BY id`
    );
    
    console.log('Updated campaigns:');
    console.table(updated);
  }
  
  await pool.end();
  
  console.log('\n✅ All campaigns now have safe concurrency limits!');
  console.log('\n📌 Recommended: Keep concurrency ≤ 8 to avoid rate limits');
})().catch(console.error);
