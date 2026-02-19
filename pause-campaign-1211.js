require('dotenv').config();
const mysql = require('mysql2/promise');

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  await pool.query("UPDATE dialer_campaigns SET status = 'paused' WHERE name = '1211'");
  console.log('✓ Paused campaign "1211"');
  const [r] = await pool.query("SELECT name, status, concurrency_limit FROM dialer_campaigns WHERE status='running'");
  console.log('\nRunning campaigns:');
  console.table(r);
  const total = r.reduce((sum, c) => sum + c.concurrency_limit, 0);
  console.log(`\n✓ Total concurrent capacity: ${total} (Pipecat limit: ~10)`);
  await pool.end();
})();
