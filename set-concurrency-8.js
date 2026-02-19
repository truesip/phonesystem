const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.log('Setting all campaign concurrency limits to 8...');
  
  const [result] = await pool.execute(
    'UPDATE dialer_campaigns SET concurrency_limit = 8 WHERE concurrency_limit > 8'
  );
  
  console.log(`Updated ${result.affectedRows} campaigns`);
  
  const [campaigns] = await pool.query(
    'SELECT id, name, status, concurrency_limit FROM dialer_campaigns ORDER BY id'
  );
  
  console.table(campaigns);
  
  await pool.end();
})().catch(console.error);
