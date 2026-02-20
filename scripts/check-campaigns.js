const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  const [campaigns] = await pool.query(
    'SELECT id, name, status, concurrency_limit FROM dialer_campaigns WHERE status = ? ORDER BY id',
    ['running']
  );
  
  console.table(campaigns);
  
  await pool.end();
})().catch(console.error);
