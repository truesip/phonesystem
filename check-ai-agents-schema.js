require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function checkSchema() {
  const dsn = process.env.DATABASE_URL;
  const u = new URL(dsn);
  const dbName = u.pathname.replace(/^\//, '');
  
  const sslMode = (u.searchParams.get('ssl-mode') || u.searchParams.get('sslmode') || '').toUpperCase();
  const caPath = process.env.DATABASE_CA_CERT;
  let sslOptions;
  if (caPath && fs.existsSync(caPath)) {
    sslOptions = { ca: fs.readFileSync(caPath, 'utf8'), rejectUnauthorized: true };
  } else if (sslMode === 'REQUIRED') {
    sslOptions = { rejectUnauthorized: false };
  }
  
  const connection = await mysql.createConnection({
    host: u.hostname,
    port: Number(u.port) || 3306,
    user: decodeURIComponent(u.username),
    password: decodeURIComponent(u.password),
    database: dbName,
    ssl: sslOptions
  });

  try {
    const [columns] = await connection.execute(`
      SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_agents'
      ORDER BY ORDINAL_POSITION
    `, [dbName]);

    console.log('\nai_agents table columns:\n');
    columns.forEach(col => {
      console.log(`  ${col.COLUMN_NAME.padEnd(25)} ${col.COLUMN_TYPE.padEnd(20)} ${col.IS_NULLABLE === 'YES' ? 'NULL' : 'NOT NULL'}`);
    });
  } finally {
    await connection.end();
  }
}

checkSchema().catch(console.error);
