require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function checkColumnTypes() {
  const dsn = process.env.DATABASE_URL;
  if (!dsn) {
    console.error('❌ DATABASE_URL environment variable not set');
    process.exit(1);
  }
  
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
    console.log('Checking column types...\n');

    // Check signup_users.id type
    const [signupUsersId] = await connection.execute(`
      SELECT COLUMN_TYPE, DATA_TYPE, COLUMN_KEY
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'signup_users' AND COLUMN_NAME = 'id'
    `, [dbName]);
    
    console.log('signup_users.id:', signupUsersId[0]);
    console.log('');

    // Check all tables with user_id that reference legacy users table
    const [tables] = await connection.execute(`
      SELECT DISTINCT TABLE_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ?
        AND REFERENCED_TABLE_NAME = 'users'
        AND REFERENCED_COLUMN_NAME = 'id'
    `, [dbName]);

    console.log(`Tables with user_id columns (${tables.length}):\n`);

    for (const table of tables) {
      const [userIdCol] = await connection.execute(`
        SELECT COLUMN_TYPE, DATA_TYPE, IS_NULLABLE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'user_id'
      `, [dbName, table.TABLE_NAME]);
      
      const match = userIdCol[0].COLUMN_TYPE === signupUsersId[0].COLUMN_TYPE ? '✓' : '⚠';
      console.log(`  ${match} ${table.TABLE_NAME}.user_id: ${userIdCol[0].COLUMN_TYPE} (${userIdCol[0].IS_NULLABLE})`);
    }

    console.log('\n---\n');
    console.log('Expected type:', signupUsersId[0].COLUMN_TYPE);

  } catch (error) {
    console.error('❌ Error:', error.message);
    throw error;
  } finally {
    await connection.end();
  }
}

checkColumnTypes().catch(console.error);
