require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function runMigration() {
  const dsn = process.env.DATABASE_URL;
  if (!dsn) {
    console.error('❌ DATABASE_URL environment variable not set');
    process.exit(1);
  }
  
  const u = new URL(dsn);
  const dbName = u.pathname.replace(/^\//, '');
  
  // TLS options (same as server.js)
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
    console.log('Connected to database...\n');

    // Step 1: Find all tables with foreign keys pointing to 'users'
    console.log('Step 1: Finding foreign keys pointing to legacy "users" table...');
    const [fkResults] = await connection.execute(`
      SELECT 
        TABLE_NAME,
        CONSTRAINT_NAME,
        COLUMN_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = DATABASE()
        AND REFERENCED_TABLE_NAME = 'users'
        AND REFERENCED_COLUMN_NAME = 'id'
    `);

    if (fkResults.length === 0) {
      console.log('✓ No foreign keys found pointing to legacy "users" table.\n');
    } else {
      console.log(`Found ${fkResults.length} foreign key(s) to fix:`);
      fkResults.forEach(fk => {
        console.log(`  - ${fk.TABLE_NAME}.${fk.COLUMN_NAME} (${fk.CONSTRAINT_NAME})`);
      });
      console.log('');
    }

    // Step 2: Fix ai_agents table
    console.log('Step 2: Fixing ai_agents foreign key...');
    try {
      await connection.execute('ALTER TABLE ai_agents DROP FOREIGN KEY ai_agents_ibfk_1');
      console.log('✓ Dropped old constraint: ai_agents_ibfk_1');
    } catch (error) {
      if (error.code === 'ER_CANT_DROP_FIELD_OR_KEY') {
        console.log('⚠ Constraint ai_agents_ibfk_1 does not exist (may already be fixed)');
      } else {
        throw error;
      }
    }

    await connection.execute(`
      ALTER TABLE ai_agents 
      ADD CONSTRAINT fk_ai_agents_user 
      FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
    `);
    console.log('✓ Added new constraint: fk_ai_agents_user -> signup_users(id)\n');

    // Step 3: Verify all tables now reference signup_users
    console.log('Step 3: Verifying all foreign keys...');
    const [verifyResults] = await connection.execute(`
      SELECT 
        TABLE_NAME,
        CONSTRAINT_NAME,
        COLUMN_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = DATABASE()
        AND COLUMN_NAME = 'user_id'
        AND REFERENCED_TABLE_NAME IS NOT NULL
      ORDER BY TABLE_NAME
    `);

    console.log(`Found ${verifyResults.length} user_id foreign key(s):`);
    verifyResults.forEach(fk => {
      const status = fk.REFERENCED_TABLE_NAME === 'signup_users' ? '✓' : '⚠';
      console.log(`  ${status} ${fk.TABLE_NAME}.${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME}(${fk.REFERENCED_COLUMN_NAME})`);
    });

    console.log('\n✅ Migration completed successfully!');

  } catch (error) {
    console.error('❌ Migration failed:', error.message);
    throw error;
  } finally {
    await connection.end();
  }
}

runMigration().catch(console.error);
