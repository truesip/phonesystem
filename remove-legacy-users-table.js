require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function removeLegacyTable() {
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
    console.log('🗑️  Removing Legacy Users Table\n');
    console.log('='.repeat(60) + '\n');

    // Safety check: Verify no foreign keys reference users table
    console.log('Safety check: Verifying no foreign keys point to legacy table...');
    const [legacyFks] = await connection.execute(`
      SELECT TABLE_NAME, CONSTRAINT_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ?
        AND REFERENCED_TABLE_NAME = 'users'
        AND REFERENCED_COLUMN_NAME = 'id'
    `, [dbName]);

    if (legacyFks.length > 0) {
      console.log('❌ ABORT: Foreign keys still reference legacy users table:');
      legacyFks.forEach(fk => {
        console.log(`  - ${fk.TABLE_NAME}.${fk.CONSTRAINT_NAME}`);
      });
      process.exit(1);
    }
    console.log('✅ No foreign keys reference legacy table\n');

    // Check if backup already exists
    const [backupExists] = await connection.execute(`
      SELECT 1 FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users_backup'
    `, [dbName]);

    if (backupExists.length > 0) {
      console.log('⚠️  Backup table "users_backup" already exists');
      console.log('Skipping backup creation...\n');
    } else {
      // Create backup with primary key
      console.log('Creating backup: users_backup...');
      await connection.execute('CREATE TABLE users_backup LIKE users');
      await connection.execute('INSERT INTO users_backup SELECT * FROM users');
      const [backupCount] = await connection.execute('SELECT COUNT(*) as count FROM users_backup');
      console.log(`✅ Backup created with ${backupCount[0].count} users\n`);
    }

    // Drop legacy users table
    console.log('Dropping legacy "users" table...');
    await connection.execute('DROP TABLE IF EXISTS users');
    console.log('✅ Legacy table removed\n');

    // Verify it's gone
    const [tableCheck] = await connection.execute(`
      SELECT TABLE_NAME
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ('users', 'users_backup')
    `, [dbName]);

    console.log('Final status:');
    tableCheck.forEach(table => {
      if (table.TABLE_NAME === 'users') {
        console.log('  ❌ users (should be deleted!)');
      } else if (table.TABLE_NAME === 'users_backup') {
        console.log('  ✅ users_backup (backup preserved)');
      }
    });

    if (!tableCheck.find(t => t.TABLE_NAME === 'users')) {
      console.log('\n🎉 Success! Legacy users table removed.');
      console.log('Backup available in "users_backup" table if needed.\n');
    }

  } catch (error) {
    console.error('\n❌ Error:', error.message);
    throw error;
  } finally {
    await connection.end();
  }
}

removeLegacyTable().catch(console.error);
