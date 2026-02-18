require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function verifyAndCleanup() {
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
    console.log('🔍 Final Verification and Cleanup\n');
    console.log('='.repeat(60) + '\n');

    // Step 1: Check for any remaining foreign keys to legacy users table
    console.log('Step 1: Checking for legacy foreign key references...');
    const [legacyFks] = await connection.execute(`
      SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ?
        AND REFERENCED_TABLE_NAME = 'users'
        AND REFERENCED_COLUMN_NAME = 'id'
    `, [dbName]);

    if (legacyFks.length > 0) {
      console.log(`⚠️  Found ${legacyFks.length} foreign key(s) still pointing to legacy 'users' table:`);
      legacyFks.forEach(fk => {
        console.log(`  - ${fk.TABLE_NAME}.${fk.COLUMN_NAME} (${fk.CONSTRAINT_NAME})`);
      });
      console.log('\n❌ Cannot proceed with cleanup until these are fixed.\n');
      return;
    } else {
      console.log('✅ No foreign keys reference legacy "users" table\n');
    }

    // Step 2: Compare user counts
    console.log('Step 2: Comparing user data...');
    const [legacyCount] = await connection.execute('SELECT COUNT(*) as count FROM users');
    const [currentCount] = await connection.execute('SELECT COUNT(*) as count FROM signup_users');
    
    console.log(`  Legacy users table: ${legacyCount[0].count} users`);
    console.log(`  Current signup_users table: ${currentCount[0].count} users`);
    
    if (legacyCount[0].count > currentCount[0].count) {
      console.log(`\n⚠️  WARNING: Legacy table has more users! (${legacyCount[0].count - currentCount[0].count} more)`);
      console.log('You may want to migrate remaining users first.\n');
    } else {
      console.log('✅ All users migrated\n');
    }

    // Step 3: Check if billing_history still references users table
    console.log('Step 3: Checking billing_history references...');
    const [billingFks] = await connection.execute(`
      SELECT CONSTRAINT_NAME, REFERENCED_TABLE_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'billing_history' AND COLUMN_NAME = 'user_id'
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [dbName]);
    
    if (billingFks.length > 0) {
      billingFks.forEach(fk => {
        const status = fk.REFERENCED_TABLE_NAME === 'signup_users' ? '✅' : '⚠️';
        console.log(`  ${status} billing_history.user_id -> ${fk.REFERENCED_TABLE_NAME}(id)`);
      });
    } else {
      console.log('  ℹ️  No foreign key on billing_history.user_id');
    }
    console.log('');

    // Step 4: Summary of all user_id foreign keys
    console.log('Step 4: Summary of all user_id foreign keys...');
    const [allFks] = await connection.execute(`
      SELECT 
        TABLE_NAME,
        CONSTRAINT_NAME,
        REFERENCED_TABLE_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ?
        AND COLUMN_NAME = 'user_id'
        AND REFERENCED_TABLE_NAME IS NOT NULL
      ORDER BY TABLE_NAME
    `, [dbName]);

    const signupUsersRefs = allFks.filter(fk => fk.REFERENCED_TABLE_NAME === 'signup_users');
    const legacyUsersRefs = allFks.filter(fk => fk.REFERENCED_TABLE_NAME === 'users');
    
    console.log(`  Total foreign keys: ${allFks.length}`);
    console.log(`  ✅ Pointing to signup_users: ${signupUsersRefs.length}`);
    console.log(`  ⚠️  Pointing to legacy users: ${legacyUsersRefs.length}`);
    console.log('');

    // Step 5: Ready to remove?
    console.log('='.repeat(60));
    if (legacyUsersRefs.length === 0) {
      console.log('\n✅ SAFE TO REMOVE LEGACY USERS TABLE\n');
      console.log('To remove the legacy table, run:\n');
      console.log('  -- Backup first (optional but recommended)');
      console.log('  CREATE TABLE users_backup AS SELECT * FROM users;\n');
      console.log('  -- Then drop the legacy table');
      console.log('  DROP TABLE IF EXISTS users;\n');
      
      // Optionally drop the table automatically
      console.log('Would you like to drop the legacy users table now? (Y/N)');
      console.log('Note: This script will NOT automatically drop it for safety.');
      console.log('Run the SQL commands above manually when ready.\n');
    } else {
      console.log('\n⚠️  NOT SAFE - Fix remaining foreign keys first\n');
    }

  } catch (error) {
    console.error('\n❌ Error:', error.message);
    throw error;
  } finally {
    await connection.end();
  }
}

verifyAndCleanup().catch(console.error);
