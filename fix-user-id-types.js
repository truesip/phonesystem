require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function fixUserIdTypes() {
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
    console.log('🚀 Starting user_id column type migration...\n');

    // Step 1: Find all foreign keys pointing to legacy 'users' table
    console.log('Step 1: Finding foreign keys to drop...');
    const [fkResults] = await connection.execute(`
      SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ?
        AND REFERENCED_TABLE_NAME = 'users'
        AND REFERENCED_COLUMN_NAME = 'id'
    `, [dbName]);

    console.log(`Found ${fkResults.length} foreign key(s) to drop\n`);

    // Step 2: Drop all foreign keys
    console.log('Step 2: Dropping foreign key constraints...');
    for (const fk of fkResults) {
      try {
        await connection.execute(`ALTER TABLE ${fk.TABLE_NAME} DROP FOREIGN KEY ${fk.CONSTRAINT_NAME}`);
        console.log(`  ✓ Dropped ${fk.TABLE_NAME}.${fk.CONSTRAINT_NAME}`);
      } catch (error) {
        if (error.code === 'ER_CANT_DROP_FIELD_OR_KEY') {
          console.log(`  ⚠ ${fk.TABLE_NAME}.${fk.CONSTRAINT_NAME} already dropped`);
        } else {
          throw error;
        }
      }
    }
    console.log('');

    // Step 3: Convert user_id columns from INT UNSIGNED to BIGINT
    console.log('Step 3: Converting user_id columns to BIGINT...');
    const tables = [...new Set(fkResults.map(fk => fk.TABLE_NAME))];
    
    for (const tableName of tables) {
      try {
        await connection.execute(`ALTER TABLE ${tableName} MODIFY COLUMN user_id BIGINT NOT NULL`);
        console.log(`  ✓ Converted ${tableName}.user_id to BIGINT`);
      } catch (error) {
        console.log(`  ⚠ ${tableName}.user_id conversion failed: ${error.message}`);
      }
    }
    console.log('');

    // Step 4: Add foreign keys pointing to signup_users
    console.log('Step 4: Adding new foreign keys to signup_users...');
    for (const tableName of tables) {
      try {
        const constraintName = `fk_${tableName}_user`;
        await connection.execute(`
          ALTER TABLE ${tableName}
          ADD CONSTRAINT ${constraintName}
          FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
        `);
        console.log(`  ✓ Added ${tableName}.${constraintName} -> signup_users(id)`);
      } catch (error) {
        if (error.code === 'ER_DUP_KEYNAME') {
          console.log(`  ⚠ ${tableName} foreign key already exists`);
        } else {
          console.log(`  ❌ ${tableName} foreign key failed: ${error.message}`);
        }
      }
    }
    console.log('');

    // Step 5: Check for ai_agents specifically (from original error)
    console.log('Step 5: Verifying ai_agents table...');
    const [aiAgentsFk] = await connection.execute(`
      SELECT CONSTRAINT_NAME, REFERENCED_TABLE_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_agents' AND COLUMN_NAME = 'user_id'
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [dbName]);
    
    if (aiAgentsFk.length > 0) {
      console.log(`  ✓ ai_agents.user_id -> ${aiAgentsFk[0].REFERENCED_TABLE_NAME}(id)`);
    } else {
      console.log('  ⚠ ai_agents has no foreign key on user_id');
    }
    console.log('');

    // Step 6: Verify all tables now reference signup_users
    console.log('Step 6: Final verification...');
    const [verifyResults] = await connection.execute(`
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

    console.log(`\nAll user_id foreign keys (${verifyResults.length}):`);
    verifyResults.forEach(fk => {
      const status = fk.REFERENCED_TABLE_NAME === 'signup_users' ? '✅' : '⚠️';
      console.log(`  ${status} ${fk.TABLE_NAME} -> ${fk.REFERENCED_TABLE_NAME}(id)`);
    });

    // Check if any still point to legacy users table
    const legacyRefs = verifyResults.filter(fk => fk.REFERENCED_TABLE_NAME === 'users');
    if (legacyRefs.length > 0) {
      console.log('\n⚠️  WARNING: Some tables still reference legacy "users" table:');
      legacyRefs.forEach(fk => console.log(`  - ${fk.TABLE_NAME}`));
    } else {
      console.log('\n✅ All foreign keys now point to signup_users!');
    }

    console.log('\n🎉 Migration completed successfully!');

  } catch (error) {
    console.error('\n❌ Migration failed:', error.message);
    console.error('Error code:', error.code);
    throw error;
  } finally {
    await connection.end();
  }
}

fixUserIdTypes().catch(console.error);
