require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function fixAiAgents() {
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
    console.log('🔧 Fixing ai_agents table...\n');

    // Check current user_id column type
    const [userIdCol] = await connection.execute(`
      SELECT COLUMN_TYPE, DATA_TYPE
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_agents' AND COLUMN_NAME = 'user_id'
    `, [dbName]);

    console.log('Current ai_agents.user_id type:', userIdCol[0].COLUMN_TYPE);

    // Convert to BIGINT if needed
    if (userIdCol[0].COLUMN_TYPE !== 'bigint') {
      console.log('Converting ai_agents.user_id to BIGINT...');
      await connection.execute('ALTER TABLE ai_agents MODIFY COLUMN user_id BIGINT NOT NULL');
      console.log('✓ Converted to BIGINT\n');
    } else {
      console.log('✓ Already BIGINT\n');
    }

    // Check if foreign key already exists
    const [existingFk] = await connection.execute(`
      SELECT CONSTRAINT_NAME
      FROM information_schema.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_agents' AND COLUMN_NAME = 'user_id'
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [dbName]);

    if (existingFk.length > 0) {
      console.log(`Foreign key already exists: ${existingFk[0].CONSTRAINT_NAME}`);
    } else {
      console.log('Adding foreign key constraint...');
      await connection.execute(`
        ALTER TABLE ai_agents
        ADD CONSTRAINT fk_ai_agents_user
        FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
      `);
      console.log('✓ Added fk_ai_agents_user -> signup_users(id)');
    }

    console.log('\n✅ ai_agents table fixed successfully!');

  } catch (error) {
    console.error('\n❌ Failed:', error.message);
    console.error('Error code:', error.code);
    throw error;
  } finally {
    await connection.end();
  }
}

fixAiAgents().catch(console.error);
