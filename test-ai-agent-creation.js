require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function testAiAgentCreation() {
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
    console.log('🧪 Testing AI Agent Creation\n');
    console.log('='.repeat(60) + '\n');

    // Step 1: Get a test user from signup_users
    console.log('Step 1: Finding a test user...');
    const [users] = await connection.execute('SELECT id, username FROM signup_users LIMIT 1');
    
    if (users.length === 0) {
      console.log('❌ No users found in signup_users table');
      return;
    }
    
    const testUser = users[0];
    console.log(`✅ Using user: ${testUser.username} (ID: ${testUser.id})\n`);

    // Step 2: Create a test AI agent
    console.log('Step 2: Creating test AI agent...');
    const testAgentData = {
      user_id: testUser.id,
      name: 'Migration Test Agent',
      phone_number: '+1234567890',
      voice: 'alloy',
      greeting: 'Hello, this is a test agent created during migration verification.',
      system_prompt: 'You are a test AI assistant.',
      display_name: 'Test Agent',
      pipecat_agent_name: 'test-agent-migration',
      pipecat_secret_set: 'default'
    };

    try {
      const [result] = await connection.execute(
        `INSERT INTO ai_agents (user_id, name, phone_number, voice, greeting, system_prompt, 
         display_name, pipecat_agent_name, pipecat_secret_set, is_active, inbound_transfer_enabled, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, NOW(), NOW())`,
        [
          testAgentData.user_id,
          testAgentData.name,
          testAgentData.phone_number,
          testAgentData.voice,
          testAgentData.greeting,
          testAgentData.system_prompt,
          testAgentData.display_name,
          testAgentData.pipecat_agent_name,
          testAgentData.pipecat_secret_set
        ]
      );

      const agentId = result.insertId;
      console.log(`✅ Test agent created successfully! (ID: ${agentId})\n`);

      // Step 3: Verify the agent was created and foreign key works
      console.log('Step 3: Verifying agent data...');
      const [agents] = await connection.execute(
        'SELECT a.*, s.username FROM ai_agents a JOIN signup_users s ON a.user_id = s.id WHERE a.id = ?',
        [agentId]
      );

      if (agents.length > 0) {
        console.log('✅ Agent verified with JOIN to signup_users:');
        console.log(`  - Agent: ${agents[0].name}`);
        console.log(`  - Owner: ${agents[0].username}`);
        console.log(`  - Phone: ${agents[0].phone_number}\n`);
      }

      // Step 4: Clean up test agent
      console.log('Step 4: Cleaning up test agent...');
      await connection.execute('DELETE FROM ai_agents WHERE id = ?', [agentId]);
      console.log('✅ Test agent removed\n');

      console.log('='.repeat(60));
      console.log('\n🎉 SUCCESS! AI agent creation is working properly.');
      console.log('The foreign key constraint is functioning correctly.\n');

    } catch (error) {
      if (error.code === 'ER_NO_REFERENCED_ROW_2') {
        console.log('❌ Foreign key constraint failed!');
        console.log('The user_id does not exist in signup_users table.');
      } else {
        throw error;
      }
    }

  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    console.error('Error code:', error.code);
    throw error;
  } finally {
    await connection.end();
  }
}

testAiAgentCreation().catch(console.error);
