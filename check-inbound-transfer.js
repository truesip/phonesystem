require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

async function checkInboundTransfer() {
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
    const [agents] = await connection.execute(`
      SELECT id, display_name, inbound_transfer_enabled, inbound_transfer_number
      FROM ai_agents
      ORDER BY id DESC
    `);

    console.log('\nAI Agents - Inbound Transfer Settings:\n');
    console.log('ID | Display Name | Enabled | Number');
    console.log('-'.repeat(70));
    
    for (const agent of agents) {
      const enabled = agent.inbound_transfer_enabled ? 'YES' : 'NO';
      const number = agent.inbound_transfer_number || '(null)';
      console.log(`${agent.id} | ${agent.display_name} | ${enabled} | ${number}`);
    }
    
    console.log('');
  } finally {
    await connection.end();
  }
}

checkInboundTransfer().catch(console.error);
