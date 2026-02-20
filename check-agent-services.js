const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.log('\n=== AI Agents and their Pipecat Services ===\n');
  
  const [agents] = await pool.query(
    `SELECT id, user_id, display_name, pipecat_agent_name, pipecat_region,
            created_at
     FROM ai_agents
     ORDER BY user_id, id`
  );
  
  console.table(agents.map(a => ({
    id: a.id,
    user: a.user_id,
    name: a.display_name,
    pipecat_service: a.pipecat_agent_name,
    region: a.pipecat_region,
    created: a.created_at.toISOString().split('T')[0]
  })));
  
  // Check if multiple users are sharing the same service
  const serviceUsage = {};
  agents.forEach(a => {
    const svc = a.pipecat_agent_name;
    if (!serviceUsage[svc]) {
      serviceUsage[svc] = { users: new Set(), agents: 0 };
    }
    serviceUsage[svc].users.add(a.user_id);
    serviceUsage[svc].agents++;
  });
  
  console.log('\n=== Service Usage Summary ===\n');
  Object.entries(serviceUsage).forEach(([svc, data]) => {
    console.log(`${svc}:`);
    console.log(`  - ${data.agents} agents`);
    console.log(`  - ${data.users.size} users: [${Array.from(data.users).join(', ')}]`);
  });
  
  await pool.end();
})().catch(console.error);
