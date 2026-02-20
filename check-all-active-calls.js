const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.log('\n=== All Active AI Inbound Calls ===\n');
  
  const [inbound] = await pool.query(
    `SELECT 
       id, user_id, agent_id, call_id, status, 
       from_number, to_number,
       created_at, updated_at,
       TIMESTAMPDIFF(MINUTE, created_at, NOW()) as age_minutes
     FROM ai_call_logs
     WHERE status IN ('initiated', 'ringing', 'connected', 'active')
     ORDER BY created_at DESC
     LIMIT 100`
  );
  
  console.log(`Found ${inbound.length} active inbound calls:\n`);
  console.table(inbound.map(c => ({
    id: c.id,
    user: c.user_id,
    agent: c.agent_id,
    status: c.status,
    from: c.from_number,
    age_min: c.age_minutes,
    created: c.created_at.toISOString().split('T')[1].split('.')[0]
  })));
  
  console.log('\n=== All Active Dialer (Outbound) Calls ===\n');
  
  const [outbound] = await pool.query(
    `SELECT 
       l.id, l.user_id, l.campaign_id, l.lead_id, l.call_id, l.status,
       c.name as campaign_name, c.ai_agent_id,
       a.display_name as agent_name,
       TIMESTAMPDIFF(MINUTE, l.created_at, NOW()) as age_minutes
     FROM dialer_call_logs l
     LEFT JOIN dialer_campaigns c ON c.id = l.campaign_id
     LEFT JOIN ai_agents a ON a.id = c.ai_agent_id
     WHERE l.status IN ('queued', 'dialing')
     ORDER BY l.created_at DESC
     LIMIT 100`
  );
  
  console.log(`Found ${outbound.length} active outbound calls:\n`);
  console.table(outbound.map(c => ({
    id: c.id,
    user: c.user_id,
    campaign: c.campaign_name,
    agent: c.agent_name,
    status: c.status,
    age_min: c.age_minutes
  })));
  
  console.log('\n=== Summary ===\n');
  console.log(`Total tracked active calls: ${inbound.length + outbound.length}`);
  console.log(`Pipecat reports: 50 active sessions`);
  console.log(`Difference: ${50 - (inbound.length + outbound.length)} untracked sessions`);
  
  if (50 - (inbound.length + outbound.length) > 0) {
    console.log('\n⚠️  Possible causes of untracked sessions:');
    console.log('  - Stuck/zombie sessions that never closed');
    console.log('  - Calls from other users/systems using same Pipecat org');
    console.log('  - Database records not updated (webhook delays)');
  }
  
  await pool.end();
})().catch(console.error);
