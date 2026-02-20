const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.log('\n=== Active Dialer Calls by Campaign ===\n');
  
  const [campaigns] = await pool.query(
    `SELECT 
       c.id as campaign_id,
       c.name as campaign_name,
       c.status as campaign_status,
       c.concurrency_limit,
       c.ai_agent_id,
       a.display_name as agent_name,
       a.pipecat_agent_name,
       COUNT(CASE WHEN l.status IN ('queued', 'dialing') THEN 1 END) as active_leads,
       COUNT(CASE WHEN l.status = 'pending' THEN 1 END) as pending_leads
     FROM dialer_campaigns c
     LEFT JOIN dialer_leads l ON l.campaign_id = c.id AND l.user_id = c.user_id
     LEFT JOIN ai_agents a ON a.id = c.ai_agent_id
     WHERE c.status = 'running'
     GROUP BY c.id
     ORDER BY active_leads DESC`
  );
  
  console.table(campaigns);
  
  console.log('\n=== Active Calls by Pipecat Service ===\n');
  
  const [byService] = await pool.query(
    `SELECT 
       a.pipecat_agent_name as service,
       COUNT(*) as active_calls
     FROM dialer_leads l
     JOIN dialer_campaigns c ON c.id = l.campaign_id
     JOIN ai_agents a ON a.id = c.ai_agent_id
     WHERE l.status IN ('queued', 'dialing')
     GROUP BY a.pipecat_agent_name`
  );
  
  console.table(byService);
  
  console.log('\n=== Recent Failed Calls (last 10) ===\n');
  
  const [failed] = await pool.query(
    `SELECT 
       l.id,
       l.campaign_id,
       l.call_id,
       l.status,
       l.result,
       l.notes,
       l.created_at,
       a.pipecat_agent_name as service
     FROM dialer_call_logs l
     LEFT JOIN dialer_campaigns c ON c.id = l.campaign_id
     LEFT JOIN ai_agents a ON a.id = c.ai_agent_id
     WHERE l.status = 'error' AND l.notes LIKE '%maximum instances%'
     ORDER BY l.created_at DESC
     LIMIT 10`
  );
  
  console.table(failed.map(f => ({
    id: f.id,
    campaign: f.campaign_id,
    service: f.service,
    time: f.created_at.toISOString().split('T')[1].split('.')[0],
    notes: f.notes?.substring(0, 50)
  })));
  
  await pool.end();
})().catch(console.error);
