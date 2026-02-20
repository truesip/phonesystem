const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.log('\n=== Recent Rate Limit Errors (Last 4 Hours) ===\n');
  
  const [errors] = await pool.query(
    `SELECT 
       id, campaign_id, lead_id, call_id, status, result,
       LEFT(notes, 100) as notes,
       created_at,
       TIMESTAMPDIFF(MINUTE, created_at, NOW()) as minutes_ago
     FROM dialer_call_logs
     WHERE notes LIKE '%maximum instances%'
       AND created_at > DATE_SUB(NOW(), INTERVAL 4 HOUR)
     ORDER BY created_at DESC
     LIMIT 20`
  );
  
  if (errors.length === 0) {
    console.log('✅ No rate limit errors in the last 4 hours!');
  } else {
    console.log(`Found ${errors.length} rate limit errors:\n`);
    console.table(errors.map(e => ({
      id: e.id,
      campaign: e.campaign_id,
      lead: e.lead_id,
      minutes_ago: e.minutes_ago,
      time: e.created_at.toISOString().split('T')[1].split('.')[0],
      notes: e.notes
    })));
  }
  
  console.log('\n=== Recent Successful Calls (Last Hour) ===\n');
  
  const [success] = await pool.query(
    `SELECT 
       COUNT(*) as total,
       COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
       COUNT(CASE WHEN status = 'dialing' THEN 1 END) as dialing,
       COUNT(CASE WHEN status = 'error' THEN 1 END) as errors
     FROM dialer_call_logs
     WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)`
  );
  
  console.table(success);
  
  console.log('\n=== Campaign Status ===\n');
  
  const [campaigns] = await pool.query(
    `SELECT 
       c.id, c.name, c.status, c.concurrency_limit,
       COUNT(CASE WHEN l.status IN ('queued', 'dialing') THEN 1 END) as active_calls,
       COUNT(CASE WHEN l.status = 'pending' THEN 1 END) as pending_leads
     FROM dialer_campaigns c
     LEFT JOIN dialer_leads l ON l.campaign_id = c.id
     WHERE c.status = 'running'
     GROUP BY c.id`
  );
  
  console.table(campaigns);
  
  await pool.end();
})().catch(console.error);
