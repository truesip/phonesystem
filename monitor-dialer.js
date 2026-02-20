const mysql = require('mysql2/promise');
require('dotenv').config();

async function monitor() {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.clear();
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║         DIALER MONITORING DASHBOARD                     ║');
  console.log('║         Press Ctrl+C to exit                             ║');
  console.log('╚══════════════════════════════════════════════════════════╝\n');
  
  const now = new Date().toISOString().split('T')[1].split('.')[0];
  console.log(`🕐 Last updated: ${now}\n`);
  
  // Running campaigns
  const [campaigns] = await pool.query(
    `SELECT 
       c.id, c.name, c.concurrency_limit,
       COUNT(CASE WHEN l.status IN ('queued', 'dialing') THEN 1 END) as active,
       COUNT(CASE WHEN l.status = 'pending' THEN 1 END) as pending
     FROM dialer_campaigns c
     LEFT JOIN dialer_leads l ON l.campaign_id = c.id
     WHERE c.status = 'running'
     GROUP BY c.id`
  );
  
  console.log('📊 RUNNING CAMPAIGNS:');
  if (campaigns.length === 0) {
    console.log('   No campaigns running\n');
  } else {
    console.table(campaigns.map(c => ({
      ID: c.id,
      Name: c.name.substring(0, 20),
      'Max Concurrent': c.concurrency_limit,
      Active: c.active,
      Pending: c.pending
    })));
  }
  
  // Recent errors (last 5 minutes)
  const [errors] = await pool.query(
    `SELECT COUNT(*) as count
     FROM dialer_call_logs
     WHERE notes LIKE '%maximum instances%'
       AND created_at > DATE_SUB(NOW(), INTERVAL 5 MINUTE)`
  );
  
  const errorCount = errors[0]?.count || 0;
  if (errorCount > 0) {
    console.log(`\n❌ RATE LIMIT ERRORS: ${errorCount} in last 5 minutes`);
  } else {
    console.log('\n✅ NO RATE LIMIT ERRORS in last 5 minutes');
  }
  
  // Call stats (last 10 minutes)
  const [stats] = await pool.query(
    `SELECT 
       COUNT(*) as total,
       COUNT(CASE WHEN status = 'dialing' THEN 1 END) as dialing,
       COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
       COUNT(CASE WHEN status = 'error' THEN 1 END) as failed
     FROM dialer_call_logs
     WHERE created_at > DATE_SUB(NOW(), INTERVAL 10 MINUTE)`
  );
  
  const s = stats[0] || {};
  console.log('\n📞 CALLS (Last 10 min):');
  console.log(`   Total: ${s.total || 0} | Dialing: ${s.dialing || 0} | Completed: ${s.completed || 0} | Failed: ${s.failed || 0}`);
  
  await pool.end();
}

// Run once
monitor().catch(console.error);
