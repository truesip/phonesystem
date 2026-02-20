const mysql = require('mysql2/promise');
require('dotenv').config();

(async () => {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  console.log('\n=== Cleaning Up Stuck Dialer Calls ===\n');
  
  // Mark calls stuck for more than 1 hour as failed
  const [result] = await pool.execute(
    `UPDATE dialer_call_logs
     SET status = 'error',
         result = 'failed',
         notes = 'Timed out - stuck for over 1 hour'
     WHERE status IN ('queued', 'dialing')
       AND TIMESTAMPDIFF(MINUTE, created_at, NOW()) > 60`
  );
  
  console.log(`✓ Cleaned up ${result.affectedRows} stuck call log records\n`);
  
  // Update corresponding leads
  const [leadResult] = await pool.execute(
    `UPDATE dialer_leads l
     INNER JOIN dialer_call_logs cl ON cl.lead_id = l.id
     SET l.status = 'failed'
     WHERE l.status IN ('queued', 'dialing')
       AND cl.status = 'error'
       AND cl.notes LIKE '%stuck%'`
  );
  
  console.log(`✓ Updated ${leadResult.affectedRows} lead records\n`);
  
  // Show summary
  const [summary] = await pool.query(
    `SELECT 
       status,
       COUNT(*) as count
     FROM dialer_call_logs
     WHERE status IN ('queued', 'dialing', 'error')
     GROUP BY status`
  );
  
  console.log('Current status summary:');
  console.table(summary);
  
  await pool.end();
  
  console.log('\n✅ Cleanup complete!');
})().catch(console.error);
