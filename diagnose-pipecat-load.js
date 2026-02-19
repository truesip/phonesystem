require('dotenv').config();
const mysql = require('mysql2/promise');

async function run() {
  const pool = mysql.createPool(process.env.DATABASE_URL);
  
  try {
    console.log('\n=== PIPECAT LOAD DIAGNOSTICS ===\n');
    
    // 1. Check campaign concurrency settings
    const [campaigns] = await pool.query(`
      SELECT 
        id,
        name,
        status,
        concurrency_limit,
        ai_agent_name
      FROM dialer_campaigns 
      WHERE status IN ('running', 'paused')
      ORDER BY status DESC, id DESC
    `);
    
    console.log('📊 Campaign Concurrency Settings:');
    console.table(campaigns);
    
    // 2. Check actual in-flight calls
    const [activeCalls] = await pool.query(`
      SELECT 
        c.id AS campaign_id,
        c.name AS campaign_name,
        c.concurrency_limit,
        COUNT(l.id) AS active_leads,
        c.status AS campaign_status
      FROM dialer_campaigns c
      LEFT JOIN dialer_leads l ON l.campaign_id = c.id AND l.status IN ('queued', 'dialing')
      WHERE c.status = 'running'
      GROUP BY c.id, c.name, c.concurrency_limit, c.status
      ORDER BY active_leads DESC
    `);
    
    console.log('\n🔥 Active Calls Right Now:');
    console.table(activeCalls);
    
    // 3. Calculate total load
    const totalConfiguredConcurrency = campaigns
      .filter(c => c.status === 'running')
      .reduce((sum, c) => sum + Number(c.concurrency_limit || 0), 0);
    
    const totalActiveCalls = activeCalls
      .reduce((sum, c) => sum + Number(c.active_leads || 0), 0);
    
    console.log('\n📈 System-Wide Load:');
    console.log(`   Max Possible Concurrent: ${totalConfiguredConcurrency} calls`);
    console.log(`   Currently Active: ${totalActiveCalls} calls`);
    console.log(`   Pipecat Limit (suspected): ~10-15 instances`);
    
    if (totalConfiguredConcurrency > 15) {
      console.log('\n⚠️  WARNING: Total configured concurrency exceeds suspected Pipecat limit!');
      console.log('   This is likely causing "Service maximum instances reached" errors.');
      console.log('\n💡 Recommendation: Run fix-concurrency.js to reduce all campaigns to 3');
    } else if (totalConfiguredConcurrency > 10) {
      console.log('\n⚠️  CAUTION: Total concurrency is close to Pipecat limit.');
    } else {
      console.log('\n✅ Total concurrency looks safe.');
    }
    
    // 4. Recent error logs from database
    const [recentErrors] = await pool.query(`
      SELECT 
        campaign_id,
        result,
        notes,
        created_at
      FROM dialer_call_logs
      WHERE status = 'error' 
        AND notes LIKE '%maximum instances%'
      ORDER BY created_at DESC
      LIMIT 10
    `);
    
    if (recentErrors.length > 0) {
      console.log('\n❌ Recent "Maximum Instances" Errors:');
      console.table(recentErrors.map(e => ({
        campaign_id: e.campaign_id,
        time: e.created_at,
        error: String(e.notes || '').substring(0, 80)
      })));
    }
    
  } catch (err) {
    console.error('✗ Error:', err.message);
  } finally {
    await pool.end();
  }
}

run();
