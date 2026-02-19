#!/usr/bin/env node
require('dotenv').config();
const mysql = require('mysql2/promise');

async function main() {
  const pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'phonesystem',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });

  console.log('\n========================================');
  console.log('DIALER FAILURE DIAGNOSTIC REPORT');
  console.log('========================================\n');

  try {
    // Query 1: Check failed lead statuses
    console.log('1. Lead Status Breakdown:');
    console.log('------------------------------------------');
    const [leadStats] = await pool.query(`
      SELECT status, COUNT(*) as count 
      FROM dialer_leads 
      WHERE campaign_id IN (SELECT id FROM dialer_campaigns WHERE name IN ('test', 'Trump', 'John')) 
      GROUP BY status
      ORDER BY count DESC
    `);
    console.table(leadStats);

    // Query 2: Check call log results
    console.log('\n2. Call Log Status & Result Breakdown:');
    console.log('------------------------------------------');
    const [callStats] = await pool.query(`
      SELECT status, result, COUNT(*) as count 
      FROM dialer_call_logs 
      WHERE campaign_id IN (SELECT id FROM dialer_campaigns WHERE name IN ('test', 'Trump', 'John')) 
      GROUP BY status, result
      ORDER BY count DESC
    `);
    console.table(callStats);

    // Query 3: Check for error notes
    console.log('\n3. Error Notes (Top 20):');
    console.log('------------------------------------------');
    const [errorNotes] = await pool.query(`
      SELECT notes, COUNT(*) as count 
      FROM dialer_call_logs 
      WHERE campaign_id IN (SELECT id FROM dialer_campaigns WHERE name IN ('test', 'Trump', 'John')) 
      AND notes IS NOT NULL 
      GROUP BY notes 
      ORDER BY count DESC
      LIMIT 20
    `);
    if (errorNotes.length === 0) {
      console.log('No error notes found in call logs.');
    } else {
      console.table(errorNotes);
    }

    // Additional diagnostics
    console.log('\n4. Recent Failed Calls (Last 10):');
    console.log('------------------------------------------');
    const [recentFailed] = await pool.query(`
      SELECT 
        dcl.id,
        dc.name as campaign,
        dl.phone_number,
        dcl.status,
        dcl.result,
        dcl.notes,
        dcl.created_at
      FROM dialer_call_logs dcl
      JOIN dialer_campaigns dc ON dcl.campaign_id = dc.id
      LEFT JOIN dialer_leads dl ON dcl.lead_id = dl.id
      WHERE dc.name IN ('test', 'Trump', 'John')
      AND dcl.status IN ('failed', 'error')
      ORDER BY dcl.created_at DESC
      LIMIT 10
    `);
    if (recentFailed.length === 0) {
      console.log('No recent failed calls found.');
    } else {
      console.table(recentFailed);
    }

    console.log('\n========================================\n');

  } catch (error) {
    console.error('Error running diagnostic queries:', error.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main().catch(console.error);
