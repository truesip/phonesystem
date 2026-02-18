-- Performance Indexes for 1 Million User Scalability
-- Run this script on your production database
-- These indexes optimize the most frequent queries

-- AI Call Logs Indexes
ALTER TABLE ai_call_logs 
ADD INDEX IF NOT EXISTS idx_user_time (user_id, time_start),
ADD INDEX IF NOT EXISTS idx_user_billed (user_id, billed),
ADD INDEX IF NOT EXISTS idx_billed_status (billed, status);

-- CDRs Indexes  
ALTER TABLE cdrs 
ADD INDEX IF NOT EXISTS idx_user_time (user_id, time_start),
ADD INDEX IF NOT EXISTS idx_time_range (time_start, time_end);

-- User DIDs Indexes
ALTER TABLE user_dids
ADD INDEX IF NOT EXISTS idx_user_created (user_id, created_at),
ADD INDEX IF NOT EXISTS idx_cancel_pending (cancel_pending, user_id);

-- AI Numbers Indexes
ALTER TABLE ai_numbers
ADD INDEX IF NOT EXISTS idx_user_created (user_id, created_at),
ADD INDEX IF NOT EXISTS idx_cancel_pending (cancel_pending, user_id),
ADD INDEX IF NOT EXISTS idx_active (is_active, user_id);

-- Dialer Campaigns Indexes
ALTER TABLE dialer_campaigns
ADD INDEX IF NOT EXISTS idx_user_status (user_id, status),
ADD INDEX IF NOT EXISTS idx_status_updated (status, updated_at);

-- Dialer Leads Indexes
ALTER TABLE dialer_leads
ADD INDEX IF NOT EXISTS idx_campaign_status (campaign_id, status),
ADD INDEX IF NOT EXISTS idx_campaign_pending (campaign_id, status, last_attempt_at);

-- Sessions Indexes (if not using Redis yet)
ALTER TABLE sessions
ADD INDEX IF NOT EXISTS idx_expires (expires);

-- AI Agents Indexes
ALTER TABLE ai_agents
ADD INDEX IF NOT EXISTS idx_user_active (user_id, is_active);

-- User Trunks Indexes
ALTER TABLE user_trunks
ADD INDEX IF NOT EXISTS idx_user_created (user_id, created_at);

-- Billing History - Additional covering index
ALTER TABLE billing_history
ADD INDEX IF NOT EXISTS idx_user_status_created (user_id, status, created_at);

-- AI Call Messages - For conversation history
ALTER TABLE ai_call_messages
ADD INDEX IF NOT EXISTS idx_user_call (user_id, call_domain, call_id, created_at);

-- Analyze tables after adding indexes
ANALYZE TABLE signup_users;
ANALYZE TABLE billing_history;
ANALYZE TABLE ai_call_logs;
ANALYZE TABLE cdrs;
ANALYZE TABLE user_dids;
ANALYZE TABLE ai_numbers;
ANALYZE TABLE dialer_campaigns;
ANALYZE TABLE dialer_leads;
ANALYZE TABLE ai_agents;

-- Show index sizes
SELECT 
    TABLE_NAME,
    ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS "Size (MB)",
    ROUND((INDEX_LENGTH / 1024 / 1024), 2) AS "Index Size (MB)",
    TABLE_ROWS as "Rows"
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME IN (
        'signup_users', 'billing_history', 'ai_call_logs', 
        'cdrs', 'user_dids', 'ai_numbers', 'dialer_campaigns', 
        'dialer_leads', 'ai_agents', 'sessions'
    )
ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;
