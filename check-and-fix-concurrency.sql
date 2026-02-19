-- Check current concurrency settings and active calls
SELECT 
  'Current Campaign Settings' AS section,
  c.id,
  c.name,
  c.status,
  c.concurrency_limit,
  COUNT(CASE WHEN l.status IN ('queued', 'dialing') THEN 1 END) AS active_calls
FROM dialer_campaigns c
LEFT JOIN dialer_leads l ON l.campaign_id = c.id
WHERE c.status IN ('running', 'paused')
GROUP BY c.id, c.name, c.status, c.concurrency_limit
ORDER BY c.id DESC;

-- Show total potential concurrent load
SELECT 
  'Total System Load' AS section,
  SUM(concurrency_limit) AS max_possible_concurrent_calls,
  SUM(CASE WHEN status = 'running' THEN concurrency_limit ELSE 0 END) AS max_running_concurrent_calls
FROM dialer_campaigns
WHERE status IN ('running', 'paused');

-- Update all campaigns to safe concurrency limit of 3
UPDATE dialer_campaigns 
SET concurrency_limit = 3 
WHERE concurrency_limit > 3 
  AND status IN ('running', 'paused', 'draft');

-- Verify the update
SELECT 
  'After Update' AS section,
  id,
  name,
  status,
  concurrency_limit
FROM dialer_campaigns
WHERE status IN ('running', 'paused', 'draft')
ORDER BY id DESC;
