-- Remove legacy 'users' table safely
-- Run this AFTER confirming all foreign keys have been fixed
-- and all data has been migrated to signup_users

-- Step 1: Verify all foreign keys now point to signup_users, not users
SELECT 
    CONCAT('⚠️  WARNING: ', TABLE_NAME, ' still references legacy users table via ', CONSTRAINT_NAME) AS warning,
    TABLE_NAME,
    CONSTRAINT_NAME,
    COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
    AND REFERENCED_TABLE_NAME = 'users'
    AND REFERENCED_COLUMN_NAME = 'id';

-- If the above query returns any rows, DO NOT proceed!
-- First run db/fix_ai_agents_fk.sql to fix those constraints

-- Step 2: Check if users table still exists
SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS 'Size (MB)'
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'users';

-- Step 3: Compare user counts between tables
SELECT 
    'Legacy users table' AS source,
    COUNT(*) AS user_count
FROM users
UNION ALL
SELECT 
    'Current signup_users table' AS source,
    COUNT(*) AS user_count
FROM signup_users;

-- Step 4: Identify users in legacy table not in signup_users (if any)
SELECT 
    u.id,
    u.username,
    u.email,
    u.created_at,
    'NOT IN signup_users' AS status
FROM users u
LEFT JOIN signup_users s ON u.username = s.username OR u.email = s.email
WHERE s.id IS NULL;

-- If the above query returns rows, you may want to migrate them first

-- Step 5: BACKUP the users table before dropping (optional but recommended)
-- CREATE TABLE users_backup AS SELECT * FROM users;

-- Step 6: Drop the legacy users table
-- UNCOMMENT the line below only after verifying steps 1-4
-- DROP TABLE IF EXISTS users;

-- Step 7: Verify the drop was successful
SELECT 
    TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME IN ('users', 'users_backup');

-- Expected result: only users_backup should exist (if you created it)
