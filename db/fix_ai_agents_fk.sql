-- Fix foreign key constraints to reference signup_users instead of legacy users table
-- This fixes the error: Cannot add or update a child row: a foreign key constraint fails

-- First, find all tables with foreign keys pointing to 'users' table
SELECT 
    CONCAT('ALTER TABLE `', TABLE_NAME, '` DROP FOREIGN KEY `', CONSTRAINT_NAME, '`;') AS drop_command,
    CONCAT('ALTER TABLE `', TABLE_NAME, '` ADD CONSTRAINT `', CONSTRAINT_NAME, '` FOREIGN KEY (`', COLUMN_NAME, '`) REFERENCES signup_users(id) ON DELETE CASCADE;') AS add_command,
    TABLE_NAME,
    CONSTRAINT_NAME,
    COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
    AND REFERENCED_TABLE_NAME = 'users'
    AND REFERENCED_COLUMN_NAME = 'id';

-- Fix ai_agents table (most common issue)
ALTER TABLE ai_agents DROP FOREIGN KEY ai_agents_ibfk_1;
ALTER TABLE ai_agents 
ADD CONSTRAINT fk_ai_agents_user 
FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE;

-- Fix any other tables that reference the old 'users' table
-- (Run the SELECT query above first to identify them, then add ALTER statements here)

-- Verify all tables now reference signup_users correctly
SELECT 
    TABLE_NAME,
    CONSTRAINT_NAME,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
    AND COLUMN_NAME = 'user_id'
    AND REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY TABLE_NAME;
