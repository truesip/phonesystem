-- Phone.System Database Schema
-- Self-contained user management, CDR storage, and billing tracking

-- Users table: replaces MagnusBilling user management
CREATE TABLE IF NOT EXISTS `users` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `username` VARCHAR(100) NOT NULL UNIQUE,
  `email` VARCHAR(255) NOT NULL UNIQUE,
  `password_hash` VARCHAR(255) NOT NULL,
  `name` VARCHAR(255) DEFAULT NULL,
  `phone` VARCHAR(50) DEFAULT NULL,
  `balance` DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
  `call_limit` INT UNSIGNED NOT NULL DEFAULT 10,
  `cps_limit` INT UNSIGNED NOT NULL DEFAULT 10,
  `address1` VARCHAR(255) DEFAULT NULL,
  `city` VARCHAR(100) DEFAULT NULL,
  `state` VARCHAR(100) DEFAULT NULL,
  `postal_code` VARCHAR(20) DEFAULT NULL,
  `country` VARCHAR(2) DEFAULT NULL,
  `signup_ip` VARCHAR(45) DEFAULT NULL,
  `sip_domain` VARCHAR(255) DEFAULT NULL,
  `is_active` TINYINT(1) NOT NULL DEFAULT 1,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_username` (`username`),
  INDEX `idx_email` (`email`),
  INDEX `idx_is_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- CDRs table: stores AI agent call detail records
CREATE TABLE IF NOT EXISTS `cdrs` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `direction` ENUM('inbound', 'outbound') NOT NULL,
  `src_number` VARCHAR(50) DEFAULT NULL,
  `dst_number` VARCHAR(50) DEFAULT NULL,
  `did_number` VARCHAR(50) DEFAULT NULL,
  `time_start` DATETIME NOT NULL,
  `time_end` DATETIME DEFAULT NULL,
  `duration` INT UNSIGNED DEFAULT 0,
  `billsec` INT UNSIGNED DEFAULT 0,
  `price` DECIMAL(10,4) DEFAULT 0.0000,
  `status` VARCHAR(100) DEFAULT 'completed',
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `campaign_id` INT UNSIGNED DEFAULT NULL,
  `session_id` VARCHAR(255) DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_direction` (`direction`),
  INDEX `idx_time_start` (`time_start`),
  INDEX `idx_ai_agent_id` (`ai_agent_id`),
  INDEX `idx_campaign_id` (`campaign_id`),
  INDEX `idx_session_id` (`session_id`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Billing history table: tracks all balance transactions
CREATE TABLE IF NOT EXISTS `billing_history` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `amount` DECIMAL(10,4) NOT NULL,
  `description` VARCHAR(500) NOT NULL,
  `transaction_type` ENUM('credit', 'debit', 'payment', 'refund', 'adjustment') NOT NULL,
  `payment_method` VARCHAR(50) DEFAULT NULL COMMENT 'card, crypto, ach, etc',
  `reference_id` VARCHAR(255) DEFAULT NULL COMMENT 'Stripe charge ID, crypto tx hash, etc',
  `status` ENUM('pending', 'completed', 'failed') NOT NULL DEFAULT 'completed',
  `balance_before` DECIMAL(10,4) NOT NULL,
  `balance_after` DECIMAL(10,4) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_transaction_type` (`transaction_type`),
  INDEX `idx_status` (`status`),
  INDEX `idx_created_at` (`created_at`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Agents table (already exists, ensure it has user_id)
CREATE TABLE IF NOT EXISTS `ai_agents` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `voice` VARCHAR(100) DEFAULT 'alloy',
  `greeting` TEXT DEFAULT NULL,
  `system_prompt` TEXT DEFAULT NULL,
  `phone_number` VARCHAR(50) DEFAULT NULL,
  `is_active` TINYINT(1) NOT NULL DEFAULT 1,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_phone_number` (`phone_number`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Numbers table (phone numbers assigned to AI agents)
CREATE TABLE IF NOT EXISTS `ai_numbers` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `phone_number` VARCHAR(50) NOT NULL UNIQUE,
  `number_type` ENUM('local', 'tollfree') NOT NULL DEFAULT 'local',
  `monthly_fee` DECIMAL(10,4) NOT NULL DEFAULT 10.2000,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `provider` VARCHAR(50) NOT NULL DEFAULT 'daily',
  `provider_config` JSON DEFAULT NULL,
  `is_active` TINYINT(1) NOT NULL DEFAULT 1,
  `last_billed_at` TIMESTAMP NULL DEFAULT NULL,
  `next_billing_date` DATE DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_phone_number` (`phone_number`),
  INDEX `idx_ai_agent_id` (`ai_agent_id`),
  INDEX `idx_next_billing_date` (`next_billing_date`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`ai_agent_id`) REFERENCES `ai_agents`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Conversations table (stores conversation history)
CREATE TABLE IF NOT EXISTS `ai_conversations` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `session_id` VARCHAR(255) NOT NULL,
  `caller_number` VARCHAR(50) DEFAULT NULL,
  `direction` ENUM('inbound', 'outbound') NOT NULL DEFAULT 'inbound',
  `started_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ended_at` TIMESTAMP NULL DEFAULT NULL,
  `duration_seconds` INT UNSIGNED DEFAULT 0,
  `message_count` INT UNSIGNED DEFAULT 0,
  `status` VARCHAR(50) DEFAULT 'active',
  `cdr_id` BIGINT UNSIGNED DEFAULT NULL,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_ai_agent_id` (`ai_agent_id`),
  INDEX `idx_session_id` (`session_id`),
  INDEX `idx_caller_number` (`caller_number`),
  INDEX `idx_started_at` (`started_at`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`ai_agent_id`) REFERENCES `ai_agents`(`id`) ON DELETE SET NULL,
  FOREIGN KEY (`cdr_id`) REFERENCES `cdrs`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Messages table (stores individual messages in conversations)
CREATE TABLE IF NOT EXISTS `ai_messages` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `conversation_id` BIGINT UNSIGNED NOT NULL,
  `message_id` VARCHAR(64) DEFAULT NULL,
  `role` ENUM('user', 'assistant', 'system') NOT NULL,
  `content` TEXT NOT NULL,
  `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_conversation_id` (`conversation_id`),
  INDEX `idx_message_id` (`message_id`),
  INDEX `idx_timestamp` (`timestamp`),
  FOREIGN KEY (`conversation_id`) REFERENCES `ai_conversations`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dialer Campaigns table
CREATE TABLE IF NOT EXISTS `dialer_campaigns` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `concurrency` INT UNSIGNED NOT NULL DEFAULT 1,
  `status` ENUM('draft', 'running', 'paused', 'completed', 'deleted') NOT NULL DEFAULT 'draft',
  `has_campaign_audio` TINYINT(1) NOT NULL DEFAULT 0,
  `audio_file_path` VARCHAR(500) DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_status` (`status`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`ai_agent_id`) REFERENCES `ai_agents`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dialer Leads table
CREATE TABLE IF NOT EXISTS `dialer_leads` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `campaign_id` INT UNSIGNED NOT NULL,
  `phone_number` VARCHAR(50) NOT NULL,
  `first_name` VARCHAR(255) DEFAULT NULL,
  `last_name` VARCHAR(255) DEFAULT NULL,
  `email` VARCHAR(255) DEFAULT NULL,
  `custom_data` JSON DEFAULT NULL,
  `status` ENUM('pending', 'queued', 'dialing', 'answered', 'voicemail', 'transferred', 'failed', 'completed') NOT NULL DEFAULT 'pending',
  `attempts` INT UNSIGNED NOT NULL DEFAULT 0,
  `last_attempt_at` TIMESTAMP NULL DEFAULT NULL,
  `completed_at` TIMESTAMP NULL DEFAULT NULL,
  `cdr_id` BIGINT UNSIGNED DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_campaign_id` (`campaign_id`),
  INDEX `idx_phone_number` (`phone_number`),
  INDEX `idx_status` (`status`),
  FOREIGN KEY (`campaign_id`) REFERENCES `dialer_campaigns`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`cdr_id`) REFERENCES `cdrs`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Tool Settings table (stores SMTP, SMS, transfer, etc. configurations per user)
CREATE TABLE IF NOT EXISTS `ai_tool_settings` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `tool_name` VARCHAR(50) NOT NULL COMMENT 'smtp, sms, transfer, meeting, mail, square, stripe, etc',
  `config` JSON NOT NULL,
  `is_enabled` TINYINT(1) NOT NULL DEFAULT 1,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `unique_user_tool` (`user_id`, `tool_name`),
  INDEX `idx_tool_name` (`tool_name`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Email Queue table (stores emails sent by AI agents)
CREATE TABLE IF NOT EXISTS `ai_emails` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `conversation_id` BIGINT UNSIGNED DEFAULT NULL,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `to_email` VARCHAR(255) NOT NULL,
  `subject` VARCHAR(500) DEFAULT NULL,
  `body` TEXT DEFAULT NULL,
  `status` VARCHAR(50) DEFAULT 'pending',
  `error_message` TEXT DEFAULT NULL,
  `sent_at` TIMESTAMP NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_conversation_id` (`conversation_id`),
  INDEX `idx_status` (`status`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`conversation_id`) REFERENCES `ai_conversations`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI SMS Queue table
CREATE TABLE IF NOT EXISTS `ai_sms` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `conversation_id` BIGINT UNSIGNED DEFAULT NULL,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `to_number` VARCHAR(50) NOT NULL,
  `message` TEXT NOT NULL,
  `status` VARCHAR(50) DEFAULT 'pending',
  `dlr_status` VARCHAR(50) DEFAULT NULL,
  `message_id` VARCHAR(255) DEFAULT NULL,
  `error_message` TEXT DEFAULT NULL,
  `sent_at` TIMESTAMP NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_conversation_id` (`conversation_id`),
  INDEX `idx_status` (`status`),
  INDEX `idx_message_id` (`message_id`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`conversation_id`) REFERENCES `ai_conversations`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Meeting Links table
CREATE TABLE IF NOT EXISTS `ai_meetings` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `conversation_id` BIGINT UNSIGNED DEFAULT NULL,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `meeting_url` VARCHAR(500) NOT NULL,
  `scheduled_time` DATETIME DEFAULT NULL,
  `status` VARCHAR(50) DEFAULT 'sent',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_conversation_id` (`conversation_id`),
  INDEX `idx_status` (`status`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`conversation_id`) REFERENCES `ai_conversations`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Physical Mail table
CREATE TABLE IF NOT EXISTS `ai_mail` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `conversation_id` BIGINT UNSIGNED DEFAULT NULL,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `recipient_name` VARCHAR(255) NOT NULL,
  `recipient_address` TEXT NOT NULL,
  `document_path` VARCHAR(500) DEFAULT NULL,
  `status` VARCHAR(50) DEFAULT 'pending',
  `tracking_number` VARCHAR(255) DEFAULT NULL,
  `error_message` TEXT DEFAULT NULL,
  `sent_at` TIMESTAMP NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_conversation_id` (`conversation_id`),
  INDEX `idx_status` (`status`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`conversation_id`) REFERENCES `ai_conversations`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- AI Payment Links table (Square/Stripe)
CREATE TABLE IF NOT EXISTS `ai_payments` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT UNSIGNED NOT NULL,
  `conversation_id` BIGINT UNSIGNED DEFAULT NULL,
  `ai_agent_id` INT UNSIGNED DEFAULT NULL,
  `provider` ENUM('square', 'stripe') NOT NULL,
  `payment_link` VARCHAR(500) NOT NULL,
  `amount` DECIMAL(10,2) NOT NULL,
  `currency` VARCHAR(3) DEFAULT 'USD',
  `description` VARCHAR(500) DEFAULT NULL,
  `status` VARCHAR(50) DEFAULT 'pending',
  `external_id` VARCHAR(255) DEFAULT NULL,
  `paid_at` TIMESTAMP NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user_id` (`user_id`),
  INDEX `idx_conversation_id` (`conversation_id`),
  INDEX `idx_status` (`status`),
  INDEX `idx_external_id` (`external_id`),
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`conversation_id`) REFERENCES `ai_conversations`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
