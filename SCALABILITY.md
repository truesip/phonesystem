# Platform Scalability Optimization for 1 Million Users

## Current Bottlenecks Identified

1. **Database Connection Pool**: Currently set to 50 connections - insufficient for high concurrency
2. **No caching layer**: Balance queries hit database every time
3. **No read replicas**: All queries go to primary database
4. **Synchronous operations**: Many operations block the event loop
5. **No connection pooling optimization**: Missing query timeouts and retry logic
6. **Session store**: Direct MySQL sessions without Redis
7. **No rate limiting tiers**: Single rate limit for all users
8. **Missing database partitioning**: Large tables will slow down
9. **No CDN**: Static assets served from app server

## Optimization Strategy

### Phase 1: Database Optimization (Critical - Week 1)

#### A. Increase Connection Pool
```javascript
// Current: connectionLimit: 50
// Optimized for 1M users: 200-500 per instance
connectionLimit: 200,
connectTimeout: 10000,
acquireTimeout: 10000,
timeout: 60000,
```

#### B. Add Read Replicas
- Set up MySQL read replicas (2-3 replicas)
- Route read queries to replicas
- Keep writes on primary

#### C. Database Indexes (Already implemented but verify)
- `idx_user_status` on billing_history
- `idx_user_created` on billing_history
- `idx_created_at` on signup_users

#### D. Add Composite Indexes for Common Queries
```sql
-- For AI call logs
ALTER TABLE ai_call_logs ADD INDEX idx_user_time (user_id, time_start);
ALTER TABLE ai_call_logs ADD INDEX idx_user_billed (user_id, billed);

-- For CDRs
ALTER TABLE cdrs ADD INDEX idx_user_time (user_id, time_start);

-- For sessions
ALTER TABLE sessions ADD INDEX idx_expires (expires);
```

### Phase 2: Caching Layer (Critical - Week 1-2)

#### A. Redis Integration
```javascript
// Add Redis for:
// 1. Session storage
// 2. Balance caching
// 3. Rate limiting
// 4. User profile caching
```

#### B. Balance Caching Strategy
```javascript
// Cache user balance for 5 minutes
// Invalidate on balance updates
// Key: `balance:${userId}`
```

#### C. Session Storage
```javascript
// Move from MySQL to Redis
// Reduces database load significantly
```

### Phase 3: Application Optimization (Week 2)

#### A. Connection Pool per Service
```javascript
// Separate pools for:
// - User operations (150 connections)
// - Billing operations (100 connections)
// - CDR/Call logs (100 connections)
// - Background jobs (50 connections)
```

#### B. Async Operations
```javascript
// Convert synchronous operations to async:
// - Email sending (use queue)
// - PDF generation (use worker)
// - Webhook calls (use queue)
```

#### C. Query Optimization
```javascript
// 1. Add LIMIT to all queries
// 2. Use SELECT specific columns instead of *
// 3. Batch operations where possible
// 4. Implement pagination everywhere
```

### Phase 4: Infrastructure (Week 2-3)

#### A. Load Balancing
```
Client → Nginx/CloudFlare → Load Balancer → App Servers (3-5 instances)
                                           → Redis Cluster
                                           → MySQL Primary + Replicas
```

#### B. Horizontal Scaling
```
- 3-5 app server instances initially
- Auto-scaling based on CPU/Memory (scale up to 20 instances)
- Each instance: 2-4 vCPUs, 4-8GB RAM
```

#### C. CDN Setup
```
- CloudFlare or AWS CloudFront
- Cache all static assets
- Cache dashboard pages (with auth)
```

### Phase 5: Database Architecture (Week 3-4)

#### A. Table Partitioning
```sql
-- Partition billing_history by month
ALTER TABLE billing_history 
PARTITION BY RANGE (YEAR(created_at) * 100 + MONTH(created_at)) (
    PARTITION p202601 VALUES LESS THAN (202602),
    PARTITION p202602 VALUES LESS THAN (202603),
    -- ... add partitions for next 12 months
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Partition ai_call_logs by month
-- Partition cdrs by month
```

#### B. Archive Old Data
```javascript
// Move data older than 12 months to archive tables
// Keep primary tables fast
```

### Phase 6: Monitoring & Observability (Ongoing)

#### A. Application Performance Monitoring
```
- New Relic / DataDog / Sentry
- Track slow queries
- Monitor memory leaks
- Alert on error rates
```

#### B. Database Monitoring
```
- Monitor connection pool usage
- Track slow queries
- Monitor replication lag
- Alert on connection exhaustion
```

#### C. Custom Metrics
```javascript
// Track:
// - Active users per minute
// - API response times
// - Balance query times
// - Failed logins
// - Rate limit hits
```

## Implementation Priority (Critical First)

### Week 1: Emergency Performance Fixes
1. ✅ Increase connection pool to 200
2. ✅ Add Redis for sessions and caching
3. ✅ Optimize balance queries (add caching)
4. ✅ Add missing indexes
5. ✅ Implement query timeouts

### Week 2: Scaling Foundation
1. Set up read replicas
2. Implement load balancer
3. Add 2 more app server instances
4. Set up CDN

### Week 3: Long-term Optimization
1. Implement table partitioning
2. Add monitoring
3. Optimize background jobs
4. Archive old data

### Week 4: Polish & Testing
1. Load testing (simulate 1M users)
2. Performance tuning
3. Documentation
4. Disaster recovery planning

## Expected Performance Metrics

### Current (optimized):
- 100-500 concurrent users
- ~50-100ms average response time
- 50 database connections

### Target (1M users):
- 50,000-100,000 concurrent users
- <100ms average response time (p95)
- 200-500 database connections per instance
- 99.9% uptime
- <1% error rate

## Cost Implications

### Infrastructure Costs (Monthly Estimates)
- Database: $200-500 (Primary + 2 replicas)
- Redis: $50-100 (Managed service)
- App Servers: $300-600 (3-5 instances)
- CDN: $50-100
- Load Balancer: $50-100
- Monitoring: $100-200
- **Total: ~$750-1600/month**

### Scaling Beyond 1M Users
- Add more app server instances (auto-scale)
- Increase database size/replicas
- Consider database sharding by user ID
- Implement microservices for specific features

## Immediate Actions (Today)

1. Increase connection pool
2. Add query timeouts
3. Implement balance caching
4. Add health check endpoint
5. Set up error monitoring
