const AriClient = require('ari-client');
const axios = require('axios');
const crypto = require('crypto');

// Configuration from environment variables
const ARI_HOST = String(process.env.ARI_HOST || '').trim();
const ARI_PORT = Math.max(1, parseInt(process.env.ARI_PORT || '8088', 10) || 8088);
const ARI_USER = String(process.env.ARI_USER || '').trim();
const ARI_PASSWORD = String(process.env.ARI_PASSWORD || '').trim();
const ARI_APP = String(process.env.ARI_APP || 'dialer').trim() || 'dialer';
const ARI_DIAL_PREFIX = String(process.env.ARI_DIAL_PREFIX || process.env.PJSIP_PREFIX || 'PJSIP/').trim() || 'PJSIP/';
const ARI_CONNECT_RETRY_MS = Math.max(1000, parseInt(process.env.ARI_CONNECT_RETRY_MS || '3000', 10) || 3000);
const ARI_PLAYBACK_HANGUP_DELAY_MS = Math.max(0, parseInt(process.env.ARI_PLAYBACK_HANGUP_DELAY_MS || '0', 10) || 0);
const ARI_NUMBER_SUFFIX = (() => {
  const raw = process.env.ARI_NUMBER_SUFFIX;
  if (raw !== undefined) {
    const trimmed = String(raw).trim();
    return trimmed || '@switch';
  }
  return '@switch';
})();
const ARI_ORIGINATE_TIMEOUT_MS = Math.max(
  3000,
  parseInt(process.env.ARI_ORIGINATE_TIMEOUT_MS || '15000', 10) || 15000
);

const DEBUG = process.env.DEBUG === '1' || process.env.LOG_LEVEL === 'debug';

class AriService {
  constructor() {
    this.client = null;
    this.pool = null;
    this.isConnected = false;
    this.callCache = new Map(); // Map<channelId, CallState>
    this.reconnectTimer = null;
  }

  init(pool) {
    this.pool = pool;
    if (this.shouldConnect()) {
      this.connect().catch(err => {
        console.error('[ari-service] Initial connection failed:', err.message);
        this.scheduleReconnect();
      });
    } else {
      console.warn('[ari-service] ARI is not fully configured (ARI_HOST, ARI_USER, ARI_PASSWORD are required).');
    }
  }

  shouldConnect() {
    return !!(ARI_HOST && ARI_USER && ARI_PASSWORD);
  }

  async connect() {
    if (this.isConnected) return;

    try {
      const url = `http://${ARI_HOST}:${ARI_PORT}`;
      this.client = await new Promise((resolve, reject) => {
        AriClient.connect(url, ARI_USER, ARI_PASSWORD, (err, client) => {
          if (err) return reject(err);
          resolve(client);
        });
      });

      this.client.on('StasisStart', this.handleStasisStart.bind(this));
      this.client.on('StasisEnd', this.handleStasisEnd.bind(this));
      this.client.on('ChannelStateChange', this.handleChannelStateChange.bind(this));
      this.client.on('ChannelDestroyed', this.handleChannelDestroyed.bind(this));
      
      // Cleanup handlers on client error
      this.client.on('error', (err) => {
        console.error('[ari-service] Client error:', err);
        this.isConnected = false;
        this.scheduleReconnect();
      });

      await this.client.start(ARI_APP);
      this.isConnected = true;
      console.log(`[ari-service] Connected to ${ARI_HOST}:${ARI_PORT} app=${ARI_APP}`);

    } catch (err) {
      this.isConnected = false;
      throw err;
    }
  }

  scheduleReconnect() {
    if (this.reconnectTimer) return;
    console.log(`[ari-service] Scheduling reconnect in ${ARI_CONNECT_RETRY_MS}ms...`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch(err => {
        console.error('[ari-service] Reconnect failed:', err.message);
        this.scheduleReconnect();
      });
    }, ARI_CONNECT_RETRY_MS);
  }

  // Originate a call
  async originateCall({ toNumber, fromNumber, audioUrl, campaignId, leadId, userId, callId }) {
    if (!this.isConnected) throw new Error('ARI not connected');

    const suffix = ARI_NUMBER_SUFFIX || '';
    const dest = (suffix && !toNumber.endsWith(suffix)) ? `${toNumber}${suffix}` : toNumber;
    const endpoint = `${ARI_DIAL_PREFIX}${dest}`;
    
    const variables = { 
      audio_url: audioUrl,
      campaign_id: String(campaignId),
      lead_id: String(leadId),
      user_id: String(userId),
      call_id: String(callId)
    };

    console.log(`[ari-service] Originating call to ${endpoint} from ${fromNumber}`);

    // Set a timeout for origination
    let timeoutHandle;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutHandle = setTimeout(() => reject(new Error('ARI originate timeout')), ARI_ORIGINATE_TIMEOUT_MS);
    });

    try {
      const channel = await Promise.race([
        this.client.channels.originate({
          endpoint,
          callerId: fromNumber,
          app: ARI_APP,
          appArgs: 'dialer', // Simple argument to identify dialer calls
          variables
        }),
        timeoutPromise
      ]);

      clearTimeout(timeoutHandle);

      // Cache initial state
      if (channel && channel.id) {
        this.updateCallCache(channel.id, {
          dialerCallId: callId,
          campaignId,
          leadId,
          userId,
          status: 'dialing',
          startTime: new Date()
        });
      }

      return { channelId: channel.id, channelName: channel.name };
    } catch (err) {
      clearTimeout(timeoutHandle);
      throw err;
    }
  }

  // Helpers
  updateCallCache(channelId, updates) {
    const current = this.callCache.get(channelId) || {};
    this.callCache.set(channelId, { ...current, ...updates });
    return this.callCache.get(channelId);
  }

  getCallCache(channelId) {
    return this.callCache.get(channelId);
  }
  
  removeCallCache(channelId) {
    const cached = this.callCache.get(channelId);
    this.callCache.delete(channelId);
    return cached;
  }

  // --- Event Handlers ---

  async handleStasisStart(event, channel) {
    const channelId = channel.id;
    const args = event.args || [];
    const channelVars = channel.variables || {}; // Variables might not be populated in event.channel depending on asterisk version, but let's hope
    
    console.log(`[ari-service] StasisStart: ${channelId} args=${args}`);

    // If we originated this call, we should have variables in our cache or we can fetch them
    let callState = this.getCallCache(channelId);

    // If not in cache, try to link via LinkedID (common issue with Local channels)
    if (!callState) {
        // Try to fetch variable linkedid if not present
        try {
            const linkedIdVar = await this.getChannelVariable(channelId, 'CHANNEL(linkedid)');
            if (linkedIdVar) {
                const parentState = this.getCallCache(linkedIdVar);
                if (parentState) {
                    console.log(`[ari-service] Linked channel ${channelId} to parent ${linkedIdVar}`);
                    callState = this.updateCallCache(channelId, { ...parentState, parentChannelId: linkedIdVar });
                }
            }
        } catch (e) {
            console.warn(`[ari-service] Failed to link channel ${channelId}:`, e.message);
        }
    }

    // Answer the channel
    try {
      await this.client.channels.answer({ channelId });
      this.updateCallCache(channelId, { status: 'answered', answerTime: new Date() });
      await this.updateCallLogStatus(callState?.dialerCallId, 'answered');
    } catch (err) {
      console.error(`[ari-service] Failed to answer channel ${channelId}:`, err.message);
    }

    // Play audio if available
    // We check variables passed during originate
    // Since 'variables' in event.channel might be empty, we rely on our cache or try to fetch variable
    let audioUrl = callState?.audioUrl;
    if (!audioUrl) {
         try {
             audioUrl = await this.getChannelVariable(channelId, 'audio_url');
         } catch (e) {}
    }

    if (audioUrl) {
      console.log(`[ari-service] Playing audio ${audioUrl} on ${channelId}`);
      try {
        const playback = await this.client.channels.play({ channelId, media: audioUrl });
        playback.once('PlaybackFinished', () => {
            console.log(`[ari-service] Playback finished on ${channelId}`);
            setTimeout(() => {
                this.client.channels.hangup({ channelId }).catch(() => {});
            }, ARI_PLAYBACK_HANGUP_DELAY_MS);
        });
      } catch (err) {
        console.error(`[ari-service] Playback failed on ${channelId}:`, err.message);
        this.client.channels.hangup({ channelId }).catch(() => {});
      }
    } else {
        console.warn(`[ari-service] No audio_url found for ${channelId}`);
        // Just hangup if no audio? Or wait? For now, let's wait a bit then hangup to avoid dead air forever
        setTimeout(() => {
             this.client.channels.hangup({ channelId }).catch(() => {});
        }, 5000); 
    }
  }

  async handleStasisEnd(event, channel) {
    const channelId = channel.id;
    console.log(`[ari-service] StasisEnd: ${channelId}`);
    // We handle the final duration calculation in ChannelDestroyed usually, 
    // but StasisEnd is good for knowing the app is done.
  }

  async handleChannelStateChange(event, channel) {
    const channelId = channel.id;
    const state = channel.state;
    console.log(`[ari-service] ChannelStateChange: ${channelId} -> ${state}`);

    if (state === 'Up') {
      const callState = this.getCallCache(channelId);
      if (callState && !callState.answerTime) {
        const answerTime = new Date();
        this.updateCallCache(channelId, { status: 'answered', answerTime });
        await this.updateCallLogStatus(callState.dialerCallId, 'answered', answerTime);
      }
    } else if (state === 'Ringing') {
        const callState = this.getCallCache(channelId);
        if (callState && !callState.ringTime) {
            this.updateCallCache(channelId, { status: 'ringing', ringTime: new Date() });
            await this.updateCallLogStatus(callState.dialerCallId, 'ringing');
        }
    }
  }

  async handleChannelDestroyed(event, channel) {
    const channelId = channel.id;
    const cause = event.cause_txt || event.cause;
    console.log(`[ari-service] ChannelDestroyed: ${channelId} Cause: ${cause}`);

    const callState = this.removeCallCache(channelId);
    if (callState) {
        const endTime = new Date();
        const startTime = callState.answerTime || callState.startTime || endTime;
        const duration = Math.max(0, Math.round((endTime - startTime) / 1000));
        
        let status = 'completed';
        if (cause && (String(cause).includes('BUSY') || String(cause).includes('NO ANSWER') || String(cause).includes('CONGESTION'))) {
            status = 'failed';
        } else if (!callState.answerTime) {
            status = 'failed'; // Never answered
        }

        await this.finalizeCallLog(callState.dialerCallId, status, duration, endTime, cause);
    }
  }

  async getChannelVariable(channelId, variable) {
      if (!this.client) return null;
      try {
          const resp = await this.client.channels.getChannelVar({ channelId, variable });
          return resp.value;
      } catch (e) {
          return null;
      }
  }

  // Database updates
  async updateCallLogStatus(callId, status, timestamp = new Date()) {
      if (!this.pool || !callId) return;
      try {
          // Update dialer_call_logs
          await this.pool.execute(
              `UPDATE dialer_call_logs SET status = ? WHERE call_id = ? LIMIT 1`,
              [status, callId]
          );
          
          // Also update lead status if needed
          if (status === 'answered') {
               // Find lead_id from call_id
               const [rows] = await this.pool.execute('SELECT lead_id, user_id FROM dialer_call_logs WHERE call_id = ? LIMIT 1', [callId]);
               if (rows[0]) {
                   await this.pool.execute(
                       `UPDATE dialer_leads SET status = 'answered', last_call_at = ? WHERE id = ? AND user_id = ? LIMIT 1`,
                       [timestamp, rows[0].lead_id, rows[0].user_id]
                   );
               }
          }
      } catch (err) {
          console.error('[ari-service] Failed to update call status:', err.message);
      }
  }

  async finalizeCallLog(callId, status, duration, endTime, cause) {
      if (!this.pool || !callId) return;
      try {
          await this.pool.execute(
              `UPDATE dialer_call_logs 
               SET status = ?, duration = ?, billsec = ?, time_end = ?, result = ?
               WHERE call_id = ? LIMIT 1`,
              [status, duration, duration, endTime, cause, callId]
          );

          // Update lead status
          const [rows] = await this.pool.execute('SELECT lead_id, user_id FROM dialer_call_logs WHERE call_id = ? LIMIT 1', [callId]);
          if (rows[0]) {
              let leadStatus = 'completed';
              if (status === 'failed') leadStatus = 'failed'; // Or keep as is?
              
              await this.pool.execute(
                  `UPDATE dialer_leads SET status = ? WHERE id = ? AND user_id = ? LIMIT 1`,
                  [leadStatus, rows[0].lead_id, rows[0].user_id]
              );
          }
      } catch (err) {
          console.error('[ari-service] Failed to finalize call log:', err.message);
      }
  }

  async cleanupStuckCalls() {
    if (!this.pool) return;
    try {
      // 1. Mark stuck call logs as failed
      const [logResult] = await this.pool.execute(
        `UPDATE dialer_call_logs
           SET status = 'error',
               result = 'failed',
               notes = CASE
                 WHEN notes IS NULL OR notes = '' THEN 'Reset after restart (missing ARI channel)'
                 ELSE notes
               END
         WHERE status IN ('queued','dialing','ringing')
           AND created_at < (NOW() - INTERVAL 30 SECOND)`
      );

      // 2. Reset stuck leads to pending so they can be retried
      const [leadResult] = await this.pool.execute(
        `UPDATE dialer_leads
            SET status = 'pending',
                last_call_at = NULL
          WHERE status IN ('queued','dialing')`
      );

      if (DEBUG) {
        console.log('[ari-service] Reset stuck dialer state', {
          callLogsUpdated: logResult?.affectedRows || 0,
          leadsUpdated: leadResult?.affectedRows || 0
        });
      }
    } catch (err) {
      console.warn('[ari-service] Failed to reset stuck dialer state:', err?.message || err);
    }
  }
}

module.exports = new AriService();
