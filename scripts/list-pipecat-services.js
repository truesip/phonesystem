require('dotenv').config();
const axios = require('axios');

const PIPECAT_API_KEY = process.env.PIPECAT_PRIVATE_API_KEY || process.env.PIPECAT_API_KEY;
const PIPECAT_API_URL = process.env.PIPECAT_API_URL || 'https://api.pipecat.daily.co/v1';

async function listServices() {
  const client = axios.create({
    baseURL: PIPECAT_API_URL,
    timeout: 30000,
    headers: {
      'Authorization': `Bearer ${PIPECAT_API_KEY}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    validateStatus: () => true
  });

  console.log('\n=== All Pipecat Services in Org ===\n');

  try {
    const resp = await client.get('/agents');

    const list = Array.isArray(resp.data)
      ? resp.data
      : (Array.isArray(resp.data?.services) ? resp.data.services : []);

    if (resp.status === 200 && list.length) {
      console.log(`Found ${list.length} services:\n`);

      const services = [];
      for (const s of list) {
        const name = s && s.name ? String(s.name).trim() : '';
        if (!name) continue;

        let d = null;
        try {
          const dresp = await client.get(`/agents/${encodeURIComponent(name)}`);
          if (dresp.status === 200) d = dresp.data || null;
        } catch {}

        const as = (d && typeof d === 'object' && d.autoScaling && typeof d.autoScaling === 'object')
          ? d.autoScaling
          : ((s && typeof s === 'object' && s.autoScaling && typeof s.autoScaling === 'object') ? s.autoScaling : {});

        const createdRaw = (d && d.createdAt) || s.createdAt;
        let created = 'N/A';
        try {
          if (createdRaw) created = new Date(createdRaw).toISOString().split('T')[0];
        } catch {}

        services.push({
          name,
          region: (d && d.region) || s.region || 'N/A',
          profile: (d && d.agentProfile) || s.agentProfile || 'N/A',
          activeSessions: Number((d && d.activeSessionCount) || s.activeSessionCount || 0) || 0,
          concurrency: (as && as.concurrency != null) ? as.concurrency : 'N/A',
          maxReplicas: (as && (as.maxReplicas != null || as.maxAgents != null)) ? (as.maxReplicas ?? as.maxAgents) : 'N/A',
          created
        });
      }

      services.sort((a, b) => String(a.name).localeCompare(String(b.name)));
      console.table(services);

      const totalSessions = services.reduce((sum, s) => sum + (Number(s.activeSessions) || 0), 0);
      console.log(`\n📊 Total active sessions across all services: ${totalSessions}`);

      const phonesystemSvc = services.find(s => s.name === 'u9-jimmyassistant-badc60');
      if (phonesystemSvc) {
        console.log(`\n🔍 Phone.System service (u9-jimmyassistant-badc60): ${phonesystemSvc.activeSessions} sessions`);
      }

    } else {
      console.error(`❌ Failed: ${resp.status}`);
      console.error(resp.data);
    }
  } catch (e) {
    console.error('Error:', e.message);
  }
}

listServices().catch(console.error);
