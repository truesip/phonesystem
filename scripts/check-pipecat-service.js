require('dotenv').config();
const axios = require('axios');

const PIPECAT_API_KEY = process.env.PIPECAT_PRIVATE_API_KEY || process.env.PIPECAT_API_KEY;
const PIPECAT_API_URL = process.env.PIPECAT_API_URL || 'https://api.pipecat.daily.co/v1';
const SERVICE_NAME = 'u9-jimmyassistant-badc60';

async function checkService() {
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

  console.log('\n=== Checking Pipecat Service Configuration ===\n');
  console.log(`Service: ${SERVICE_NAME}\n`);

  try {
    // Get service details
    const resp = await client.get(`/agents/${encodeURIComponent(SERVICE_NAME)}`);
    
    if (resp.status === 200) {
      console.log('Service Configuration:');
      console.log(JSON.stringify(resp.data, null, 2));
      
      if (resp.data.autoScaling) {
        console.log('\n📊 Auto-scaling:');
        console.log(`  minAgents: ${resp.data.autoScaling.minAgents}`);
        console.log(`  maxAgents: ${resp.data.autoScaling.maxAgents}`);
      }
      
      if (resp.data.agentProfile) {
        console.log(`\n🔧 Agent Profile: ${resp.data.agentProfile}`);
      }
    } else {
      console.error(`❌ Failed to get service: ${resp.status}`);
      console.error(resp.data);
    }
  } catch (e) {
    console.error('Error:', e.message);
  }
}

checkService().catch(console.error);
