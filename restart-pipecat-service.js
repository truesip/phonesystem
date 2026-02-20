require('dotenv').config();
const axios = require('axios');

const PIPECAT_API_KEY = process.env.PIPECAT_PRIVATE_API_KEY || process.env.PIPECAT_API_KEY;
const PIPECAT_API_URL = process.env.PIPECAT_API_URL || 'https://api.pipecat.daily.co/v1';
const SERVICE_NAME = 'u9-jimmyassistant-badc60';

async function restartService() {
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

  console.log(`\n🔄 Restarting Pipecat service: ${SERVICE_NAME}\n`);
  console.log('This will terminate all zombie sessions and clear capacity.\n');

  try {
    // Delete and recreate the service (nuclear option)
    console.log('⚠️  WARNING: This will temporarily interrupt service');
    console.log('Press Ctrl+C now if you want to cancel...\n');
    
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    console.log('Step 1: Triggering service restart via scale-down...');
    
    // Scale to zero
    const scaleDown = await client.post(`/agents/${encodeURIComponent(SERVICE_NAME)}`, {
      autoScaling: {
        minReplicas: 0,
        maxReplicas: 0
      }
    });
    
    if (scaleDown.status >= 200 && scaleDown.status < 300) {
      console.log('✓ Scaled down to 0 replicas');
    } else {
      console.log(`⚠️  Scale down response: ${scaleDown.status}`);
      console.log(scaleDown.data);
    }
    
    console.log('Waiting 10 seconds for pods to terminate...');
    await new Promise(resolve => setTimeout(resolve, 10000));
    
    // Scale back up
    console.log('Step 2: Scaling service back up...');
    const scaleUp = await client.post(`/agents/${encodeURIComponent(SERVICE_NAME)}`, {
      autoScaling: {
        minReplicas: 0,
        maxReplicas: 50
      }
    });
    
    if (scaleUp.status >= 200 && scaleUp.status < 300) {
      console.log('✓ Scaled back up to max 50 replicas');
    } else {
      console.log(`⚠️  Scale up response: ${scaleUp.status}`);
      console.log(scaleUp.data);
    }
    
    console.log('\n✅ Service restart complete!');
    console.log('All zombie sessions should now be cleared.');
    console.log('\nCheck status in 30 seconds with: node check-pipecat-service.js');
    
  } catch (e) {
    console.error('❌ Error:', e.message);
    if (e.response) {
      console.error('Response:', e.response.data);
    }
  }
}

restartService().catch(console.error);
