const axios = require('axios');
const path = require('path');

const envPath = process.argv[2] ? String(process.argv[2]) : '';
const serviceName = process.argv[3] ? String(process.argv[3]) : '';

if (!envPath || !serviceName) {
  console.error('Usage: node pipecat-get-service.js C:\\path\\to\\.env SERVICE_NAME');
  process.exit(2);
}

require('dotenv').config({ path: envPath });

const PIPECAT_API_KEY = process.env.PIPECAT_PRIVATE_API_KEY || process.env.PIPECAT_API_KEY;
const PIPECAT_API_URL = process.env.PIPECAT_API_URL || 'https://api.pipecat.daily.co/v1';

(async () => {
  if (!PIPECAT_API_KEY) throw new Error('PIPECAT_PRIVATE_API_KEY not set in env');

  const client = axios.create({
    baseURL: PIPECAT_API_URL,
    timeout: 30000,
    headers: {
      Authorization: `Bearer ${PIPECAT_API_KEY}`,
      Accept: 'application/json'
    },
    validateStatus: () => true
  });

  const resp = await client.get(`/agents/${encodeURIComponent(serviceName)}`);
  if (resp.status < 200 || resp.status >= 300) {
    console.error('Failed:', resp.status);
    process.exit(1);
  }

  const d = resp.data || {};
  const as = d.autoScaling || {};
  const manifest = d.deployment?.manifest;
  const spec = manifest?.spec || {};

  const out = {
    env: path.basename(envPath),
    apiUrl: PIPECAT_API_URL,
    name: d.name,
    region: d.region,
    agentProfile: d.agentProfile,
    activeSessionCount: d.activeSessionCount,
    autoScaling: {
      concurrency: as.concurrency,
      minReplicas: as.minReplicas,
      maxReplicas: as.maxReplicas,
      initialScale: as.initialScale,
      overProvisionFactor: as.overProvisionFactor,
      scaleToZeroTime: as.scaleToZeroTime
    },
    deployment: {
      image: spec.image,
      resources: spec.resources || null,
      nodeType: spec.dailyNodeType || null,
      deploymentMode: spec.deploymentMode || null
    }
  };

  console.log(JSON.stringify(out, null, 2));
})();
