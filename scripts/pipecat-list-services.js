const path = require('path');
const axios = require('axios');

const envPath = process.argv[2] ? String(process.argv[2]) : '';
if (!envPath) {
  console.error('Usage: node pipecat-list-services.js C:\\path\\to\\.env');
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

  const resp = await client.get('/agents');
  if (resp.status < 200 || resp.status >= 300) {
    console.error('Failed:', resp.status);
    process.exit(1);
  }

  const services = Array.isArray(resp.data)
    ? resp.data
    : (Array.isArray(resp.data?.services) ? resp.data.services : []);

  const names = services.map(s => s?.name).filter(Boolean);
  names.sort();

  console.log('Env file:', path.basename(envPath));
  console.log('Pipecat API URL:', PIPECAT_API_URL);
  console.log('Services found:', names.length);
  names.forEach(n => console.log(' - ' + n));
})();
