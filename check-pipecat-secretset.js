require('dotenv').config();
const axios = require('axios');

const PIPECAT_API_KEY = process.env.PIPECAT_PRIVATE_API_KEY || process.env.PIPECAT_API_KEY;
const PIPECAT_API_URL = process.env.PIPECAT_API_URL || 'https://api.pipecat.daily.co/v1';

const SECRET_SET = process.argv[2] || 'u9-jimmyassistant-badc60-secrets';

(async () => {
  if (!PIPECAT_API_KEY) throw new Error('PIPECAT_PRIVATE_API_KEY not set');

  const client = axios.create({
    baseURL: PIPECAT_API_URL,
    timeout: 30000,
    headers: {
      Authorization: `Bearer ${PIPECAT_API_KEY}`,
      Accept: 'application/json'
    },
    validateStatus: () => true
  });

  const resp = await client.get(`/secrets/${encodeURIComponent(SECRET_SET)}`);
  if (resp.status < 200 || resp.status >= 300) {
    console.error('Failed:', resp.status);
    // Don't dump resp.data because it may contain secret values.
    process.exit(1);
  }

  const topKeys = (resp.data && typeof resp.data === 'object') ? Object.keys(resp.data) : [];

  const secrets = resp.data?.secrets
    || resp.data?.data?.secrets
    || resp.data?.items
    || resp.data?.data
    || [];

  const keys = Array.isArray(secrets)
    ? secrets.map(s => s?.secretKey || s?.key || s?.name || s?.fieldName).filter(Boolean)
    : [];

  console.log(`Secret set: ${SECRET_SET}`);
  console.log('Response top-level keys:', topKeys.join(', ') || '(none)');
  console.log('Secrets container type:', Array.isArray(secrets) ? `array(len=${secrets.length})` : (typeof secrets));

  if (Array.isArray(secrets) && secrets.length) {
    const sample = secrets[0];
    const sampleKeys = (sample && typeof sample === 'object') ? Object.keys(sample) : [];
    console.log('First secret item keys:', sampleKeys.join(', ') || '(none)');
  }

  console.log(`Secret keys (${keys.length}):`);
  keys.sort().forEach(k => console.log(' - ' + k));
})();
