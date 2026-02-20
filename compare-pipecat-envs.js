const crypto = require('crypto');
const path = require('path');

function sha8(s) {
  return crypto.createHash('sha256').update(String(s || '')).digest('hex').slice(0, 8);
}

function loadEnv(envPath) {
  const dotenv = require('dotenv');
  const res = dotenv.config({ path: envPath });
  const parsed = res.parsed || {};
  return {
    envPath,
    PIPECAT_API_URL: parsed.PIPECAT_API_URL || 'https://api.pipecat.daily.co/v1',
    PIPECAT_ORG_ID: parsed.PIPECAT_ORG_ID || '',
    PIPECAT_PRIVATE_API_KEY: parsed.PIPECAT_PRIVATE_API_KEY || parsed.PIPECAT_API_KEY || '',
    PIPECAT_PUBLIC_API_KEY: parsed.PIPECAT_PUBLIC_API_KEY || parsed.PIPECAT_PUBLIC_KEY || '',
  };
}

function mask(s) {
  const v = String(s || '');
  if (!v) return '(empty)';
  return `sha256:${sha8(v)} len:${v.length}`;
}

const aPath = process.argv[2];
const bPath = process.argv[3];
if (!aPath || !bPath) {
  console.error('Usage: node compare-pipecat-envs.js C:\\path\\to\\a.env C:\\path\\to\\b.env');
  process.exit(2);
}

const a = loadEnv(aPath);
const b = loadEnv(bPath);

console.log('A:', path.basename(a.envPath));
console.log('  PIPECAT_API_URL:', a.PIPECAT_API_URL);
console.log('  PIPECAT_ORG_ID:', mask(a.PIPECAT_ORG_ID));
console.log('  PIPECAT_PRIVATE_API_KEY:', mask(a.PIPECAT_PRIVATE_API_KEY));
console.log('  PIPECAT_PUBLIC_API_KEY:', mask(a.PIPECAT_PUBLIC_API_KEY));

console.log('B:', path.basename(b.envPath));
console.log('  PIPECAT_API_URL:', b.PIPECAT_API_URL);
console.log('  PIPECAT_ORG_ID:', mask(b.PIPECAT_ORG_ID));
console.log('  PIPECAT_PRIVATE_API_KEY:', mask(b.PIPECAT_PRIVATE_API_KEY));
console.log('  PIPECAT_PUBLIC_API_KEY:', mask(b.PIPECAT_PUBLIC_API_KEY));

console.log('\nMatch:');
console.log('  PIPECAT_API_URL:', a.PIPECAT_API_URL === b.PIPECAT_API_URL);
console.log('  PIPECAT_ORG_ID:', a.PIPECAT_ORG_ID === b.PIPECAT_ORG_ID);
console.log('  PIPECAT_PRIVATE_API_KEY:', a.PIPECAT_PRIVATE_API_KEY === b.PIPECAT_PRIVATE_API_KEY);
console.log('  PIPECAT_PUBLIC_API_KEY:', a.PIPECAT_PUBLIC_API_KEY === b.PIPECAT_PUBLIC_API_KEY);
