// Load .env.local first (for local development), fallback to .env
const fs = require('fs');
const path = require('path');
const os = require('os');
const { execFile } = require('child_process');
const util = require('util');
const execFileAsync = util.promisify(execFile);
const envLocalPath = path.join(__dirname, '.env.local');
if (fs.existsSync(envLocalPath)) {
  require('dotenv').config({ path: envLocalPath });
  console.log('Loaded environment from .env.local');
} else {
  require('dotenv').config();
}
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const Stripe = require('stripe');
const crypto = require('crypto');
const https = require('https');
const zlib = require('zlib');
const mysql = require('mysql2/promise');
const nodemailer = require('nodemailer');
const multer = require('multer');
const Papa = require('papaparse');
const PizZip = require('pizzip');
const Docxtemplater = require('docxtemplater');
const session = require('express-session');
const MySQLStore = require('express-mysql-session')(session);
const bcrypt = require('bcryptjs');
const rateLimit = require('express-rate-limit');
const { XMLParser } = require('fast-xml-parser');
const { telecomCenterApiCall } = require('./lib/telecomCenter');

// Optional dependency used to generate PDFs for AI physical mail without DOCX templates.
// Loaded lazily to avoid crashing older deployments that haven't installed it yet.
let PDFDocument = null;
try { PDFDocument = require('pdfkit'); } catch {}

const app = express();
const PORT = process.env.PORT || 8080;
const DEBUG = process.env.DEBUG === '1' || process.env.LOG_LEVEL === 'debug';
const SESSION_SECRET = process.env.SESSION_SECRET || 'dev-change-this';
const CHECKOUT_MIN_AMOUNT = parseFloat(process.env.CHECKOUT_MIN_AMOUNT || '100');
const CHECKOUT_MAX_AMOUNT = parseFloat(process.env.CHECKOUT_MAX_AMOUNT || '500');

// Inbound call billing (flat rates expressed per minute, billed per second)
// Defaults: Local $0.025/min equivalent, Toll-Free $0.03/min equivalent
const INBOUND_LOCAL_RATE_PER_MIN = parseFloat(process.env.INBOUND_LOCAL_RATE_PER_MIN || '0.025') || 0.025;
const INBOUND_TOLLFREE_RATE_PER_MIN = parseFloat(process.env.INBOUND_TOLLFREE_RATE_PER_MIN || '0.03') || 0.03;

// AI inbound call billing (Daily dial-in -> Pipecat). Defaults to inbound rates unless AI-specific overrides are provided.
const AI_INBOUND_LOCAL_RATE_PER_MIN = parseFloat(
  process.env.AI_INBOUND_LOCAL_RATE_PER_MIN || String(INBOUND_LOCAL_RATE_PER_MIN)
) || INBOUND_LOCAL_RATE_PER_MIN;
const AI_INBOUND_TOLLFREE_RATE_PER_MIN = parseFloat(
  process.env.AI_INBOUND_TOLLFREE_RATE_PER_MIN || String(INBOUND_TOLLFREE_RATE_PER_MIN)
) || INBOUND_TOLLFREE_RATE_PER_MIN;

// If enabled, AI inbound calls are billed in whole-minute increments (rounded up).
// Default is per-second billing. Set AI_INBOUND_BILLING_ROUND_UP_TO_MINUTE=1 to enable minute rounding.
const AI_INBOUND_BILLING_ROUND_UP_TO_MINUTE = String(process.env.AI_INBOUND_BILLING_ROUND_UP_TO_MINUTE ?? '0') !== '0';

// AI inbound call acceptance gating based on local balance.
// If a user's balance falls below AI_INBOUND_MIN_CREDIT, inbound AI calls will be blocked.
const AI_INBOUND_MIN_CREDIT = parseFloat(process.env.AI_INBOUND_MIN_CREDIT || '0') || 0;
// If enabled, we proactively delete Daily dial-in configs while blocked and restore them when balance is sufficient.
const AI_INBOUND_DISABLE_NUMBERS_WHEN_BALANCE_LOW = String(process.env.AI_INBOUND_DISABLE_NUMBERS_WHEN_BALANCE_LOW ?? '1') !== '0';
// If enabled, a balance lookup failure will also block inbound calls (fail closed).
const AI_INBOUND_BALANCE_FAIL_CLOSED = String(process.env.AI_INBOUND_BALANCE_FAIL_CLOSED ?? '0') !== '0';

// AI caller memory (returning caller context)
// If enabled, the dial-in webhook will fetch recent conversation history for a returning caller
// (matched by digits-only caller number) and pass it into the agent runtime as context.
const AI_CALLER_MEMORY_ENABLE = String(process.env.AI_CALLER_MEMORY_ENABLE ?? '1') !== '0';
const AI_CALLER_MEMORY_MAX_CALLS = Math.max(1, parseInt(process.env.AI_CALLER_MEMORY_MAX_CALLS || '1', 10) || 1);
const AI_CALLER_MEMORY_MAX_MESSAGES = Math.max(0, parseInt(process.env.AI_CALLER_MEMORY_MAX_MESSAGES || '20', 10) || 20);
const AI_CALLER_MEMORY_MAX_CHARS_PER_MESSAGE = Math.max(
  50,
  parseInt(process.env.AI_CALLER_MEMORY_MAX_CHARS_PER_MESSAGE || '500', 10) || 500
);
const AI_CALLER_MEMORY_MAX_DAYS = Math.max(1, parseInt(process.env.AI_CALLER_MEMORY_MAX_DAYS || '30', 10) || 30);
// Outbound dialer settings
const DIALER_MIN_CONCURRENCY = 1;
const DIALER_MAX_CONCURRENCY = 20;
const DIALER_MAX_LEADS_PER_UPLOAD = Math.max(1, parseInt(process.env.DIALER_MAX_LEADS_PER_UPLOAD || '5000', 10) || 5000);
const DIALER_CAMPAIGN_STATUSES = ['draft', 'running', 'paused', 'completed', 'deleted'];
const DIALER_LEAD_STATUSES = ['pending', 'queued', 'dialing', 'answered', 'voicemail', 'transferred', 'failed', 'completed'];
const DIALER_WORKER_INTERVAL_SECONDS = Math.max(0, parseInt(process.env.DIALER_WORKER_INTERVAL_SECONDS || '0', 10) || 0);
const DIALER_DAILY_ROOM_TTL_SECONDS = Math.max(0, parseInt(process.env.DIALER_DAILY_ROOM_TTL_SECONDS || '1800', 10) || 1800);
// Dialer outbound call billing (per-minute equivalent, billed per second by default)
const DIALER_OUTBOUND_RATE_PER_MIN = parseFloat(
  process.env.DIALER_OUTBOUND_RATE_PER_MIN || String(AI_INBOUND_LOCAL_RATE_PER_MIN)
) || AI_INBOUND_LOCAL_RATE_PER_MIN;
// If 1, dialer outbound calls are billed in whole-minute increments (rounded up).
const DIALER_OUTBOUND_BILLING_ROUND_UP_TO_MINUTE = String(process.env.DIALER_OUTBOUND_BILLING_ROUND_UP_TO_MINUTE ?? '0') !== '0';

const dialerLeadUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 } // 10MB
});

// Optional: cancel/release AI numbers when monthly service fees cannot be paid.
// When enabled, the system will mark numbers as cancel_pending and attempt to release them.
const AI_MONTHLY_CANCEL_ON_INSUFFICIENT_BALANCE = String(process.env.AI_MONTHLY_CANCEL_ON_INSUFFICIENT_BALANCE ?? '0') !== '0';

// Grace period before cancelling AI numbers for nonpayment (in days).
const AI_MONTHLY_GRACE_DAYS = Math.max(0, parseInt(process.env.AI_MONTHLY_GRACE_DAYS || '3', 10) || 3);

app.set('trust proxy', 1);
app.set('etag', false);
const COOKIE_SECURE = process.env.COOKIE_SECURE === '1';

function joinUrl(base, p) {
  if (!base) return p || '';
  let b = String(base);
  let s = String(p || '');
  if (b.endsWith('/')) b = b.slice(0, -1);
  if (s && !s.startsWith('/')) s = '/' + s;
  return b + s;
}

function digitsOnly(s) {
  return String(s || '').replace(/[^0-9]/g, '');
}

function normalizeAiMessageId(raw) {
  const s = String(raw || '').trim();
  if (!s) return null;

  // Accept UUIDs by stripping hyphens.
  const noHyphen = s.replace(/-/g, '');
  if (/^[a-f0-9]{32}$/i.test(noHyphen)) return noHyphen.toLowerCase();

  // Fallback: hash any other identifier into a stable 32-hex id.
  try {
    return crypto.createHash('sha256').update(s).digest('hex').slice(0, 32);
  } catch {
    return null;
  }
}

function normalizeIpString(ip) {
  const s = String(ip || '').trim();
  if (!s) return null;
  const m = s.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
  if (m) return m[1];
  return s;
}

function getClientIp(req) {
  try {
    const ip = normalizeIpString(req?.ip);
    if (ip) return ip;

    // Fallbacks if trust proxy isn't effective (should be rare)
    const xff = req?.headers?.['x-forwarded-for'];
    if (xff) {
      const first = String(xff).split(',')[0].trim();
      const xip = normalizeIpString(first);
      if (xip) return xip;
    }

    const ra = req?.socket?.remoteAddress || req?.connection?.remoteAddress;
    return normalizeIpString(ra);
  } catch {
    return null;
  }
}

// ========== Log sanitization ==========
// Prevent secrets (tokens/passwords/etc.) from leaking into application logs.
const LOG_REDACT_VALUE = '***REDACTED***';

function shouldRedactLogKey(key) {
  const k = String(key || '').toLowerCase();
  if (!k) return false;

  // Credentials / secrets
  if (k === 'password' || k === 'pass' || k === 'passwd' || k === 'pwd') return true;
  if (k.includes('password') || k.includes('passwd')) return true;

  if (k === 'code') return true; // OTP / verification codes

  // Tokens / auth
  if (k.includes('token')) return true; // access_token, refresh_token, id_token, etc.
  if (k.includes('secret')) return true;
  if (k.includes('api_key') || k === 'apikey' || k.includes('apikey') || k.includes('api-key')) return true;
  if (k.includes('client_secret') || k.includes('clientsecret')) return true;
  if (k.includes('private_key') || k.includes('privatekey')) return true;
  if (k === 'authorization' || k.endsWith('authorization')) return true;
  if (k === 'cookie' || k === 'set-cookie') return true;

  // Bank account details (never log)
  // NOTE: avoid over-redacting non-sensitive IDs like account_id/external_account_id.
  if (k.includes('routing_number') || k.includes('routingnumber')) return true;
  if (k === 'aba' || k.includes('bank_routing')) return true;
  if (k.includes('account_number') || k.includes('accountnumber')) return true;
  if (k.includes('bank_account_number') || k.includes('bankaccountnumber')) return true;

  return false;
}

function sanitizeForLog(value, opts) {
  const options = Object.assign({
    maxDepth: 5,
    maxKeys: 100,
    maxArrayLength: 50,
    maxStringLength: 300
  }, opts || {});

  const seen = new WeakSet();

  function inner(v, depth) {
    if (v == null) return v;

    const t = typeof v;
    if (t === 'string') {
      if (v.length > options.maxStringLength) return v.slice(0, options.maxStringLength) + '…';
      return v;
    }
    if (t === 'number' || t === 'boolean') return v;
    if (t === 'bigint') return String(v);
    if (t !== 'object') return String(v);

    if (Buffer.isBuffer(v)) return `[Buffer ${v.length} bytes]`;
    if (v instanceof Date) return v.toISOString();

    if (seen.has(v)) return '[Circular]';
    if (depth >= options.maxDepth) return '[Object]';

    seen.add(v);

    if (Array.isArray(v)) {
      const out = [];
      const n = Math.min(v.length, options.maxArrayLength);
      for (let i = 0; i < n; i++) out.push(inner(v[i], depth + 1));
      if (v.length > n) out.push(`[+${v.length - n} more]`);
      return out;
    }

    const out = {};
    const keys = Object.keys(v);
    const n = Math.min(keys.length, options.maxKeys);
    for (let i = 0; i < n; i++) {
      const k = keys[i];
      out[k] = shouldRedactLogKey(k) ? LOG_REDACT_VALUE : inner(v[k], depth + 1);
    }
    if (keys.length > n) out._truncatedKeys = keys.length - n;
    return out;
  }

  try {
    return inner(value, 0);
  } catch {
    return '[Unserializable]';
  }
}

// ========== Click2Mail (Physical Mail) ==========
const CLICK2MAIL_USERNAME = String(process.env.CLICK2MAIL_USERNAME || '').trim();
const CLICK2MAIL_PASSWORD = String(process.env.CLICK2MAIL_PASSWORD || '').trim();
// Defaults are the Click2Mail staging subdomains used in their Postman collections.
// Staging REST:  https://stage-rest.click2mail.com
// Staging Batch: https://stage-batch.click2mail.com
const CLICK2MAIL_REST_BASE_URL = String(process.env.CLICK2MAIL_REST_BASE_URL || 'https://stage-rest.click2mail.com').trim().replace(/\/+$/, '');
const CLICK2MAIL_BATCH_BASE_URL = String(process.env.CLICK2MAIL_BATCH_BASE_URL || 'https://stage-batch.click2mail.com').trim().replace(/\/+$/, '');
const CLICK2MAIL_APP_SIGNATURE = String(process.env.CLICK2MAIL_APP_SIGNATURE || 'Phone.System').trim() || 'Phone.System';

// Default Click2Mail product options for AI physical mail
// User requirement: default product should be "Priority Mail Letter 8.5 x 11".
const CLICK2MAIL_DEFAULT_DOCUMENT_CLASS = String(process.env.CLICK2MAIL_DEFAULT_DOCUMENT_CLASS || 'Priority Mail Letter 8.5 x 11').trim();
const CLICK2MAIL_DEFAULT_LAYOUT = String(process.env.CLICK2MAIL_DEFAULT_LAYOUT || 'Address on Separate Page').trim();
const CLICK2MAIL_DEFAULT_PRODUCTION_TIME = String(process.env.CLICK2MAIL_DEFAULT_PRODUCTION_TIME || 'Same Day').trim();
const CLICK2MAIL_DEFAULT_ENVELOPE = String(process.env.CLICK2MAIL_DEFAULT_ENVELOPE || 'Flat Rate USPS Priority Mail').trim();
const CLICK2MAIL_DEFAULT_COLOR = String(process.env.CLICK2MAIL_DEFAULT_COLOR || 'Black and White').trim();
const CLICK2MAIL_DEFAULT_PAPER_TYPE = String(process.env.CLICK2MAIL_DEFAULT_PAPER_TYPE || 'White 24#').trim();
const CLICK2MAIL_DEFAULT_PRINT_OPTION = String(process.env.CLICK2MAIL_DEFAULT_PRINT_OPTION || 'Printing both sides').trim();
const CLICK2MAIL_DEFAULT_MAIL_CLASS = String(process.env.CLICK2MAIL_DEFAULT_MAIL_CLASS || 'Priority Mail with Delivery Confirmation').trim();
// Batch tracking uses trackingType=impb in Click2Mail examples.
const CLICK2MAIL_DEFAULT_TRACKING_TYPE = String(process.env.CLICK2MAIL_DEFAULT_TRACKING_TYPE || 'impb').trim();

// Optional: platform markup added on top of Click2Mail cost estimate (USD)
// Example: AI_MAIL_MARKUP_FLAT=0.50 and AI_MAIL_MARKUP_PERCENT=10 => add $0.50 + 10% of base cost.
const AI_MAIL_MARKUP_FLAT = Math.max(0, parseFloat(process.env.AI_MAIL_MARKUP_FLAT || '0') || 0);
const AI_MAIL_MARKUP_PERCENT = Math.max(0, parseFloat(process.env.AI_MAIL_MARKUP_PERCENT || '0') || 0);

// Physical mail is high-risk; require an explicit opt-in.
const AI_PHYSICAL_MAIL_ENABLED = ['1', 'true', 'yes', 'on'].includes(
  String(process.env.AI_PHYSICAL_MAIL_ENABLED || '').trim().toLowerCase()
);

// Optional: platform fees for AI tools (USD). Set to 0 to disable charging for that action.
const AI_EMAIL_SEND_COST = Math.max(0, parseFloat(process.env.AI_EMAIL_SEND_COST || '1') || 0);
const AI_VIDEO_MEETING_LINK_COST = Math.max(0, parseFloat(process.env.AI_VIDEO_MEETING_LINK_COST || '5') || 0);
const AI_SMS_SEND_COST = Math.max(0, parseFloat(process.env.AI_SMS_SEND_COST || '0') || 0);

const click2mailXmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  removeNSPrefix: true,
  trimValues: true,
  parseTagValue: true,
  parseAttributeValue: false
});

function click2mailEnsureConfigured() {
  if (!CLICK2MAIL_USERNAME || !CLICK2MAIL_PASSWORD) {
    throw new Error('Click2Mail credentials not configured (CLICK2MAIL_USERNAME / CLICK2MAIL_PASSWORD)');
  }
  if (!CLICK2MAIL_REST_BASE_URL || !CLICK2MAIL_BATCH_BASE_URL) {
    throw new Error('Click2Mail base URLs not configured');
  }
}

function click2mailParseXml(xmlText) {
  try {
    const s = String(xmlText || '').trim();
    if (!s) return null;
    return click2mailXmlParser.parse(s);
  } catch {
    return null;
  }
}

function xmlEscape(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function click2mailAxios({ baseURL }) {
  click2mailEnsureConfigured();
  return axios.create({
    baseURL,
    auth: { username: CLICK2MAIL_USERNAME, password: CLICK2MAIL_PASSWORD },
    timeout: 30000,
    // Click2Mail returns XML; keep response as text
    transformResponse: [(d) => d]
  });
}

function estimatePdfPageCount(pdfBuffer) {
  try {
    if (!pdfBuffer || !Buffer.isBuffer(pdfBuffer)) return 0;
    // Crude but generally reliable for PDFs produced by LibreOffice.
    const s = pdfBuffer.toString('latin1');
    const m = s.match(/\/Type\s*\/Page\b/g);
    return m ? m.length : 0;
  } catch {
    return 0;
  }
}

function normalizeUsState2(state) {
  const s = String(state || '').trim().toUpperCase();
  if (!s) return '';
  // Already 2-letter
  if (/^[A-Z]{2}$/.test(s)) return s;
  return s.slice(0, 2);
}

function normalizePostalCode(zip) {
  const raw = String(zip || '').trim();
  if (!raw) return '';
  // Allow ZIP+4
  const cleaned = raw.replace(/\s+/g, '');
  return cleaned;
}

function normalizeClick2MailCountry(country) {
  const c = String(country || '').trim();
  if (!c) return 'US';
  if (c.length === 2) return c.toUpperCase();
  if (/^united\s*states/i.test(c)) return 'US';
  return c.toUpperCase().slice(0, 2);
}

async function click2mailAddressCorrectionOne(address) {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_REST_BASE_URL });

  const name = String(address?.name || '').trim();
  const organization = String(address?.organization || '').trim();
  const address1 = String(address?.address1 || '').trim();
  const address2 = String(address?.address2 || '').trim();
  const address3 = String(address?.address3 || '').trim();
  const city = String(address?.city || '').trim();
  const state = normalizeUsState2(address?.state || '');
  const zip = normalizePostalCode(address?.postalCode || address?.zip || '');
  const country = normalizeClick2MailCountry(address?.country || 'US');

  const xml =
    `<addresses>` +
      `<address>` +
        (name ? `<name>${xmlEscape(name)}</name>` : '') +
        (organization ? `<organization>${xmlEscape(organization)}</organization>` : '') +
        (address1 ? `<address1>${xmlEscape(address1)}</address1>` : '') +
        (address2 ? `<address2>${xmlEscape(address2)}</address2>` : '') +
        (address3 ? `<address3>${xmlEscape(address3)}</address3>` : '') +
        (city ? `<city>${xmlEscape(city)}</city>` : '') +
        (state ? `<state>${xmlEscape(state)}</state>` : '') +
        (zip ? `<zip>${xmlEscape(zip)}</zip>` : '') +
        (country ? `<country>${xmlEscape(country)}</country>` : '') +
      `</address>` +
    `</addresses>`;

  const resp = await client.post('/molpro/addressCorrection', xml, {
    headers: { 'Content-Type': 'application/xml', 'Accept': 'application/xml' }
  });

  const parsed = click2mailParseXml(resp.data);
  return { raw: resp.data, parsed };
}

function pickFirst(obj, paths) {
  for (const p of paths) {
    try {
      const v = p.split('.').reduce((acc, k) => (acc && acc[k] != null) ? acc[k] : undefined, obj);
      if (v != null) return v;
    } catch {}
  }
  return undefined;
}

function toArrayMaybe(v) {
  if (Array.isArray(v)) return v;
  if (v == null) return [];
  return [v];
}

function extractClick2MailTrackingFromXml(parsed) {
  // Based on Click2Mail docs/examples; batch tracking uses the same shape.
  // It may be { tracking: { status: '0', description: 'Success', trackingDetails: { trackingDetail: [...] }}}
  const root = parsed || {};
  const tracking = root.tracking || root.Tracking || root.trackingResponse || root.trackingresponse || root;

  const status = String(pickFirst(tracking, ['status', 'tracking.status', 'trackingStatus']) ?? '').trim();
  const description = String(pickFirst(tracking, ['description', 'tracking.description']) ?? '').trim();

  // Try common list locations
  let details = pickFirst(tracking, [
    'trackingDetails.trackingDetail',
    'trackingdetails.trackingdetail',
    'trackingDetail',
    'trackingdetail'
  ]);
  details = toArrayMaybe(details);

  const items = details.map(d => {
    const barCode = String(pickFirst(d, ['barCode', 'barcode', 'bar_code']) ?? '').trim();
    const statusText = String(pickFirst(d, ['status', 'statusText', 'statustext']) ?? '').trim();
    const dateTime = String(pickFirst(d, ['dateTime', 'datetime', 'date_time']) ?? '').trim();
    const statusLocation = String(pickFirst(d, ['statusLocation', 'statuslocation', 'location']) ?? '').trim();
    return { barCode, status: statusText, dateTime, statusLocation };
  }).filter(x => x.barCode || x.status || x.dateTime || x.statusLocation);

  return { status, description, items };
}

function parseBool01(val) {
  const s = String(val ?? '').trim().toLowerCase();
  if (s === '1' || s === 'true' || s === 'yes') return true;
  if (s === '0' || s === 'false' || s === 'no') return false;
  return null;
}

function extractClick2MailAddressCorrectionFromXml(parsed) {
  const root = parsed || {};
  const ac = root.addressCorrection || root.addresscorrection || root.address_correction || root;
  const status = String(pickFirst(ac, ['status', 'addressCorrection.status']) ?? '').trim();
  const description = String(pickFirst(ac, ['description', 'addressCorrection.description']) ?? '').trim();

  // Try to locate address list
  let addrs = pickFirst(ac, [
    'addresses.address',
    'Addresses.Address',
    'addresses',
    'address'
  ]);
  addrs = toArrayMaybe(addrs);

  // Find the first object-like address
  const first = addrs.find(a => a && typeof a === 'object') || null;
  if (!first) {
    return { status, description, address: null, standard: null, international: null, nonmailable: null };
  }

  const addr = {
    name: String(pickFirst(first, ['name', 'Name']) ?? '').trim(),
    organization: String(pickFirst(first, ['organization', 'Organization']) ?? '').trim(),
    address1: String(pickFirst(first, ['address1', 'Address1']) ?? '').trim(),
    address2: String(pickFirst(first, ['address2', 'Address2']) ?? '').trim(),
    address3: String(pickFirst(first, ['address3', 'Address3']) ?? '').trim(),
    city: String(pickFirst(first, ['city', 'City']) ?? '').trim(),
    state: String(pickFirst(first, ['state', 'State']) ?? '').trim(),
    postalCode: String(pickFirst(first, ['zip', 'Zip', 'postalCode', 'Postalcode', 'PostalCode']) ?? '').trim(),
    country: String(pickFirst(first, ['country', 'Country']) ?? '').trim()
  };

  // Flags described in Click2Mail FAQ
  const standard = parseBool01(pickFirst(first, ['standard', 'Standard']));
  const international = parseBool01(pickFirst(first, ['international', 'International']));
  const nonmailable = parseBool01(pickFirst(first, ['nonmailable', 'nonMailable', 'Nonmailable', 'NonMailable']));

  return { status, description, address: addr, standard, international, nonmailable };
}

function extractClick2MailBatchStatus(parsed) {
  const root = parsed || {};
  const bj = root.batchjob || root.batchJob || root;
  const id = String(pickFirst(bj, ['id', 'batchId']) ?? '').trim();
  const hasErrors = String(pickFirst(bj, ['hasErrors']) ?? '').trim();
  const received = String(pickFirst(bj, ['received']) ?? '').trim();
  const submitted = String(pickFirst(bj, ['submitted']) ?? '').trim();
  const processing = String(pickFirst(bj, ['processing']) ?? '').trim();
  const completed = String(pickFirst(bj, ['completed']) ?? '').trim();
  return { id, hasErrors, received, submitted, processing, completed };
}

function extractClick2MailCostEstimateFromXml(parsed) {
  // Best-effort: Click2Mail XML responses vary by endpoint/product.
  // We try to find a numeric total under common key names.
  const candidates = [];

  function walk(obj, path) {
    if (!obj || typeof obj !== 'object') return;
    for (const [k, v] of Object.entries(obj)) {
      const p = path ? `${path}.${k}` : k;
      if (v && typeof v === 'object') {
        walk(v, p);
      } else {
        const key = String(k || '').toLowerCase();
        const pLower = p.toLowerCase();
        const n = parseFloat(String(v).replace(/[^0-9.+-]/g, ''));
        if (!Number.isFinite(n)) continue;

        // Ignore obvious non-cost numeric fields
        if (/(^|\.)id$/.test(pLower)) continue;
        if (key.includes('page') || key.includes('quantity')) continue;

        let score = 0;
        if (/grand.*total/.test(pLower) || /total.*(cost|amount|price)/.test(pLower)) score = 3;
        else if (key.includes('total')) score = 2;
        else if (/(cost|amount|price|postage)/.test(key)) score = 1;

        if (score > 0) candidates.push({ score, amount: n, path: p });
      }
    }
  }

  walk(parsed || {}, '');

  candidates.sort((a, b) => (b.score - a.score) || (b.amount - a.amount));
  const best = candidates.find(c => c.amount > 0 && c.amount < 1000) || null;
  if (!best) return null;
  return { amount: best.amount, path: best.path };
}

async function click2mailBatchCreate() {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_BATCH_BASE_URL });
  const resp = await client.post('/v1/batches', null, { headers: { 'Accept': 'application/xml' } });
  const parsed = click2mailParseXml(resp.data);
  const batchId = String(pickFirst(parsed, ['batchjob.id', 'batchJob.id', 'batchjob.batchId', 'batchjob.id']) || '').trim();
  return { raw: resp.data, parsed, batchId };
}

async function click2mailBatchUploadPdf(batchId, pdfBuffer, { filename = 'document.pdf' } = {}) {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_BATCH_BASE_URL });
  const resp = await client.put(`/v1/batches/${encodeURIComponent(String(batchId))}`, pdfBuffer, {
    headers: {
      'Content-Type': 'application/pdf',
      'Content-Disposition': `attachment; filename="${String(filename || 'document.pdf').replace(/\"/g, '')}"`
    }
  });
  return { status: resp.status };
}

async function click2mailBatchUploadXml(batchId, xmlText, { filename = 'batch.xml' } = {}) {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_BATCH_BASE_URL });
  const resp = await client.put(`/v1/batches/${encodeURIComponent(String(batchId))}`, xmlText, {
    headers: {
      'Content-Type': 'application/xml',
      'Accept': 'application/xml',
      'Content-Disposition': `attachment; filename="${String(filename || 'batch.xml').replace(/\"/g, '')}"`
    }
  });
  return { status: resp.status, raw: resp.data };
}

async function click2mailBatchSubmit(batchId) {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_BATCH_BASE_URL });
  const resp = await client.post(`/v1/batches/${encodeURIComponent(String(batchId))}`, null, { headers: { 'Accept': 'application/xml' } });
  const parsed = click2mailParseXml(resp.data);
  return { status: resp.status, raw: resp.data, parsed };
}

async function click2mailBatchStatus(batchId) {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_BATCH_BASE_URL });
  const resp = await client.get(`/v1/batches/${encodeURIComponent(String(batchId))}`, { headers: { 'Accept': 'application/xml' } });
  const parsed = click2mailParseXml(resp.data);
  return { status: resp.status, raw: resp.data, parsed };
}

async function click2mailBatchDetails(batchId) {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_BATCH_BASE_URL });
  const resp = await client.get(`/v1/batches/${encodeURIComponent(String(batchId))}/details`, { headers: { 'Accept': 'application/xml' } });
  const parsed = click2mailParseXml(resp.data);
  return { status: resp.status, raw: resp.data, parsed };
}

async function click2mailCostEstimate(queryParams) {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_REST_BASE_URL });
  const resp = await client.get('/molpro/costEstimate', {
    params: queryParams || {},
    headers: { 'Accept': 'application/xml' }
  });
  const parsed = click2mailParseXml(resp.data);
  return { status: resp.status, raw: resp.data, parsed };
}

async function click2mailBatchTracking(batchId, trackingType = 'impb') {
  const client = click2mailAxios({ baseURL: CLICK2MAIL_BATCH_BASE_URL });
  const resp = await client.get(`/v1/batches/${encodeURIComponent(String(batchId))}/tracking?trackingType=${encodeURIComponent(String(trackingType || 'impb'))}`, {
    headers: { 'Accept': 'application/xml' }
  });
  const parsed = click2mailParseXml(resp.data);
  const extracted = extractClick2MailTrackingFromXml(parsed);
  return { status: resp.status, raw: resp.data, parsed, extracted };
}

function passwordIsAlnum(pwd) {
  return /^[A-Za-z0-9]+$/.test(String(pwd || ''));
}
function usernameIsDigitsMin10(u){
  return /^\d{10,}$/.test(String(u||''));
}
function usernameIsAlnumMax10(u){
  return /^[A-Za-z0-9]{1,10}$/.test(String(u||''));
}
function passwordIsAlnumMax10(pwd){
  return /^[A-Za-z0-9]{1,10}$/.test(String(pwd||''));
}
function buildNonce() {
  const nowSec = Math.floor(Date.now() / 1000);
  const micro = String(Number(process.hrtime.bigint() % 1000000n)).padStart(6, '0');
  return `${nowSec}${micro}`;
}
function requireAuth(req, res, next) {
  if (req.session && req.session.userId) return next();
  if (DEBUG) console.warn('[auth.fail]', { sid: req.sessionID, hasSession: !!req.session, userId: req.session?.userId });
  // Return JSON 401 for API routes so fetch() callers get parseable error
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ success: false, message: 'Session expired. Please log in again.' });
  }
  return res.redirect('/login');
}
async function requireAdmin(req, res, next) {
  // First ensure user is authenticated
  if (!req.session || !req.session.userId) {
    if (DEBUG) console.warn('[admin.auth.fail]', { sid: req.sessionID, hasSession: !!req.session, userId: req.session?.userId });
    if (req.path.startsWith('/api/')) {
      return res.status(401).json({ success: false, message: 'Session expired. Please log in again.' });
    }
    return res.redirect('/login');
  }
  // Check if user is admin
  try {
    if (!pool) {
      if (DEBUG) console.warn('[admin.auth.fail] No database pool');
      return res.redirect('/dashboard');
    }
    const [rows] = await pool.execute('SELECT is_admin FROM signup_users WHERE id=? LIMIT 1', [req.session.userId]);
    const row = rows && rows[0];
    if (!row || !row.is_admin) {
      if (DEBUG) console.warn('[admin.auth.fail]', { userId: req.session.userId, isAdmin: row?.is_admin });
      return res.redirect('/dashboard');
    }
    return next();
  } catch (e) {
    if (DEBUG) console.warn('[admin.auth.error]', e.message || e);
    return res.redirect('/dashboard');
  }
}
const PREFETCH_DAYS = parseInt(process.env.CDR_PREFETCH_DAYS || '7', 10);
function ymd(d){ return new Date(d.getTime() - d.getTimezoneOffset()*60000).toISOString().slice(0,10); }
function defaultRange(){ const now=new Date(); const from=new Date(now.getTime()-(PREFETCH_DAYS-1)*86400000); return { from: ymd(from), to: ymd(now) }; }
// Views + Sessions + Middleware
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
// Session configuration; store will be attached after initDb() creates sessionStore
const sessionConfig = {
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    // In dev over HTTP, set COOKIE_SECURE=0 (default). In prod behind HTTPS, set COOKIE_SECURE=1
    secure: COOKIE_SECURE,
    maxAge: 7 * 24 * 60 * 60 * 1000
  }
};
let sessionMiddleware;
app.use((req, res, next) => {
  // sessionMiddleware is created in startServer() after DB init.
  // As a safety net, if it is somehow missing, fall back to a
  // MemoryStore-backed session with a warning.
  if (!sessionMiddleware) {
    if (DEBUG) console.warn('[session] Session middleware used before DB init; falling back to MemoryStore');
    sessionMiddleware = session(sessionConfig);
  }
  return sessionMiddleware(req, res, next);
});
// Capture raw JSON body (for IPN signature verification, etc.)
app.use(bodyParser.json({
  // Accept both application/json and application/*+json vendor types.
  type: ['application/json', 'application/*+json'],
  verify: (req, res, buf) => {
    // Store raw body buffer for routes that need HMAC verification (e.g., NOWPayments IPN)
    req.rawBody = buf;
  }
}));
app.use(bodyParser.urlencoded({ extended: true }));
// Lightweight request logging (always on). Logs when the response finishes.
// IMPORTANT: always sanitize query/body to avoid leaking secrets in logs.
app.use((req, res, next) => {
  const t0 = process.hrtime.bigint();
  const wantsLog = req.path.startsWith('/api/') || req.path === '/login' || req.path === '/dashboard';
  res.on('finish', () => {
    if (!wantsLog) return;
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    const isWrite = req.method === 'POST' || req.method === 'PUT' || req.method === 'PATCH';
    const line = {
      at: new Date().toISOString(),
      status: res.statusCode,
      method: req.method,
      path: req.path,
      query: sanitizeForLog(req.query || {}),
      body: isWrite ? sanitizeForLog(req.body) : undefined,
      ms: Number(ms.toFixed(1))
    };
    try { console.log('[req]', JSON.stringify(line)); } catch {}
  });
  next();
});
// Prevent caching on user-scoped/admin API responses to avoid cross-user data
function noCache(res) {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  res.set('Vary', 'Cookie');
}
app.use('/api/me', (req, res, next) => { noCache(res); next(); });
app.use('/api/admin', (req, res, next) => { noCache(res); next(); });

// Email verification store (MySQL)
const OTP_TTL_MS = (parseInt(process.env.EMAIL_VERIFICATION_TTL_MINUTES) || 10) * 60 * 1000;
const OTP_MAX_ATTEMPTS = parseInt(process.env.OTP_MAX_ATTEMPTS || '5', 10);
function otp() { return String(Math.floor(100000 + Math.random() * 900000)); }
function sha256(s) { return crypto.createHash('sha256').update(String(s)).digest('hex'); }

// ========== Encryption helpers (AES-256-GCM) ==========
// Used for storing per-user SMTP passwords and per-agent action tokens.
let cachedUserSmtpEncKey = undefined;
function getUserSmtpEncryptionKey() {
  if (cachedUserSmtpEncKey !== undefined) return cachedUserSmtpEncKey;
  const raw = String(process.env.USER_SMTP_ENCRYPTION_KEY || '').trim();
  if (!raw) {
    cachedUserSmtpEncKey = null;
    return cachedUserSmtpEncKey;
  }
  try {
    const buf = Buffer.from(raw, 'base64');
    if (buf.length !== 32) throw new Error('Expected 32 bytes');
    cachedUserSmtpEncKey = buf;
    return cachedUserSmtpEncKey;
  } catch (e) {
    cachedUserSmtpEncKey = null;
    if (DEBUG) console.warn('[enc] Invalid USER_SMTP_ENCRYPTION_KEY; must be base64-encoded 32 bytes');
    return cachedUserSmtpEncKey;
  }
}

function encryptAes256Gcm(plainText, keyBuf) {
  if (!keyBuf || !Buffer.isBuffer(keyBuf) || keyBuf.length !== 32) {
    throw new Error('Encryption key not configured');
  }
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', keyBuf, iv);
  const enc = Buffer.concat([cipher.update(String(plainText || ''), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return { enc, iv, tag };
}

function decryptAes256Gcm(encBuf, ivBuf, tagBuf, keyBuf) {
  if (!keyBuf || !Buffer.isBuffer(keyBuf) || keyBuf.length !== 32) {
    throw new Error('Encryption key not configured');
  }
  if (!encBuf || !ivBuf || !tagBuf) return '';
  const decipher = crypto.createDecipheriv('aes-256-gcm', keyBuf, ivBuf);
  decipher.setAuthTag(tagBuf);
  const plain = Buffer.concat([decipher.update(encBuf), decipher.final()]).toString('utf8');
  return plain;
}

let pool;
let sessionStore;

// Basic rate limits (per IP)
// Login: allow moderate retries without being too strict (20 per 15 minutes)
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, message: 'Too many login attempts. Please try again later.' }
});

// OTP-related endpoints
// - Sending codes must be strict (prevents email flooding)
// - Verifying codes can be more lenient (attempts are also limited per token in DB)
const otpSendLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, message: 'Too many verification code requests. Please try again later.' }
});
const otpVerifyLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, message: 'Too many verification attempts. Please try again later.' }
});
// ========== Local User & Balance Management Helper Functions ==========
// These use the module-level `pool` variable initialised by initDb().

async function getUserBalance(userId) {
  try {
    if (!pool) throw new Error('Database not configured');
    const [rows] = await pool.query(
      'SELECT balance FROM users WHERE id = ? AND is_active = 1',
      [userId]
    );
    if (!rows || rows.length === 0) return null;
    return parseFloat(rows[0].balance || 0);
  } catch (err) {
    console.error('[getUserBalance] Error:', err);
    return null;
  }
}

async function updateUserBalance(userId, amount, description, transactionType = 'adjustment', paymentMethod = null, referenceId = null) {
  if (!pool) throw new Error('Database not configured');
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [userRows] = await conn.query(
      'SELECT balance FROM users WHERE id = ? AND is_active = 1 FOR UPDATE',
      [userId]
    );
    if (!userRows || userRows.length === 0) {
      throw new Error('User not found or inactive');
    }
    const balanceBefore = parseFloat(userRows[0].balance || 0);
    const balanceAfter = balanceBefore + amount;
    const [ins] = await conn.query(
      'UPDATE users SET balance = ?, updated_at = NOW() WHERE id = ?',
      [balanceAfter, userId]
    );
    await conn.query(
      `INSERT INTO billing_history
       (user_id, amount, description, transaction_type, payment_method, reference_id,
        balance_before, balance_after, status, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', NOW())`,
      [userId, amount, description, transactionType, paymentMethod, referenceId,
       balanceBefore, balanceAfter]
    );
    const billingId = ins && ins.insertId ? Number(ins.insertId) : null;
    await conn.commit();
    if (DEBUG) {
      console.log('[updateUserBalance]', {
        userId, amount, balanceBefore, balanceAfter, transactionType,
        description: description.substring(0, 50)
      });
    }
    return { success: true, ok: true, balanceAfter, balanceBefore, billingId };
  } catch (err) {
    await conn.rollback();
    console.error('[updateUserBalance] Error:', err);
    return { success: false, ok: false, error: err.message };
  } finally {
    conn.release();
  }
}

async function deductBalance(userId, amount, description) {
  return await updateUserBalance(userId, -Math.abs(amount), description, 'debit');
}

async function creditBalance(userId, amount, description, paymentMethod, referenceId) {
  return await updateUserBalance(userId, Math.abs(amount), description, 'credit', paymentMethod, referenceId);
}

async function storeCdr(userIdOrPayload, direction, srcNumber, dstNumber, didNumber, timeStart, timeEnd, duration, billsec, price, status, aiAgentId, campaignId, sessionId) {
  try {
    if (!pool) throw new Error('Database not configured');

    // Backward-compatible API:
    // 1) Positional args (legacy callers)
    // 2) Single object payload (newer callers)
    const payload = (userIdOrPayload && typeof userIdOrPayload === 'object' && !Array.isArray(userIdOrPayload))
      ? userIdOrPayload
      : null;

    const userIdVal = payload ? payload.userId : userIdOrPayload;
    const directionVal = payload ? (payload.direction || direction || 'inbound') : direction;
    const srcNumberVal = payload ? (payload.srcNumber ?? payload.fromNumber ?? srcNumber ?? null) : srcNumber;
    const dstNumberVal = payload ? (payload.dstNumber ?? payload.toNumber ?? dstNumber ?? null) : dstNumber;
    const didNumberVal = payload ? (payload.didNumber ?? payload.toNumber ?? didNumber ?? null) : didNumber;
    const timeStartVal = payload ? (payload.timeStart ?? payload.startTime ?? timeStart ?? null) : timeStart;
    const timeEndVal = payload ? (payload.timeEnd ?? payload.endTime ?? timeEnd ?? null) : timeEnd;
    const durationVal = payload ? (payload.duration ?? duration ?? null) : duration;
    const billsecVal = payload ? (payload.billsec ?? billsec ?? null) : billsec;
    const priceVal = payload ? (payload.price ?? payload.cost ?? price ?? null) : price;
    const statusVal = payload ? (payload.status ?? status ?? null) : status;
    const aiAgentIdVal = payload ? (payload.aiAgentId ?? payload.agentId ?? aiAgentId ?? null) : aiAgentId;
    const campaignIdVal = payload ? (payload.campaignId ?? campaignId ?? null) : campaignId;
    const sessionIdVal = payload ? (payload.sessionId ?? sessionId ?? null) : sessionId;

    if (!userIdVal) throw new Error('Missing userId for CDR insert');
    await pool.query(
      `INSERT INTO cdrs
       (user_id, direction, src_number, dst_number, did_number, time_start, time_end,
        duration, billsec, price, status, ai_agent_id, campaign_id, session_id, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`,
      [userIdVal, directionVal, srcNumberVal, dstNumberVal, didNumberVal, timeStartVal, timeEndVal,
       durationVal, billsecVal, priceVal, statusVal, aiAgentIdVal, campaignIdVal, sessionIdVal]
    );
    if (DEBUG) {
      console.log('[storeCdr]', {
        userId: userIdVal,
        direction: directionVal,
        srcNumber: srcNumberVal,
        dstNumber: dstNumberVal,
        duration: durationVal,
        price: priceVal,
        status: statusVal
      });
    }
    return { success: true };
  } catch (err) {
    console.error('[storeCdr] Error:', err);
    return { success: false, error: err.message };
  }
}

async function getUserById(userId) {
  try {
    if (!pool) throw new Error('Database not configured');
    const [rows] = await pool.query(
      `SELECT id, username, email, name, phone, balance,
              address1, city, state, postal_code, country, signup_ip,
              is_active, created_at
       FROM users
       WHERE id = ? AND is_active = 1`,
      [userId]
    );
    if (!rows || rows.length === 0) return null;
    return rows[0];
  } catch (err) {
    console.error('[getUserById] Error:', err);
    return null;
  }
}

async function getUserByUsernameOrEmail(identifier) {
  try {
    if (!pool) throw new Error('Database not configured');
    const [rows] = await pool.query(
      `SELECT id, username, email, password_hash, name, is_active
       FROM users
       WHERE (username = ? OR email = ?) AND is_active = 1
       LIMIT 1`,
      [identifier, identifier]
    );
    if (!rows || rows.length === 0) return null;
    return rows[0];
  } catch (err) {
    console.error('[getUserByUsernameOrEmail] Error:', err);
    return null;
  }
}

async function userExists(username, email) {
  try {
    if (!pool) throw new Error('Database not configured');
    const [rows] = await pool.query(
      'SELECT id FROM users WHERE username = ? OR email = ? LIMIT 1',
      [username, email]
    );
    return rows && rows.length > 0;
  } catch (err) {
    console.error('[userExists] Error:', err);
    return false;
  }
}

async function createUser(userData) {
  try {
    if (!pool) throw new Error('Database not configured');
    const {
      username, email, passwordHash, name = null, phone = null,
      balance = 0.00, signupIp = null,
      address1 = null, city = null, state = null, postalCode = null, country = null
    } = userData;
    const [result] = await pool.query(
      `INSERT INTO users
       (username, email, password_hash, name, phone, balance, signup_ip,
        address1, city, state, postal_code, country, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`,
      [username, email, passwordHash, name, phone, balance, signupIp,
       address1, city, state, postalCode, country]
    );
    const userId = result.insertId;
    if (DEBUG) {
      console.log('[createUser] User created:', { userId, username, email, balance });
    }
    return { success: true, userId };
  } catch (err) {
    console.error('[createUser] Error:', err);
    return { success: false, error: err.message };
  }
}

async function updateUserProfile(userId, updates) {
  try {
    if (!pool) throw new Error('Database not configured');
    const allowedFields = ['name', 'phone', 'address1', 'city', 'state', 'postal_code', 'country'];
    const fields = [];
    const values = [];
    for (const [key, value] of Object.entries(updates)) {
      if (allowedFields.includes(key) && value !== undefined) {
        fields.push(`${key} = ?`);
        values.push(value);
      }
    }
    if (fields.length === 0) {
      return { success: true, message: 'No fields to update' };
    }
    values.push(userId);
    await pool.query(
      `UPDATE users SET ${fields.join(', ')}, updated_at = NOW() WHERE id = ?`,
      values
    );
    if (DEBUG) {
      console.log('[updateUserProfile]', { userId, fields: Object.keys(updates) });
    }
    return { success: true };
  } catch (err) {
    console.error('[updateUserProfile] Error:', err);
    return { success: false, error: err.message };
  }
}

// MagnusBilling was removed — this stub prevents ReferenceError in the CDR timeline.
async function loadLocalOutboundCdrs() {
  return [];
}

async function initDb() {
  const dsn = process.env.DATABASE_URL;
  if (!dsn) return; // DB optional; if missing, endpoints will throw when used
  const u = new URL(dsn);
  const dbName = u.pathname.replace(/^\//, '');
  // TLS options
  const sslMode = (u.searchParams.get('ssl-mode') || u.searchParams.get('sslmode') || '').toUpperCase();
  const caPath = process.env.DATABASE_CA_CERT;
  let sslOptions;
  if (caPath && fs.existsSync(caPath)) {
    sslOptions = { ca: fs.readFileSync(caPath, 'utf8'), rejectUnauthorized: true };
  } else if (sslMode === 'REQUIRED') {
    sslOptions = { rejectUnauthorized: false }; // encrypted, non-verified fallback
  }

  pool = mysql.createPool({
    host: u.hostname,
    port: Number(u.port) || 3306,
    user: decodeURIComponent(u.username),
    password: decodeURIComponent(u.password),
    database: dbName,
    ssl: sslOptions,
    connectionLimit: 50,
    waitForConnections: true,
    queueLimit: 0
  });
  
  // Initialize MySQL session store
  sessionStore = new MySQLStore({
    clearExpired: true,
    checkExpirationInterval: 900000, // 15 minutes
    expiration: 86400000 * 7, // 7 days
    createDatabaseTable: true,
    schema: {
      tableName: 'sessions',
      columnNames: {
        session_id: 'session_id',
        expires: 'expires',
        data: 'data'
      }
    }
  }, pool.pool);

  await pool.query(`CREATE TABLE IF NOT EXISTS email_verifications (
    token VARCHAR(64) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    code_hash CHAR(64) NOT NULL,
    expires_at BIGINT NOT NULL,
    used TINYINT(1) NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    username VARCHAR(80) NOT NULL,
    password VARCHAR(255) NOT NULL,
    firstname VARCHAR(80) NOT NULL,
    lastname VARCHAR(80) NOT NULL,
    phone VARCHAR(40) NULL,
    address1 VARCHAR(255) NULL,
    city VARCHAR(100) NULL,
    state VARCHAR(100) NULL,
    postal_code VARCHAR(32) NULL,
    country CHAR(2) NULL,
    signup_ip VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`);
  // Ensure email_verifications.password is large enough for bcrypt hashes and attempts column exists
  try {
    const [pwdCol] = await pool.query(
      "SELECT CHARACTER_MAXIMUM_LENGTH AS len FROM information_schema.columns WHERE table_schema=? AND table_name='email_verifications' AND column_name='password' LIMIT 1",
      [dbName]
    );
    const col = pwdCol && pwdCol[0];
    if (!col || (col.len != null && col.len < 60)) {
      try {
        await pool.query('ALTER TABLE email_verifications MODIFY COLUMN password VARCHAR(255) NOT NULL');
        if (DEBUG) console.log('[schema] Upgraded email_verifications.password to VARCHAR(255) for bcrypt hashes');
      } catch (e) {
        if (DEBUG) console.warn('[schema] Failed to alter email_verifications.password length:', e.message || e);
      }
    }
    // Ensure attempts column exists for per-token OTP attempt tracking
    try {
      const [attemptCol] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='email_verifications' AND column_name='attempts' LIMIT 1",
        [dbName]
      );
      if (!attemptCol || !attemptCol.length) {
        await pool.query('ALTER TABLE email_verifications ADD COLUMN attempts INT NOT NULL DEFAULT 0 AFTER used');
        if (DEBUG) console.log('[schema] Added email_verifications.attempts column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] email_verifications.attempts check failed', e.message || e);
    }

    // Ensure address + signup IP columns exist
    for (const colDef of [
      { name: 'address1', ddl: "ALTER TABLE email_verifications ADD COLUMN address1 VARCHAR(255) NULL AFTER phone" },
      { name: 'city', ddl: "ALTER TABLE email_verifications ADD COLUMN city VARCHAR(100) NULL AFTER address1" },
      { name: 'state', ddl: "ALTER TABLE email_verifications ADD COLUMN state VARCHAR(100) NULL AFTER city" },
      { name: 'postal_code', ddl: "ALTER TABLE email_verifications ADD COLUMN postal_code VARCHAR(32) NULL AFTER state" },
      { name: 'country', ddl: "ALTER TABLE email_verifications ADD COLUMN country CHAR(2) NULL AFTER postal_code" },
      { name: 'signup_ip', ddl: "ALTER TABLE email_verifications ADD COLUMN signup_ip VARCHAR(64) NULL AFTER country" }
    ]) {
      try {
        const [hasCol] = await pool.query(
          'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'email_verifications\' AND column_name=? LIMIT 1',
          [dbName, colDef.name]
        );
        if (!hasCol || !hasCol.length) {
          await pool.query(colDef.ddl);
          if (DEBUG) console.log('[schema] Added email_verifications column:', colDef.name);
        }
      } catch (e) {
        if (DEBUG) console.warn('[schema] email_verifications column check failed:', colDef.name, e.message || e);
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] email_verifications.password length check failed', e.message || e);
  }
  await pool.query(`CREATE TABLE IF NOT EXISTS signup_users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(80) NOT NULL,
    email VARCHAR(255) NOT NULL,
    firstname VARCHAR(80) NULL,
    lastname VARCHAR(80) NULL,
    phone VARCHAR(40) NULL,
    address1 VARCHAR(255) NULL,
    city VARCHAR(100) NULL,
    state VARCHAR(100) NULL,
    postal_code VARCHAR(32) NULL,
    country CHAR(2) NULL,
    signup_ip VARCHAR(64) NULL,
    password_hash VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_username (username),
    UNIQUE KEY uniq_email (email)
  )`);

  // Password reset tokens table
  await pool.query(`CREATE TABLE IF NOT EXISTS password_resets (
    token VARCHAR(64) PRIMARY KEY,
    user_id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    code_hash CHAR(64) NOT NULL,
    expires_at BIGINT NOT NULL,
    used TINYINT(1) NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_user_id (user_id),
    KEY idx_email (email),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Ensure signup_users address + signup IP columns exist
  for (const colDef of [
    { name: 'address1', ddl: "ALTER TABLE signup_users ADD COLUMN address1 VARCHAR(255) NULL AFTER phone" },
    { name: 'city', ddl: "ALTER TABLE signup_users ADD COLUMN city VARCHAR(100) NULL AFTER address1" },
    { name: 'state', ddl: "ALTER TABLE signup_users ADD COLUMN state VARCHAR(100) NULL AFTER city" },
    { name: 'postal_code', ddl: "ALTER TABLE signup_users ADD COLUMN postal_code VARCHAR(32) NULL AFTER state" },
    { name: 'country', ddl: "ALTER TABLE signup_users ADD COLUMN country CHAR(2) NULL AFTER postal_code" },
    { name: 'signup_ip', ddl: "ALTER TABLE signup_users ADD COLUMN signup_ip VARCHAR(64) NULL AFTER country" }
  ]) {
    try {
      const [hasCol] = await pool.query(
        'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'signup_users\' AND column_name=? LIMIT 1',
        [dbName, colDef.name]
      );
      if (!hasCol || !hasCol.length) {
        await pool.query(colDef.ddl);
        if (DEBUG) console.log('[schema] Added signup_users column:', colDef.name);
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] signup_users column check failed:', colDef.name, e.message || e);
    }
  }

  // User trunks table - stores trunk ownership per user
  await pool.query(`CREATE TABLE IF NOT EXISTS user_trunks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    provider_trunk_id VARCHAR(64) NOT NULL,
    kind VARCHAR(16) NOT NULL DEFAULT 'pstn',
    name VARCHAR(255) NOT NULL,
    dst VARCHAR(32) NULL,
    sip_username VARCHAR(255) NULL,
    sip_host VARCHAR(255) NULL,
    sip_port INT NULL,
    transport_protocol_id INT NULL,
    description TEXT NULL,
    capacity_limit INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_trunk (provider_trunk_id),
    KEY idx_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Ensure user_trunks columns exist even if table was created on an older schema
  for (const colDef of [
    { name: 'kind', ddl: "ALTER TABLE user_trunks ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'pstn' AFTER provider_trunk_id" },
    { name: 'sip_username', ddl: "ALTER TABLE user_trunks ADD COLUMN sip_username VARCHAR(255) NULL AFTER dst" },
    { name: 'sip_host', ddl: "ALTER TABLE user_trunks ADD COLUMN sip_host VARCHAR(255) NULL AFTER sip_username" },
    { name: 'sip_port', ddl: "ALTER TABLE user_trunks ADD COLUMN sip_port INT NULL AFTER sip_host" },
    { name: 'transport_protocol_id', ddl: "ALTER TABLE user_trunks ADD COLUMN transport_protocol_id INT NULL AFTER sip_port" }
  ]) {
    try {
      const [hasCol] = await pool.query(
        'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'user_trunks\' AND column_name=? LIMIT 1',
        [dbName, colDef.name]
      );
      if (!hasCol || !hasCol.length) {
        await pool.query(colDef.ddl);
        if (DEBUG) console.log('[schema] Added user_trunks column:', colDef.name);
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] user_trunks column check failed:', colDef.name, e.message || e);
    }
  }

  // User DIDs table - stores purchased phone numbers per user
  await pool.query(`CREATE TABLE IF NOT EXISTS user_dids (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    did_id VARCHAR(64) NOT NULL,
    did_number VARCHAR(32) NOT NULL,
    country VARCHAR(64) NULL,
    region VARCHAR(64) NULL,
    city VARCHAR(64) NULL,
    did_type VARCHAR(32) NULL,
    monthly_price DECIMAL(10,4) NULL,
    setup_price DECIMAL(10,4) NULL,
    trunk_id VARCHAR(64) NULL,
    cancel_pending TINYINT(1) NOT NULL DEFAULT 0,
    cancel_pending_since DATETIME NULL,
    cancel_after DATETIME NULL,
    cancel_billed_to DATETIME NULL,
    cancel_notice_initial_sent_at DATETIME NULL,
    cancel_notice_reminder_sent_at DATETIME NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_did (did_id),
    KEY idx_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // Ensure user_dids.trunk_id exists even if table was created on an older schema
  try {
    const [trunkCol] = await pool.query(
      "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='user_dids' AND column_name='trunk_id' LIMIT 1",
      [dbName]
    );
    if (!trunkCol || !trunkCol.length) {
      await pool.query("ALTER TABLE user_dids ADD COLUMN trunk_id VARCHAR(64) NULL");
      if (DEBUG) console.log('[schema] Added user_dids.trunk_id column');
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] user_dids.trunk_id check failed', e.message || e);
  }

  // Ensure user_dids.cancel_pending exists (used for nonpayment cancellations)
  try {
    const [cancelCol] = await pool.query(
      "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='user_dids' AND column_name='cancel_pending' LIMIT 1",
      [dbName]
    );
    if (!cancelCol || !cancelCol.length) {
      await pool.query("ALTER TABLE user_dids ADD COLUMN cancel_pending TINYINT(1) NOT NULL DEFAULT 0 AFTER trunk_id");
      if (DEBUG) console.log('[schema] Added user_dids.cancel_pending column');
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] user_dids.cancel_pending check failed', e.message || e);
  }

  // Ensure user_dids nonpayment/grace columns exist
  for (const col of [
    { name: 'cancel_pending_since', ddl: "ALTER TABLE user_dids ADD COLUMN cancel_pending_since DATETIME NULL AFTER cancel_pending" },
    { name: 'cancel_after', ddl: "ALTER TABLE user_dids ADD COLUMN cancel_after DATETIME NULL AFTER cancel_pending_since" },
    { name: 'cancel_billed_to', ddl: "ALTER TABLE user_dids ADD COLUMN cancel_billed_to DATETIME NULL AFTER cancel_after" },
    { name: 'cancel_notice_initial_sent_at', ddl: "ALTER TABLE user_dids ADD COLUMN cancel_notice_initial_sent_at DATETIME NULL AFTER cancel_billed_to" },
    { name: 'cancel_notice_reminder_sent_at', ddl: "ALTER TABLE user_dids ADD COLUMN cancel_notice_reminder_sent_at DATETIME NULL AFTER cancel_notice_initial_sent_at" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='user_dids' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added user_dids.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] user_dids.${col.name} check failed`, e.message || e);
    }
  }

  // Phone.System (telecom.center) mappings
  await pool.query(`CREATE TABLE IF NOT EXISTS phonesystem_customers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tc_customer_id VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_tc_customer (user_id, tc_customer_id),
    KEY idx_user_id (user_id),
    CONSTRAINT fk_phonesystem_customers_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS phonesystem_incoming_trunks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tc_customer_id VARCHAR(64) NOT NULL,
    tc_incoming_trunk_id VARCHAR(64) NOT NULL,
    domain VARCHAR(255) NULL,
    destination_field VARCHAR(64) NULL,
    forwarding_trunk_id VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_tc_incoming_trunk (user_id, tc_incoming_trunk_id),
    KEY idx_user_customer (user_id, tc_customer_id),
    CONSTRAINT fk_phonesystem_incoming_trunks_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS phonesystem_dids (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tc_customer_id VARCHAR(64) NOT NULL,
    tc_available_did_number_id VARCHAR(64) NOT NULL,
    did_id VARCHAR(64) NOT NULL,
    did_number VARCHAR(32) NOT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ps_user_did (user_id, did_id),
    UNIQUE KEY uniq_user_tc_did (user_id, tc_available_did_number_id),
    KEY idx_user_customer (user_id, tc_customer_id),
    CONSTRAINT fk_phonesystem_dids_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS phonesystem_termination_gateways (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tc_customer_id VARCHAR(64) NOT NULL,
    tc_termination_gateway_id VARCHAR(64) NOT NULL,
    host VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_tc_gateway (user_id, tc_termination_gateway_id),
    KEY idx_user_customer (user_id, tc_customer_id),
    CONSTRAINT fk_phonesystem_term_gateways_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS phonesystem_termination_routes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tc_customer_id VARCHAR(64) NOT NULL,
    tc_termination_route_id VARCHAR(64) NOT NULL,
    name VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_tc_route (user_id, tc_termination_route_id),
    KEY idx_user_customer (user_id, tc_customer_id),
    CONSTRAINT fk_phonesystem_term_routes_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // DID markup tracking table - stores last billed cycle per DID per user (for reporting/debugging)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_did_markups (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    did_id VARCHAR(64) NOT NULL,
    last_billed_to DATETIME NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_did (user_id, did_id),
    KEY idx_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // DID markup cycles table - enforces at-most-once billing per (user, DID, billing period)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_did_markup_cycles (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    did_id VARCHAR(64) NOT NULL,
    billed_to DATETIME NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_did_cycle (user_id, did_id, billed_to),
    KEY idx_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // Per-DID CDR history table - stores inbound CDRs per user/DID
  await pool.query(`CREATE TABLE IF NOT EXISTS user_did_cdrs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cdr_id VARCHAR(64) NOT NULL,
    user_id BIGINT NULL,
    did_number VARCHAR(32) NOT NULL,
    direction VARCHAR(16) NOT NULL DEFAULT 'inbound',
    src_number VARCHAR(64) NULL,
    dst_number VARCHAR(64) NULL,
    time_start DATETIME NULL,
    time_connect DATETIME NULL,
    time_end DATETIME NULL,
    duration INT NULL,
    billsec INT NULL,
    price DECIMAL(18,8) NULL,
    raw_cdr JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_cdr_id (cdr_id),
    KEY idx_user_id (user_id),
    KEY idx_did_number (did_number),
    CONSTRAINT fk_user_did_cdrs_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE SET NULL
  )`);
  // Per-user outbound CDR history table - stores normalized billing system CDRs per user
  await pool.query(`CREATE TABLE IF NOT EXISTS user_mb_cdrs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cdr_id VARCHAR(64) NOT NULL,
    user_id BIGINT NOT NULL,
    direction VARCHAR(16) NOT NULL DEFAULT 'outbound',
    src_number VARCHAR(64) NULL,
    dst_number VARCHAR(64) NULL,
    did_number VARCHAR(32) NULL,
    time_start DATETIME NULL,
    duration INT NULL,
    billsec INT NULL,
    price DECIMAL(18,8) NULL,
    raw_cdr JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_mb_cdr (cdr_id),
    KEY idx_mb_user (user_id),
    KEY idx_mb_time_start (time_start),
    CONSTRAINT fk_user_mb_cdrs_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // Ensure uniq_user_did_cycle index exists even if table was created on an older schema
  try {
    const [cycleIdx] = await pool.query(
      "SELECT 1 FROM information_schema.statistics WHERE table_schema=? AND table_name='user_did_markup_cycles' AND index_name='uniq_user_did_cycle' LIMIT 1",
      [dbName]
    );
    if (!cycleIdx || !cycleIdx.length) {
      try {
        await pool.query('ALTER TABLE user_did_markup_cycles ADD UNIQUE KEY uniq_user_did_cycle (user_id, did_id, billed_to)');
      } catch (e) {
        if (DEBUG) console.warn('[schema] Failed to add uniq_user_did_cycle index:', e.message || e);
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] Failed to verify uniq_user_did_cycle index:', e.message || e);
  }
  // Billing history table - records user refills/credits
  await pool.query(`CREATE TABLE IF NOT EXISTS billing_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount DECIMAL(18,8) NOT NULL,
    description VARCHAR(255) NULL,
    status ENUM('pending', 'completed', 'failed') NOT NULL DEFAULT 'completed',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_billing_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // Pending orders table - tracks DID orders that have not completed yet
  await pool.query(`CREATE TABLE IF NOT EXISTS pending_orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    reconciled TINYINT(1) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_order (order_id),
    KEY idx_pending_user (user_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // DID purchase receipts table - enforces at-most-once customer receipt per (user, order)
  await pool.query(`CREATE TABLE IF NOT EXISTS did_purchase_receipts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_order (user_id, order_id),
    KEY idx_receipt_user (user_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // NOWPayments payments table - tracks crypto payments and credits
  await pool.query(`CREATE TABLE IF NOT EXISTS nowpayments_payments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    payment_id VARCHAR(128) NOT NULL,
    order_id VARCHAR(191) NOT NULL,
    price_amount DECIMAL(18,8) NOT NULL,
    price_currency VARCHAR(16) NOT NULL DEFAULT 'usd',
    pay_amount DECIMAL(36,18) NULL,
    pay_currency VARCHAR(32) NULL,
    payment_status VARCHAR(64) NOT NULL DEFAULT 'pending',
    credited TINYINT(1) NOT NULL DEFAULT 0,
    raw_payload JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_payment_id (payment_id),
    KEY idx_np_user (user_id),
    KEY idx_np_order (order_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);
  // Square payments table - tracks card payments (Square Payment Links) and credits
  await pool.query(`CREATE TABLE IF NOT EXISTS square_payments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    payment_link_id VARCHAR(128) NOT NULL,
    order_id VARCHAR(191) NOT NULL,
    square_order_id VARCHAR(128) NULL,
    square_payment_id VARCHAR(128) NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(16) NOT NULL DEFAULT 'USD',
    status VARCHAR(64) NOT NULL DEFAULT 'pending',
    credited TINYINT(1) NOT NULL DEFAULT 0,
    raw_payload JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_payment_link (payment_link_id),
    KEY idx_sq_user (user_id),
    KEY idx_sq_order (square_order_id),
    KEY idx_sq_local_order (order_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Stripe payments table - tracks card payments (Stripe Checkout) and credits
  await pool.query(`CREATE TABLE IF NOT EXISTS stripe_payments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    order_id VARCHAR(191) NOT NULL,
    payment_intent_id VARCHAR(191) NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(16) NOT NULL DEFAULT 'USD',
    status VARCHAR(64) NOT NULL DEFAULT 'pending',
    credited TINYINT(1) NOT NULL DEFAULT 0,
    raw_payload JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_stripe_session (session_id),
    KEY idx_stripe_user (user_id),
    KEY idx_stripe_order (order_id),
    KEY idx_stripe_payment_intent (payment_intent_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Bill.com / BILL payments table
  await pool.query(`CREATE TABLE IF NOT EXISTS billcom_payments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    invoice_id VARCHAR(128) NOT NULL,
    customer_id VARCHAR(128) NULL,
    order_id VARCHAR(191) NOT NULL,
    payment_link VARCHAR(2048) NULL,
    status VARCHAR(64) NOT NULL DEFAULT 'pending',
    credited TINYINT(1) NOT NULL DEFAULT 0,
    raw_payload JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_billcom_invoice (invoice_id),
    KEY idx_billcom_user (user_id),
    KEY idx_billcom_order (order_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);


  // --- Helper: upgrade a table's id column from INT to BIGINT (handles FK cascade) ---
  async function ensureBigintPK(tableName) {
    try {
      const [cols] = await pool.query(
        "SELECT COLUMN_TYPE FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name='id' LIMIT 1",
        [dbName, tableName]
      );
      if (!cols || !cols[0]) return;
      const colType = String(cols[0].COLUMN_TYPE || '').toLowerCase().trim();
      if (colType === 'bigint') return; // Already signed BIGINT — nothing to do
      if (DEBUG) console.log(`[schema] Upgrading ${tableName}.id (${colType}) -> BIGINT ...`);

      // Drop incoming FKs (other tables referencing this table's id)
      const [inFks] = await pool.query(
        `SELECT kcu.TABLE_NAME   AS tbl,
                kcu.COLUMN_NAME  AS col,
                kcu.CONSTRAINT_NAME AS fk,
                rc.DELETE_RULE   AS onDel,
                c.IS_NULLABLE    AS nullable
         FROM information_schema.KEY_COLUMN_USAGE kcu
         JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
           ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         JOIN information_schema.COLUMNS c
           ON c.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
         WHERE kcu.REFERENCED_TABLE_SCHEMA = ?
           AND kcu.REFERENCED_TABLE_NAME = ?
           AND kcu.REFERENCED_COLUMN_NAME = 'id'`,
        [dbName, tableName]
      );
      for (const fk of inFks || []) {
        try { await pool.query(`ALTER TABLE \`${fk.tbl}\` DROP FOREIGN KEY \`${fk.fk}\``); } catch {}
      }

      // Drop outgoing FKs (this table referencing other tables) — must drop to unblock ALTER
      const [outFks] = await pool.query(
        `SELECT kcu.COLUMN_NAME AS col, kcu.CONSTRAINT_NAME AS fk,
                kcu.REFERENCED_TABLE_NAME AS refTbl, kcu.REFERENCED_COLUMN_NAME AS refCol,
                rc.DELETE_RULE AS onDel, c.IS_NULLABLE AS nullable
         FROM information_schema.KEY_COLUMN_USAGE kcu
         JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
           ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         JOIN information_schema.COLUMNS c
           ON c.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
         WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL`,
        [dbName, tableName]
      );
      for (const fk of outFks || []) {
        try { await pool.query(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${fk.fk}\``); } catch {}
      }

      // Widen the PK
      await pool.query(`ALTER TABLE \`${tableName}\` MODIFY COLUMN id BIGINT AUTO_INCREMENT`);

      // Re-add incoming FKs (widen FK columns first)
      for (const fk of inFks || []) {
        const nullPart = fk.nullable === 'YES' ? 'NULL' : 'NOT NULL';
        const delAction = (fk.onDel || 'CASCADE').replace(/_/g, ' ');
        try {
          await pool.query(`ALTER TABLE \`${fk.tbl}\` MODIFY COLUMN \`${fk.col}\` BIGINT ${nullPart}`);
          await pool.query(`ALTER TABLE \`${fk.tbl}\` ADD CONSTRAINT \`${fk.fk}\` FOREIGN KEY (\`${fk.col}\`) REFERENCES \`${tableName}\`(id) ON DELETE ${delAction}`);
        } catch {}
      }

      // Re-add outgoing FKs (widen outgoing FK columns to BIGINT if still INT)
      for (const fk of outFks || []) {
        const nullPart = fk.nullable === 'YES' ? 'NULL' : 'NOT NULL';
        const delAction = (fk.onDel || 'CASCADE').replace(/_/g, ' ');
        try {
          const [fkCols] = await pool.query(
            "SELECT DATA_TYPE FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name=? LIMIT 1",
            [dbName, tableName, fk.col]
          );
          if (fkCols && fkCols[0] && fkCols[0].DATA_TYPE === 'int') {
            await pool.query(`ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${fk.col}\` BIGINT ${nullPart}`);
          }
          await pool.query(`ALTER TABLE \`${tableName}\` ADD CONSTRAINT \`${fk.fk}\` FOREIGN KEY (\`${fk.col}\`) REFERENCES \`${fk.refTbl}\`(\`${fk.refCol}\`) ON DELETE ${delAction}`);
        } catch {}
      }

      if (DEBUG) console.log(`[schema] ${tableName}.id upgraded to BIGINT`);
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ${tableName}.id BIGINT migration failed:`, e.message || e);
    }
  }

  // AI Agents (Pipecat Cloud)
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_agents (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    display_name VARCHAR(120) NOT NULL,
    greeting TEXT NULL,
    prompt TEXT NULL,
    cartesia_voice_id VARCHAR(191) NULL,
    background_audio_url VARCHAR(512) NULL,
    background_audio_gain DECIMAL(4,3) NULL,
    pipecat_agent_name VARCHAR(191) NOT NULL,
    pipecat_secret_set VARCHAR(191) NOT NULL,
    pipecat_region VARCHAR(64) NULL,
    agent_action_token_hash CHAR(64) NULL,
    agent_action_token_enc BLOB NULL,
    agent_action_token_iv VARBINARY(16) NULL,
    agent_action_token_tag VARBINARY(16) NULL,
    transfer_to_number VARCHAR(64) NULL,
    default_doc_template_id BIGINT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_pipecat_agent (pipecat_agent_name),
    KEY idx_ai_agents_user (user_id),
    KEY idx_ai_agents_default_template (default_doc_template_id),
    FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  await ensureBigintPK('ai_agents');

  // Per-agent uploaded background audio (WAV), served via a tokenized public URL.
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_agent_background_audio (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    agent_id BIGINT NOT NULL,
    original_filename VARCHAR(255) NULL,
    mime_type VARCHAR(128) NULL,
    size_bytes BIGINT NULL,
    access_token CHAR(64) NOT NULL,
    wav_blob LONGBLOB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_agent_bg_audio_agent (agent_id),
    KEY idx_ai_agent_bg_audio_user (user_id),
    CONSTRAINT fk_ai_agent_bg_audio_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_agent_bg_audio_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE CASCADE
  )`);

  // AI Phone Numbers (Daily Telephony)
  // One number can be assigned to at most one agent, and an agent can have at most one number.
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_numbers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    daily_number_id VARCHAR(191) NOT NULL,
    phone_number VARCHAR(64) NOT NULL,
    agent_id BIGINT NULL,
    dialin_config_id VARCHAR(191) NULL,
    cancel_pending TINYINT(1) NOT NULL DEFAULT 0,
    cancel_pending_since DATETIME NULL,
    cancel_after DATETIME NULL,
    cancel_billed_to DATETIME NULL,
    cancel_notice_initial_sent_at DATETIME NULL,
    cancel_notice_reminder_sent_at DATETIME NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_daily_number (daily_number_id),
    UNIQUE KEY uniq_phone_number (phone_number),
    UNIQUE KEY uniq_agent_number (agent_id),
    KEY idx_ai_numbers_user (user_id),
    CONSTRAINT fk_ai_numbers_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_numbers_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL
  )`);

  await ensureBigintPK('ai_numbers');

  // Ensure ai_numbers.daily_number_id exists (added after initial DDL)
  try {
    const [rows] = await pool.query(
      "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_numbers' AND column_name='daily_number_id' LIMIT 1",
      [dbName]
    );
    if (!rows || !rows.length) {
      await pool.query("ALTER TABLE ai_numbers ADD COLUMN daily_number_id VARCHAR(191) NOT NULL DEFAULT ''");
      try { await pool.query("ALTER TABLE ai_numbers ADD UNIQUE KEY uniq_daily_number (daily_number_id)"); } catch {}
      if (DEBUG) console.log('[schema] Added ai_numbers.daily_number_id column + UNIQUE KEY');
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] ai_numbers.daily_number_id check failed', e.message || e);
  }

  // Ensure ai_numbers.agent_id exists (added after initial DDL)
  try {
    const [rows] = await pool.query(
      "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_numbers' AND column_name='agent_id' LIMIT 1",
      [dbName]
    );
    if (!rows || !rows.length) {
      await pool.query("ALTER TABLE ai_numbers ADD COLUMN agent_id BIGINT NULL");
      try { await pool.query("ALTER TABLE ai_numbers ADD UNIQUE KEY uniq_agent_number (agent_id)"); } catch {}
      try { await pool.query("ALTER TABLE ai_numbers ADD CONSTRAINT fk_ai_numbers_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL"); } catch {}
      if (DEBUG) console.log('[schema] Added ai_numbers.agent_id column + FK');
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] ai_numbers.agent_id check failed', e.message || e);
  }

  // Ensure ai_numbers.dialin_config_id exists (needed before cancel_* AFTER references)
  try {
    const [rows] = await pool.query(
      "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_numbers' AND column_name='dialin_config_id' LIMIT 1",
      [dbName]
    );
    if (!rows || !rows.length) {
      await pool.query("ALTER TABLE ai_numbers ADD COLUMN dialin_config_id VARCHAR(191) NULL");
      if (DEBUG) console.log('[schema] Added ai_numbers.dialin_config_id column');
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] ai_numbers.dialin_config_id check failed', e.message || e);
  }

  // Ensure ai_numbers nonpayment/grace columns exist
  for (const col of [
    { name: 'cancel_pending', ddl: "ALTER TABLE ai_numbers ADD COLUMN cancel_pending TINYINT(1) NOT NULL DEFAULT 0" },
    { name: 'cancel_pending_since', ddl: "ALTER TABLE ai_numbers ADD COLUMN cancel_pending_since DATETIME NULL" },
    { name: 'cancel_after', ddl: "ALTER TABLE ai_numbers ADD COLUMN cancel_after DATETIME NULL" },
    { name: 'cancel_billed_to', ddl: "ALTER TABLE ai_numbers ADD COLUMN cancel_billed_to DATETIME NULL" },
    { name: 'cancel_notice_initial_sent_at', ddl: "ALTER TABLE ai_numbers ADD COLUMN cancel_notice_initial_sent_at DATETIME NULL" },
    { name: 'cancel_notice_reminder_sent_at', ddl: "ALTER TABLE ai_numbers ADD COLUMN cancel_notice_reminder_sent_at DATETIME NULL" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_numbers' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_numbers.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_numbers.${col.name} check failed`, e.message || e);
    }
  }

  // AI number monthly billing tracking
  // Uses a cycle table (idempotency) similar to DID markup billing.
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_number_markups (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    ai_number_id BIGINT NOT NULL,
    last_billed_to DATETIME NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_ai_number (user_id, ai_number_id),
    KEY idx_ai_number_markups_user (user_id),
    CONSTRAINT fk_ai_number_markups_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_number_markups_number FOREIGN KEY (ai_number_id) REFERENCES ai_numbers(id) ON DELETE CASCADE
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS ai_number_markup_cycles (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    ai_number_id BIGINT NOT NULL,
    billed_to DATETIME NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_number_cycle (user_id, ai_number_id, billed_to),
    KEY idx_ai_number_cycles_user (user_id),
    CONSTRAINT fk_ai_number_cycles_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_number_cycles_number FOREIGN KEY (ai_number_id) REFERENCES ai_numbers(id) ON DELETE CASCADE
  )`);

  // Per-user SMTP settings (used by AI agent document sending)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_smtp_settings (
    user_id BIGINT NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INT NOT NULL,
    secure TINYINT(1) NOT NULL DEFAULT 0,
    username VARCHAR(255) NULL,
    password_enc BLOB NULL,
    password_iv VARBINARY(16) NULL,
    password_tag VARBINARY(16) NULL,
    from_email VARCHAR(255) NOT NULL,
    from_name VARCHAR(255) NULL,
    reply_to VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    CONSTRAINT fk_user_smtp_settings_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Per-user AI call transfer settings (used by AI agent cold transfers)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_ai_transfer_settings (
    user_id BIGINT NOT NULL,
    transfer_to_number VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    CONSTRAINT fk_user_ai_transfer_settings_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Per-user AI SMS settings (outbound sender numbers)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_ai_sms_settings (
    user_id BIGINT NOT NULL,
    allowed_did_ids JSON NULL,
    default_did_id VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    CONSTRAINT fk_user_ai_sms_settings_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Per-user physical mail return address (Click2Mail)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_mail_settings (
    user_id BIGINT NOT NULL,
    name VARCHAR(255) NULL,
    organization VARCHAR(255) NULL,
    address1 VARCHAR(255) NOT NULL,
    address2 VARCHAR(255) NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(64) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(2) NOT NULL DEFAULT 'US',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    CONSTRAINT fk_user_mail_settings_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Per-user AI payment settings (Square/Stripe for invoicing callers)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_ai_payment_settings (
    user_id BIGINT NOT NULL,
    square_access_token_enc BLOB NULL,
    square_access_token_iv VARBINARY(16) NULL,
    square_access_token_tag VARBINARY(16) NULL,
    square_location_id VARCHAR(64) NULL,
    square_environment VARCHAR(16) NOT NULL DEFAULT 'production',
    stripe_secret_key_enc BLOB NULL,
    stripe_secret_key_iv VARBINARY(16) NULL,
    stripe_secret_key_tag VARBINARY(16) NULL,
    preferred_provider VARCHAR(16) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    CONSTRAINT fk_user_ai_payment_settings_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Payment requests tracking (for webhook status updates)
  await pool.query(`CREATE TABLE IF NOT EXISTS user_payment_requests (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    provider VARCHAR(16) NOT NULL,
    provider_payment_id VARCHAR(128) NULL,
    provider_checkout_id VARCHAR(128) NULL,
    amount_cents BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    description VARCHAR(512) NULL,
    customer_email VARCHAR(255) NULL,
    customer_phone VARCHAR(32) NULL,
    payment_url TEXT NULL,
    status ENUM('pending','completed','failed','expired','cancelled') NOT NULL DEFAULT 'pending',
    call_id VARCHAR(128) NULL,
    call_domain VARCHAR(128) NULL,
    metadata JSON NULL,
    paid_at DATETIME NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_payment_requests_user (user_id),
    KEY idx_payment_requests_provider (provider, provider_payment_id),
    KEY idx_payment_requests_checkout (provider, provider_checkout_id),
    KEY idx_payment_requests_status (user_id, status),
    CONSTRAINT fk_payment_requests_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // AI document templates
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_doc_templates (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name VARCHAR(191) NOT NULL,
    original_filename VARCHAR(255) NULL,
    mime_type VARCHAR(128) NULL,
    size_bytes BIGINT NULL,
    doc_blob LONGBLOB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_doc_template (user_id, name),
    KEY idx_ai_doc_templates_user (user_id),
    CONSTRAINT fk_ai_doc_templates_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  // Outbound dialer tables
  await pool.query(`CREATE TABLE IF NOT EXISTS dialer_campaigns (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name VARCHAR(191) NOT NULL,
    ai_agent_id BIGINT NOT NULL,
    ai_agent_name VARCHAR(191) NULL,
    status ENUM('draft','running','paused','completed','deleted') NOT NULL DEFAULT 'draft',
    concurrency_limit INT NOT NULL DEFAULT 1,
    last_started_at DATETIME NULL,
    last_paused_at DATETIME NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_dialer_campaigns_user (user_id),
    KEY idx_dialer_campaigns_agent (ai_agent_id),
    CONSTRAINT fk_dialer_campaigns_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_dialer_campaigns_agent FOREIGN KEY (ai_agent_id) REFERENCES ai_agents(id) ON DELETE CASCADE
  )`);

  // Ensure dialer_campaigns columns exist (added after initial DDL)
  // Uses direct ALTER TABLE with duplicate-column error suppression for reliability.
  for (const col of [
    { name: 'ai_agent_name', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN ai_agent_name VARCHAR(191) NULL" },
    { name: 'concurrency_limit', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN concurrency_limit INT NOT NULL DEFAULT 1" },
    { name: 'last_started_at', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN last_started_at DATETIME NULL" },
    { name: 'last_paused_at', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN last_paused_at DATETIME NULL" },
    { name: 'campaign_audio_blob', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN campaign_audio_blob LONGBLOB NULL" },
    { name: 'campaign_audio_filename', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN campaign_audio_filename VARCHAR(255) NULL" },
    { name: 'campaign_audio_mime', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN campaign_audio_mime VARCHAR(128) NULL" },
    { name: 'campaign_audio_size', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN campaign_audio_size BIGINT NULL" },
    { name: 'updated_at', ddl: "ALTER TABLE dialer_campaigns ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" }
  ]) {
    try {
      await pool.query(col.ddl);
      console.log(`[schema] Added dialer_campaigns.${col.name} column`);
    } catch (e) {
      // ER_DUP_FIELDNAME (1060) = column already exists; safe to ignore.
      if (e && e.errno === 1060) continue;
      console.warn(`[schema] dialer_campaigns.${col.name} migration failed:`, e.message || e);
    }
  }

  // Drop deprecated sip_account columns (MagnusBilling remnant)
  for (const { table, col } of [
    { table: 'dialer_campaigns', col: 'sip_account_id' },
    { table: 'dialer_campaigns', col: 'sip_account_label' },
    { table: 'dialer_call_logs', col: 'sip_account_id' },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name=? LIMIT 1",
        [dbName, table, col]
      );
      if (rows && rows.length) {
        await pool.query(`ALTER TABLE \`${table}\` DROP COLUMN \`${col}\``);
        if (DEBUG) console.log(`[schema] Dropped deprecated ${table}.${col} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ${table}.${col} drop failed`, e.message || e);
    }
  }

  await ensureBigintPK('dialer_campaigns');

  await pool.query(`CREATE TABLE IF NOT EXISTS dialer_leads (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    phone_number VARCHAR(32) NOT NULL,
    lead_name VARCHAR(191) NULL,
    metadata JSON NULL,
    status ENUM('pending','queued','dialing','answered','voicemail','transferred','failed','completed') NOT NULL DEFAULT 'pending',
    last_call_at DATETIME NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_campaign_phone (campaign_id, phone_number),
    KEY idx_dialer_leads_campaign (campaign_id),
    KEY idx_dialer_leads_user (user_id),
    KEY idx_dialer_leads_status (status),
    CONSTRAINT fk_dialer_leads_campaign FOREIGN KEY (campaign_id) REFERENCES dialer_campaigns(id) ON DELETE CASCADE,
    CONSTRAINT fk_dialer_leads_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE
  )`);

  await ensureBigintPK('dialer_leads');

  // Ensure dialer_leads columns exist (schema may pre-date current DDL)
  for (const col of [
    { name: 'user_id', ddl: 'ALTER TABLE dialer_leads ADD COLUMN user_id BIGINT NULL' },
    { name: 'lead_name', ddl: 'ALTER TABLE dialer_leads ADD COLUMN lead_name VARCHAR(191) NULL' },
    { name: 'metadata', ddl: 'ALTER TABLE dialer_leads ADD COLUMN metadata JSON NULL' },
    { name: 'attempt_count', ddl: 'ALTER TABLE dialer_leads ADD COLUMN attempt_count INT NOT NULL DEFAULT 0' },
    { name: 'last_call_at', ddl: 'ALTER TABLE dialer_leads ADD COLUMN last_call_at DATETIME NULL' }
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='dialer_leads' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added dialer_leads.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] dialer_leads.${col.name} check failed`, e.message || e);
    }
  }

  await pool.query(`CREATE TABLE IF NOT EXISTS dialer_call_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    lead_id BIGINT NULL,
    user_id BIGINT NOT NULL,
    ai_agent_id BIGINT NULL,
    call_id VARCHAR(128) NULL,
    status VARCHAR(32) NULL,
    result VARCHAR(32) NULL,
    duration_sec INT NULL,
    price DECIMAL(18,8) NULL,
    billed TINYINT(1) NOT NULL DEFAULT 0,
    billing_history_id BIGINT NULL,
    transferred_to VARCHAR(64) NULL,
    recording_url TEXT NULL,
    notes TEXT NULL,
    metadata JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_dialer_logs_campaign (campaign_id),
    KEY idx_dialer_logs_lead (lead_id),
    KEY idx_dialer_logs_user (user_id),
    KEY idx_dialer_logs_agent (ai_agent_id),
    KEY idx_dialer_logs_billed (user_id, billed),
    CONSTRAINT fk_dialer_logs_campaign FOREIGN KEY (campaign_id) REFERENCES dialer_campaigns(id) ON DELETE CASCADE,
    CONSTRAINT fk_dialer_logs_lead FOREIGN KEY (lead_id) REFERENCES dialer_leads(id) ON DELETE SET NULL,
    CONSTRAINT fk_dialer_logs_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_dialer_logs_agent FOREIGN KEY (ai_agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL
  )`);

  // Ensure dialer_call_logs billing columns exist (price + billed markers)
  for (const col of [
    { name: 'price', ddl: 'ALTER TABLE dialer_call_logs ADD COLUMN price DECIMAL(18,8) NULL' },
    { name: 'billed', ddl: 'ALTER TABLE dialer_call_logs ADD COLUMN billed TINYINT(1) NOT NULL DEFAULT 0' },
    { name: 'billing_history_id', ddl: 'ALTER TABLE dialer_call_logs ADD COLUMN billing_history_id BIGINT NULL' }
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='dialer_call_logs' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added dialer_call_logs.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] dialer_call_logs.${col.name} check failed`, e.message || e);
    }
  }

  try {
    const [hasIdx] = await pool.query(
      "SELECT 1 FROM information_schema.statistics WHERE table_schema=? AND table_name='dialer_call_logs' AND index_name='idx_dialer_logs_billed' LIMIT 1",
      [dbName]
    );
    if (!hasIdx || !hasIdx.length) {
      try {
        await pool.query('ALTER TABLE dialer_call_logs ADD KEY idx_dialer_logs_billed (user_id, billed)');
        if (DEBUG) console.log('[schema] Added dialer_call_logs.idx_dialer_logs_billed index');
      } catch (e) {
        if (DEBUG) console.warn('[schema] Failed to add dialer_call_logs.idx_dialer_logs_billed index:', e.message || e);
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] dialer_call_logs.idx_dialer_logs_billed check failed', e.message || e);
  }

  // Voicemail audio & direct transfer feature migrations
  // 1. Make dialer_campaigns.ai_agent_id NULLABLE
  try {
    const [cols] = await pool.query(
      "SELECT IS_NULLABLE FROM information_schema.columns WHERE table_schema=? AND table_name='dialer_campaigns' AND column_name='ai_agent_id' LIMIT 1",
      [dbName]
    );
    if (cols && cols.length && cols[0].IS_NULLABLE === 'NO') {
      try {
        // Drop foreign key constraint first
        await pool.query('ALTER TABLE dialer_campaigns DROP FOREIGN KEY fk_dialer_campaigns_agent');
        // Modify column to nullable
        await pool.query('ALTER TABLE dialer_campaigns MODIFY COLUMN ai_agent_id BIGINT NULL');
        // Re-add foreign key constraint
        await pool.query('ALTER TABLE dialer_campaigns ADD CONSTRAINT fk_dialer_campaigns_agent FOREIGN KEY (ai_agent_id) REFERENCES ai_agents(id) ON DELETE CASCADE');
        if (DEBUG) console.log('[schema] Made dialer_campaigns.ai_agent_id NULLABLE');
      } catch (e) {
        if (DEBUG) console.warn('[schema] Failed to make dialer_campaigns.ai_agent_id NULLABLE:', e.message || e);
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[schema] dialer_campaigns.ai_agent_id nullable check failed', e.message || e);
  }

  // Voicemail fields removed - will be re-added when Pipecat supports it natively

  // 3. Ensure ai_agents columns exist (added after initial DDL — no AFTER clauses for safety)
  for (const col of [
    { name: 'display_name', ddl: "ALTER TABLE ai_agents ADD COLUMN display_name VARCHAR(120) NOT NULL DEFAULT ''" },
    { name: 'greeting', ddl: "ALTER TABLE ai_agents ADD COLUMN greeting TEXT NULL" },
    { name: 'prompt', ddl: "ALTER TABLE ai_agents ADD COLUMN prompt TEXT NULL" },
    { name: 'cartesia_voice_id', ddl: "ALTER TABLE ai_agents ADD COLUMN cartesia_voice_id VARCHAR(191) NULL" },
    { name: 'background_audio_url', ddl: "ALTER TABLE ai_agents ADD COLUMN background_audio_url VARCHAR(512) NULL" },
    { name: 'background_audio_gain', ddl: "ALTER TABLE ai_agents ADD COLUMN background_audio_gain DECIMAL(4,3) NULL" },
    { name: 'pipecat_agent_name', ddl: "ALTER TABLE ai_agents ADD COLUMN pipecat_agent_name VARCHAR(191) NOT NULL DEFAULT ''" },
    { name: 'pipecat_secret_set', ddl: "ALTER TABLE ai_agents ADD COLUMN pipecat_secret_set VARCHAR(191) NOT NULL DEFAULT ''" },
    { name: 'pipecat_region', ddl: "ALTER TABLE ai_agents ADD COLUMN pipecat_region VARCHAR(64) NULL" },
    { name: 'agent_action_token_hash', ddl: "ALTER TABLE ai_agents ADD COLUMN agent_action_token_hash CHAR(64) NULL" },
    { name: 'agent_action_token_enc', ddl: "ALTER TABLE ai_agents ADD COLUMN agent_action_token_enc BLOB NULL" },
    { name: 'agent_action_token_iv', ddl: "ALTER TABLE ai_agents ADD COLUMN agent_action_token_iv VARBINARY(16) NULL" },
    { name: 'agent_action_token_tag', ddl: "ALTER TABLE ai_agents ADD COLUMN agent_action_token_tag VARBINARY(16) NULL" },
    { name: 'transfer_to_number', ddl: "ALTER TABLE ai_agents ADD COLUMN transfer_to_number VARCHAR(64) NULL" },
    { name: 'default_doc_template_id', ddl: "ALTER TABLE ai_agents ADD COLUMN default_doc_template_id BIGINT NULL" },
    { name: 'inbound_transfer_enabled', ddl: "ALTER TABLE ai_agents ADD COLUMN inbound_transfer_enabled TINYINT(1) NOT NULL DEFAULT 0" },
    { name: 'inbound_transfer_number', ddl: "ALTER TABLE ai_agents ADD COLUMN inbound_transfer_number VARCHAR(32) NULL" }
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_agents.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_agents.${col.name} check failed`, e.message || e);
    }
  }

  await ensureBigintPK('ai_doc_templates');
  await ensureBigintPK('billing_history');

  // AI email send audit log + idempotency
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_email_sends (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    agent_id BIGINT NULL,
    template_id BIGINT NULL,
    dedupe_key CHAR(64) NOT NULL,
    call_id VARCHAR(64) NULL,
    call_domain VARCHAR(64) NULL,
    to_email VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NULL,
    email_text TEXT NULL,
    email_html MEDIUMTEXT NULL,
    attachments_json JSON NULL,
    smtp_message_id VARCHAR(255) NULL,
    meeting_provider VARCHAR(32) NULL,
    meeting_room_name VARCHAR(191) NULL,
    meeting_room_url TEXT NULL,
    meeting_join_url TEXT NULL,
    billing_history_id BIGINT NULL,
    refund_status ENUM('none','pending','completed','failed') NOT NULL DEFAULT 'none',
    refund_amount DECIMAL(18,8) NULL,
    refund_billing_history_id BIGINT NULL,
    refund_error TEXT NULL,
    refunded_at DATETIME NULL,
    status ENUM('pending','completed','failed') NOT NULL DEFAULT 'pending',
    attempt_count INT NOT NULL DEFAULT 0,
    error TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_email_dedupe (dedupe_key),
    KEY idx_ai_email_user (user_id),
    KEY idx_ai_email_agent (agent_id),
    KEY idx_ai_email_template (template_id),
    KEY idx_ai_email_billing (billing_history_id),
    CONSTRAINT fk_ai_email_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_email_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL,
    CONSTRAINT fk_ai_email_template FOREIGN KEY (template_id) REFERENCES ai_doc_templates(id) ON DELETE SET NULL,
    CONSTRAINT fk_ai_email_billing FOREIGN KEY (billing_history_id) REFERENCES billing_history(id) ON DELETE SET NULL
  )`);

  // AI SMS send audit log + idempotency
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_sms_sends (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    agent_id BIGINT NULL,
    dedupe_key CHAR(64) NOT NULL,
    call_id VARCHAR(64) NULL,
    call_domain VARCHAR(64) NULL,
    from_did_id VARCHAR(64) NULL,
    from_number VARCHAR(32) NULL,
    to_number VARCHAR(32) NOT NULL,
    message_text TEXT NULL,
    provider VARCHAR(32) NOT NULL DEFAULT 'sms_provider',
    provider_message_id VARCHAR(128) NULL,

    -- Provider callback (HTTP OUT status callback)
    provider_status VARCHAR(32) NULL,
    provider_code_id INT NULL,
    provider_fragments_sent INT NULL,
    provider_price DECIMAL(18,8) NULL,
    provider_time_start DATETIME NULL,
    provider_time_end DATETIME NULL,

    -- Provider DLR callback (delivery receipt)
    dlr_status VARCHAR(32) NULL,
    dlr_time_start DATETIME NULL,

    provider_status_payload JSON NULL,
    dlr_payload JSON NULL,

    billing_history_id BIGINT NULL,
    refund_status ENUM('none','pending','completed','failed') NOT NULL DEFAULT 'none',
    refund_amount DECIMAL(18,8) NULL,
    refund_billing_history_id BIGINT NULL,
    refund_error TEXT NULL,
    refunded_at DATETIME NULL,
    status ENUM('pending','completed','failed') NOT NULL DEFAULT 'pending',
    attempt_count INT NOT NULL DEFAULT 0,
    error TEXT NULL,
    raw_payload JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_sms_dedupe (dedupe_key),
    KEY idx_ai_sms_user (user_id),
    KEY idx_ai_sms_agent (agent_id),
    KEY idx_ai_sms_provider_message_id (provider_message_id),
    KEY idx_ai_sms_billing (billing_history_id),
    CONSTRAINT fk_ai_sms_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_sms_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL,
    CONSTRAINT fk_ai_sms_billing FOREIGN KEY (billing_history_id) REFERENCES billing_history(id) ON DELETE SET NULL
  )`);

  // Ensure ai_email_sends meeting-link columns exist (existing DB migrations)
  for (const col of [
    { name: 'meeting_provider', ddl: "ALTER TABLE ai_email_sends ADD COLUMN meeting_provider VARCHAR(32) NULL AFTER subject" },
    { name: 'meeting_room_name', ddl: "ALTER TABLE ai_email_sends ADD COLUMN meeting_room_name VARCHAR(191) NULL AFTER meeting_provider" },
    { name: 'meeting_room_url', ddl: "ALTER TABLE ai_email_sends ADD COLUMN meeting_room_url TEXT NULL AFTER meeting_room_name" },
    { name: 'meeting_join_url', ddl: "ALTER TABLE ai_email_sends ADD COLUMN meeting_join_url TEXT NULL AFTER meeting_room_url" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_email_sends' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_email_sends.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_email_sends.${col.name} check failed`, e.message || e);
    }
  }

  // Ensure ai_email_sends preview columns exist (best-effort migrations)
  for (const col of [
    { name: 'email_text', ddl: "ALTER TABLE ai_email_sends ADD COLUMN email_text TEXT NULL AFTER subject" },
    { name: 'email_html', ddl: "ALTER TABLE ai_email_sends ADD COLUMN email_html MEDIUMTEXT NULL AFTER email_text" },
    { name: 'attachments_json', ddl: "ALTER TABLE ai_email_sends ADD COLUMN attachments_json JSON NULL AFTER email_html" },
    { name: 'smtp_message_id', ddl: "ALTER TABLE ai_email_sends ADD COLUMN smtp_message_id VARCHAR(255) NULL AFTER attachments_json" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_email_sends' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_email_sends.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_email_sends.${col.name} check failed`, e.message || e);
    }
  }

  // Ensure ai_email_sends billing/refund columns exist (best-effort migrations)
  for (const col of [
    { name: 'billing_history_id', ddl: "ALTER TABLE ai_email_sends ADD COLUMN billing_history_id BIGINT NULL AFTER meeting_join_url" },
    { name: 'refund_status', ddl: "ALTER TABLE ai_email_sends ADD COLUMN refund_status ENUM('none','pending','completed','failed') NOT NULL DEFAULT 'none' AFTER billing_history_id" },
    { name: 'refund_amount', ddl: "ALTER TABLE ai_email_sends ADD COLUMN refund_amount DECIMAL(18,8) NULL AFTER refund_status" },
    { name: 'refund_billing_history_id', ddl: "ALTER TABLE ai_email_sends ADD COLUMN refund_billing_history_id BIGINT NULL AFTER refund_amount" },
    { name: 'refund_error', ddl: "ALTER TABLE ai_email_sends ADD COLUMN refund_error TEXT NULL AFTER refund_billing_history_id" },
    { name: 'refunded_at', ddl: "ALTER TABLE ai_email_sends ADD COLUMN refunded_at DATETIME NULL AFTER refund_error" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_email_sends' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_email_sends.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_email_sends.${col.name} check failed`, e.message || e);
    }
  }

  // Ensure ai_sms_sends billing/refund columns exist (best-effort migrations)
  for (const col of [
    { name: 'billing_history_id', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN billing_history_id BIGINT NULL AFTER provider_message_id" },
    { name: 'refund_status', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN refund_status ENUM('none','pending','completed','failed') NOT NULL DEFAULT 'none' AFTER billing_history_id" },
    { name: 'refund_amount', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN refund_amount DECIMAL(18,8) NULL AFTER refund_status" },
    { name: 'refund_billing_history_id', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN refund_billing_history_id BIGINT NULL AFTER refund_amount" },
    { name: 'refund_error', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN refund_error TEXT NULL AFTER refund_billing_history_id" },
    { name: 'refunded_at', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN refunded_at DATETIME NULL AFTER refund_error" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_sms_sends' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_sms_sends.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_sms_sends.${col.name} check failed`, e.message || e);
    }
  }

  // Ensure ai_sms_sends provider callback status columns exist (best-effort migrations)
  for (const col of [
    { name: 'provider_status', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN provider_status VARCHAR(32) NULL AFTER provider_message_id" },
    { name: 'provider_code_id', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN provider_code_id INT NULL AFTER provider_status" },
    { name: 'provider_fragments_sent', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN provider_fragments_sent INT NULL AFTER provider_code_id" },
    { name: 'provider_price', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN provider_price DECIMAL(18,8) NULL AFTER provider_fragments_sent" },
    { name: 'provider_time_start', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN provider_time_start DATETIME NULL AFTER provider_price" },
    { name: 'provider_time_end', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN provider_time_end DATETIME NULL AFTER provider_time_start" },
    { name: 'dlr_status', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN dlr_status VARCHAR(32) NULL AFTER provider_time_end" },
    { name: 'dlr_time_start', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN dlr_time_start DATETIME NULL AFTER dlr_status" },
    { name: 'provider_status_payload', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN provider_status_payload JSON NULL AFTER dlr_time_start" },
    { name: 'dlr_payload', ddl: "ALTER TABLE ai_sms_sends ADD COLUMN dlr_payload JSON NULL AFTER provider_status_payload" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_sms_sends' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_sms_sends.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_sms_sends.${col.name} check failed`, e.message || e);
    }
  }

  // Ensure ai_sms_sends provider_message_id index exists (best-effort)
  try {
    await pool.query('ALTER TABLE ai_sms_sends ADD INDEX idx_ai_sms_provider_message_id (provider_message_id)');
  } catch (e) {
    // Ignore if already exists.
  }

  // AI physical mail send audit log + idempotency (Click2Mail)
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_mail_sends (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    agent_id BIGINT NULL,
    template_id BIGINT NULL,
    dedupe_key CHAR(64) NOT NULL,
    call_id VARCHAR(64) NULL,
    call_domain VARCHAR(64) NULL,
    to_name VARCHAR(255) NULL,
    to_organization VARCHAR(255) NULL,
    to_address1 VARCHAR(255) NOT NULL,
    to_address2 VARCHAR(255) NULL,
    to_address3 VARCHAR(255) NULL,
    to_city VARCHAR(100) NOT NULL,
    to_state VARCHAR(64) NOT NULL,
    to_postal_code VARCHAR(20) NOT NULL,
    to_country VARCHAR(2) NOT NULL DEFAULT 'US',
    corrected_to_name VARCHAR(255) NULL,
    corrected_to_organization VARCHAR(255) NULL,
    corrected_to_address1 VARCHAR(255) NULL,
    corrected_to_address2 VARCHAR(255) NULL,
    corrected_to_address3 VARCHAR(255) NULL,
    corrected_to_city VARCHAR(100) NULL,
    corrected_to_state VARCHAR(64) NULL,
    corrected_to_postal_code VARCHAR(20) NULL,
    corrected_to_country VARCHAR(2) NULL,
    provider VARCHAR(32) NOT NULL DEFAULT 'click2mail',
    product VARCHAR(128) NULL,
    mail_class VARCHAR(128) NULL,
    batch_id VARCHAR(64) NULL,
    click2mail_job_id VARCHAR(64) NULL,
    tracking_type VARCHAR(16) NULL,
    tracking_number VARCHAR(128) NULL,
    tracking_status VARCHAR(128) NULL,
    tracking_status_date DATETIME NULL,
    tracking_status_location VARCHAR(255) NULL,
    status ENUM('pending','submitted','completed','failed') NOT NULL DEFAULT 'pending',
    cost_estimate DECIMAL(18,8) NULL,
    markup_amount DECIMAL(18,8) NULL,
    total_cost DECIMAL(18,8) NULL,
    billing_history_id BIGINT NULL,
    refund_status ENUM('none','pending','completed','failed') NOT NULL DEFAULT 'none',
    refund_amount DECIMAL(18,8) NULL,
    refund_billing_history_id BIGINT NULL,
    refund_error TEXT NULL,
    refunded_at DATETIME NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    error TEXT NULL,
    raw_payload JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_mail_dedupe (dedupe_key),
    KEY idx_ai_mail_user (user_id),
    KEY idx_ai_mail_agent (agent_id),
    KEY idx_ai_mail_template (template_id),
    KEY idx_ai_mail_batch (batch_id),
    KEY idx_ai_mail_job (click2mail_job_id),
    KEY idx_ai_mail_tracking (tracking_number),
    CONSTRAINT fk_ai_mail_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_mail_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL,
    CONSTRAINT fk_ai_mail_template FOREIGN KEY (template_id) REFERENCES ai_doc_templates(id) ON DELETE SET NULL,
    CONSTRAINT fk_ai_mail_billing FOREIGN KEY (billing_history_id) REFERENCES billing_history(id) ON DELETE SET NULL
  )`);

  // Ensure ai_mail_sends cost/refund columns exist (best-effort migrations)
  for (const col of [
    { name: 'markup_amount', ddl: "ALTER TABLE ai_mail_sends ADD COLUMN markup_amount DECIMAL(18,8) NULL AFTER cost_estimate" },
    { name: 'total_cost', ddl: "ALTER TABLE ai_mail_sends ADD COLUMN total_cost DECIMAL(18,8) NULL AFTER markup_amount" },
    { name: 'refund_status', ddl: "ALTER TABLE ai_mail_sends ADD COLUMN refund_status ENUM('none','pending','completed','failed') NOT NULL DEFAULT 'none' AFTER billing_history_id" },
    { name: 'refund_amount', ddl: "ALTER TABLE ai_mail_sends ADD COLUMN refund_amount DECIMAL(18,8) NULL AFTER refund_status" },
    { name: 'refund_billing_history_id', ddl: "ALTER TABLE ai_mail_sends ADD COLUMN refund_billing_history_id BIGINT NULL AFTER refund_amount" },
    { name: 'refund_error', ddl: "ALTER TABLE ai_mail_sends ADD COLUMN refund_error TEXT NULL AFTER refund_billing_history_id" },
    { name: 'refunded_at', ddl: "ALTER TABLE ai_mail_sends ADD COLUMN refunded_at DATETIME NULL AFTER refund_error" },
  ]) {
    try {
      const [rows] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_mail_sends' AND column_name=? LIMIT 1",
        [dbName, col.name]
      );
      if (!rows || !rows.length) {
        await pool.query(col.ddl);
        if (DEBUG) console.log(`[schema] Added ai_mail_sends.${col.name} column`);
      }
    } catch (e) {
      if (DEBUG) console.warn(`[schema] ai_mail_sends.${col.name} check failed`, e.message || e);
    }
  }
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_call_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(64) NOT NULL,
    call_domain VARCHAR(64) NOT NULL,
    event_call_id VARCHAR(64) NULL,
    event_call_domain VARCHAR(64) NULL,
    user_id BIGINT NOT NULL,
    agent_id BIGINT NULL,
    ai_number_id BIGINT NULL,
    direction VARCHAR(16) NOT NULL DEFAULT 'inbound',
    from_number VARCHAR(64) NULL,
    to_number VARCHAR(64) NULL,
    time_start DATETIME NULL,
    time_connect DATETIME NULL,
    time_end DATETIME NULL,
    duration INT NULL,
    billsec INT NULL,
    price DECIMAL(18,8) NULL,
    billed TINYINT(1) NOT NULL DEFAULT 0,
    billing_history_id BIGINT NULL,
    status VARCHAR(32) NULL,
    raw_payload JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_call (call_domain, call_id),
    KEY idx_ai_call_user (user_id),
    KEY idx_ai_call_time_start (time_start),
    KEY idx_ai_call_to_number (to_number),
    KEY idx_ai_call_billed (user_id, billed),
    KEY idx_ai_call_event (event_call_domain, event_call_id),
    CONSTRAINT fk_ai_call_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_call_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL,
    CONSTRAINT fk_ai_call_number FOREIGN KEY (ai_number_id) REFERENCES ai_numbers(id) ON DELETE SET NULL
  )`);

  // AI call conversation messages (user + assistant) per inbound session.
  // Logged by the agent runtime using the agent action token.
  await pool.query(`CREATE TABLE IF NOT EXISTS ai_call_messages (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id CHAR(32) NOT NULL,
    user_id BIGINT NOT NULL,
    agent_id BIGINT NULL,
    call_id VARCHAR(64) NOT NULL,
    call_domain VARCHAR(64) NOT NULL,
    role ENUM('user','assistant') NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ai_call_message (call_domain, call_id, message_id),
    KEY idx_ai_call_messages_user (user_id),
    KEY idx_ai_call_messages_call (user_id, call_domain, call_id, id),
    KEY idx_ai_call_messages_agent (agent_id),
    CONSTRAINT fk_ai_call_messages_user FOREIGN KEY (user_id) REFERENCES signup_users(id) ON DELETE CASCADE,
    CONSTRAINT fk_ai_call_messages_agent FOREIGN KEY (agent_id) REFERENCES ai_agents(id) ON DELETE SET NULL
  )`);

  // One-time migration: drop legacy per-user API columns if present and add password_hash if missing
  try {
    const toDrop = ['api_key','api_secret'];
    for (const c of toDrop) {
      const [rows] = await pool.query(
        'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'signup_users\' AND column_name=? LIMIT 1',
        [dbName, c]
      );
      if (rows && rows.length) {
        try { await pool.query(`ALTER TABLE signup_users DROP COLUMN ${c}`); } catch (e) { if (DEBUG) console.warn('Schema drop failed', c, e.message || e); }
      }
    }
    const [hasPwd] = await pool.query(
      'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'signup_users\' AND column_name=\'password_hash\' LIMIT 1',
      [dbName]
    );
    if (!hasPwd || !hasPwd.length) {
      try { await pool.query('ALTER TABLE signup_users ADD COLUMN password_hash VARCHAR(255) NULL AFTER phone'); } catch (e) { if (DEBUG) console.warn('Add password_hash failed', e.message || e); }
    }

    // Ensure ai_agents.cartesia_voice_id exists (per-agent Cartesia voice selection)
    try {
      const [hasVoiceId] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='cartesia_voice_id' LIMIT 1",
        [dbName]
      );
      if (!hasVoiceId || !hasVoiceId.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN cartesia_voice_id VARCHAR(191) NULL AFTER prompt');
        if (DEBUG) console.log('[schema] Added ai_agents.cartesia_voice_id column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.cartesia_voice_id check failed', e.message || e);
    }

    // Ensure ai_agents has an agent action token + default doc template pointer
    try {
      const [hasTokenHash] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='agent_action_token_hash' LIMIT 1",
        [dbName]
      );
      if (!hasTokenHash || !hasTokenHash.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN agent_action_token_hash CHAR(64) NULL AFTER pipecat_region');
        if (DEBUG) console.log('[schema] Added ai_agents.agent_action_token_hash column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.agent_action_token_hash check failed', e.message || e);
    }

    try {
      const [hasTokenEnc] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='agent_action_token_enc' LIMIT 1",
        [dbName]
      );
      if (!hasTokenEnc || !hasTokenEnc.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN agent_action_token_enc BLOB NULL AFTER agent_action_token_hash');
        if (DEBUG) console.log('[schema] Added ai_agents.agent_action_token_enc column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.agent_action_token_enc check failed', e.message || e);
    }

    try {
      const [hasTokenIv] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='agent_action_token_iv' LIMIT 1",
        [dbName]
      );
      if (!hasTokenIv || !hasTokenIv.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN agent_action_token_iv VARBINARY(16) NULL AFTER agent_action_token_enc');
        if (DEBUG) console.log('[schema] Added ai_agents.agent_action_token_iv column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.agent_action_token_iv check failed', e.message || e);
    }

    try {
      const [hasTokenTag] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='agent_action_token_tag' LIMIT 1",
        [dbName]
      );
      if (!hasTokenTag || !hasTokenTag.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN agent_action_token_tag VARBINARY(16) NULL AFTER agent_action_token_iv');
        if (DEBUG) console.log('[schema] Added ai_agents.agent_action_token_tag column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.agent_action_token_tag check failed', e.message || e);
    }

    try {
      const [hasDefaultTpl] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='default_doc_template_id' LIMIT 1",
        [dbName]
      );
      if (!hasDefaultTpl || !hasDefaultTpl.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN default_doc_template_id BIGINT NULL AFTER agent_action_token_tag');
        if (DEBUG) console.log('[schema] Added ai_agents.default_doc_template_id column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.default_doc_template_id check failed', e.message || e);
    }

    // Ensure ai_agents.transfer_to_number exists (per-agent call transfer destination override)
    try {
      const [hasTransferTo] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='transfer_to_number' LIMIT 1",
        [dbName]
      );
      if (!hasTransferTo || !hasTransferTo.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN transfer_to_number VARCHAR(64) NULL AFTER agent_action_token_tag');
        if (DEBUG) console.log('[schema] Added ai_agents.transfer_to_number column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.transfer_to_number check failed', e.message || e);
    }

    try {
      const [hasDefaultTplIdx] = await pool.query(
        "SELECT 1 FROM information_schema.statistics WHERE table_schema=? AND table_name='ai_agents' AND index_name='idx_ai_agents_default_template' LIMIT 1",
        [dbName]
      );
      if (!hasDefaultTplIdx || !hasDefaultTplIdx.length) {
        try {
          await pool.query('ALTER TABLE ai_agents ADD KEY idx_ai_agents_default_template (default_doc_template_id)');
          if (DEBUG) console.log('[schema] Added ai_agents.idx_ai_agents_default_template index');
        } catch (e) {
          if (DEBUG) console.warn('[schema] Failed to add ai_agents.idx_ai_agents_default_template index:', e.message || e);
        }
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.idx_ai_agents_default_template check failed', e.message || e);
    }

    // Ensure ai_agents background audio settings exist (per-agent ambience while bot speaks)
    try {
      const [hasBgUrl] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='background_audio_url' LIMIT 1",
        [dbName]
      );
      if (!hasBgUrl || !hasBgUrl.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN background_audio_url VARCHAR(512) NULL AFTER cartesia_voice_id');
        if (DEBUG) console.log('[schema] Added ai_agents.background_audio_url column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.background_audio_url check failed', e.message || e);
    }

    try {
      const [hasBgGain] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_agents' AND column_name='background_audio_gain' LIMIT 1",
        [dbName]
      );
      if (!hasBgGain || !hasBgGain.length) {
        await pool.query('ALTER TABLE ai_agents ADD COLUMN background_audio_gain DECIMAL(4,3) NULL AFTER background_audio_url');
        if (DEBUG) console.log('[schema] Added ai_agents.background_audio_gain column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_agents.background_audio_gain check failed', e.message || e);
    }

    // Ensure ai_call_logs.time_connect exists (Daily dial-in connected timestamp)
    try {
      const [hasTimeConnect] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_call_logs' AND column_name='time_connect' LIMIT 1",
        [dbName]
      );
      if (!hasTimeConnect || !hasTimeConnect.length) {
        await pool.query('ALTER TABLE ai_call_logs ADD COLUMN time_connect DATETIME NULL AFTER time_start');
        if (DEBUG) console.log('[schema] Added ai_call_logs.time_connect column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.time_connect check failed', e.message || e);
    }

    // Ensure ai_call_logs event identifiers exist (Daily /webhooks/daily/events may use different ids)
    try {
      const [hasEventCallId] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_call_logs' AND column_name='event_call_id' LIMIT 1",
        [dbName]
      );
      if (!hasEventCallId || !hasEventCallId.length) {
        await pool.query('ALTER TABLE ai_call_logs ADD COLUMN event_call_id VARCHAR(64) NULL AFTER call_domain');
        if (DEBUG) console.log('[schema] Added ai_call_logs.event_call_id column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.event_call_id check failed', e.message || e);
    }

    try {
      const [hasEventCallDomain] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_call_logs' AND column_name='event_call_domain' LIMIT 1",
        [dbName]
      );
      if (!hasEventCallDomain || !hasEventCallDomain.length) {
        await pool.query('ALTER TABLE ai_call_logs ADD COLUMN event_call_domain VARCHAR(64) NULL AFTER event_call_id');
        if (DEBUG) console.log('[schema] Added ai_call_logs.event_call_domain column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.event_call_domain check failed', e.message || e);
    }

    try {
      const [hasEventIdx] = await pool.query(
        "SELECT 1 FROM information_schema.statistics WHERE table_schema=? AND table_name='ai_call_logs' AND index_name='idx_ai_call_event' LIMIT 1",
        [dbName]
      );
      if (!hasEventIdx || !hasEventIdx.length) {
        try {
          await pool.query('ALTER TABLE ai_call_logs ADD KEY idx_ai_call_event (event_call_domain, event_call_id)');
          if (DEBUG) console.log('[schema] Added ai_call_logs.idx_ai_call_event index');
        } catch (e) {
          if (DEBUG) console.warn('[schema] Failed to add idx_ai_call_event index:', e.message || e);
        }
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.idx_ai_call_event check failed', e.message || e);
    }

    // Ensure ai_call_logs billing columns exist (price + billed markers)
    try {
      const [hasAiPrice] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_call_logs' AND column_name='price' LIMIT 1",
        [dbName]
      );
      if (!hasAiPrice || !hasAiPrice.length) {
        await pool.query('ALTER TABLE ai_call_logs ADD COLUMN price DECIMAL(18,8) NULL AFTER billsec');
        if (DEBUG) console.log('[schema] Added ai_call_logs.price column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.price check failed', e.message || e);
    }

    try {
      const [hasAiBilled] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_call_logs' AND column_name='billed' LIMIT 1",
        [dbName]
      );
      if (!hasAiBilled || !hasAiBilled.length) {
        await pool.query('ALTER TABLE ai_call_logs ADD COLUMN billed TINYINT(1) NOT NULL DEFAULT 0 AFTER price');
        if (DEBUG) console.log('[schema] Added ai_call_logs.billed column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.billed check failed', e.message || e);
    }

    try {
      const [hasAiBillingId] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='ai_call_logs' AND column_name='billing_history_id' LIMIT 1",
        [dbName]
      );
      if (!hasAiBillingId || !hasAiBillingId.length) {
        await pool.query('ALTER TABLE ai_call_logs ADD COLUMN billing_history_id BIGINT NULL AFTER billed');
        if (DEBUG) console.log('[schema] Added ai_call_logs.billing_history_id column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.billing_history_id check failed', e.message || e);
    }

    try {
      const [hasAiBilledIdx] = await pool.query(
        "SELECT 1 FROM information_schema.statistics WHERE table_schema=? AND table_name='ai_call_logs' AND index_name='idx_ai_call_billed' LIMIT 1",
        [dbName]
      );
      if (!hasAiBilledIdx || !hasAiBilledIdx.length) {
        try {
          await pool.query('ALTER TABLE ai_call_logs ADD KEY idx_ai_call_billed (user_id, billed)');
          if (DEBUG) console.log('[schema] Added ai_call_logs.idx_ai_call_billed index');
        } catch (e) {
          if (DEBUG) console.warn('[schema] Failed to add idx_ai_call_billed index:', e.message || e);
        }
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] ai_call_logs.idx_ai_call_billed check failed', e.message || e);
    }

    // Add dst column to user_trunks if missing
    const [hasDst] = await pool.query(
      'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'user_trunks\' AND column_name=\'dst\' LIMIT 1',
      [dbName]
    );
    if (!hasDst || !hasDst.length) {
      try { await pool.query('ALTER TABLE user_trunks ADD COLUMN dst VARCHAR(32) NULL AFTER name'); } catch (e) { if (DEBUG) console.warn('Add dst column failed', e.message || e); }
    }

    // Drop legacy user_sms_trunks table now that SMS forwarding is removed
    try {
      const [hasSmsTable] = await pool.query(
        'SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=\'user_sms_trunks\' LIMIT 1',
        [dbName]
      );
      if (hasSmsTable && hasSmsTable.length) {
        if (DEBUG) console.log('[schema] Dropping legacy table user_sms_trunks');
        await pool.query('DROP TABLE IF EXISTS user_sms_trunks');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] Failed to drop user_sms_trunks:', e.message || e);
    }

    // Ensure inbound CDR billing columns exist on user_did_cdrs
    try {
      const [hasCdrBilled] = await pool.query(
        'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'user_did_cdrs\' AND column_name=\'billed\' LIMIT 1',
        [dbName]
      );
      if (!hasCdrBilled || !hasCdrBilled.length) {
        try { await pool.query('ALTER TABLE user_did_cdrs ADD COLUMN billed TINYINT(1) NOT NULL DEFAULT 0 AFTER price'); } catch (e) { if (DEBUG) console.warn('Add billed column to user_did_cdrs failed', e.message || e); }
      }
      const [hasCdrBillingId] = await pool.query(
        'SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=\'user_did_cdrs\' AND column_name=\'billing_history_id\' LIMIT 1',
        [dbName]
      );
      if (!hasCdrBillingId || !hasCdrBillingId.length) {
        try { await pool.query('ALTER TABLE user_did_cdrs ADD COLUMN billing_history_id BIGINT NULL AFTER billed'); } catch (e) { if (DEBUG) console.warn('Add billing_history_id column to user_did_cdrs failed', e.message || e); }
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] Inbound CDR billing column check failed', e.message || e);
    }

    // Ensure billing_history.amount has enough precision for small per-second charges
    try {
      const [amountCol] = await pool.query(
        "SELECT NUMERIC_SCALE AS scale, NUMERIC_PRECISION AS prec FROM information_schema.columns WHERE table_schema=? AND table_name='billing_history' AND column_name='amount' LIMIT 1",
        [dbName]
      );
      const col = amountCol && amountCol[0];
      // If scale < 4 or precision < 18, upgrade to DECIMAL(18,8)
      if (col && ((col.scale != null && col.scale < 4) || (col.prec != null && col.prec < 18))) {
        try {
          await pool.query('ALTER TABLE billing_history MODIFY COLUMN amount DECIMAL(18,8) NOT NULL');
          if (DEBUG) console.log('[schema] Upgraded billing_history.amount to DECIMAL(18,8)');
        } catch (e) {
          if (DEBUG) console.warn('[schema] Failed to alter billing_history.amount precision:', e.message || e);
        }
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] Billing_history.amount precision check failed:', e.message || e);
    }

    // Ensure signup_users.is_admin column exists for admin dashboard access
    try {
      const [hasIsAdmin] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='signup_users' AND column_name='is_admin' LIMIT 1",
        [dbName]
      );
      if (!hasIsAdmin || !hasIsAdmin.length) {
        await pool.query('ALTER TABLE signup_users ADD COLUMN is_admin TINYINT(1) NOT NULL DEFAULT 0 AFTER password_hash');
        if (DEBUG) console.log('[schema] Added signup_users.is_admin column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] signup_users.is_admin check failed', e.message || e);
    }

    // Ensure signup_users.suspended column exists for account suspension
    try {
      const [hasSuspended] = await pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name='signup_users' AND column_name='suspended' LIMIT 1",
        [dbName]
      );
      if (!hasSuspended || !hasSuspended.length) {
        await pool.query('ALTER TABLE signup_users ADD COLUMN suspended TINYINT(1) NOT NULL DEFAULT 0 AFTER is_admin');
        if (DEBUG) console.log('[schema] Added signup_users.suspended column');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] signup_users.suspended check failed', e.message || e);
    }

    // Add indexes for admin dashboard queries
    try {
      const [hasCreatedIdx] = await pool.query(
        "SELECT 1 FROM information_schema.statistics WHERE table_schema=? AND table_name='signup_users' AND index_name='idx_created_at' LIMIT 1",
        [dbName]
      );
      if (!hasCreatedIdx || !hasCreatedIdx.length) {
        await pool.query('ALTER TABLE signup_users ADD INDEX idx_created_at (created_at)');
        if (DEBUG) console.log('[schema] Added signup_users.idx_created_at index');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] signup_users idx_created_at check failed', e.message || e);
    }

    try {
      const [hasBillingIdx] = await pool.query(
        "SELECT 1 FROM information_schema.statistics WHERE table_schema=? AND table_name='billing_history' AND index_name='idx_user_created' LIMIT 1",
        [dbName]
      );
      if (!hasBillingIdx || !hasBillingIdx.length) {
        await pool.query('ALTER TABLE billing_history ADD INDEX idx_user_created (user_id, created_at)');
        if (DEBUG) console.log('[schema] Added billing_history.idx_user_created index');
      }
    } catch (e) {
      if (DEBUG) console.warn('[schema] billing_history idx_user_created check failed', e.message || e);
    }
  } catch (e) { if (DEBUG) console.warn('Schema check failed', e.message || e); }
}
async function storeVerification({ token, codeHash, email, expiresAt, fields }) {
  if (!pool) throw new Error('Database not configured');
  // Store plaintext password (legacy behavior). NOTE: less secure; consider encrypting in future.
  const plainPassword = String(fields.password || '');
  const address1 = fields.address1 != null ? String(fields.address1).trim() : '';
  const city = fields.city != null ? String(fields.city).trim() : '';
  const state = fields.state != null ? String(fields.state).trim() : '';
  const postalCode = fields.postal_code != null ? String(fields.postal_code).trim() : (fields.postalCode != null ? String(fields.postalCode).trim() : '');
  const country = fields.country != null ? String(fields.country).trim().toUpperCase() : '';
  const signupIp = fields.signup_ip != null ? String(fields.signup_ip).trim() : (fields.signupIp != null ? String(fields.signupIp).trim() : '');

  const sql = `INSERT INTO email_verifications (token,email,code_hash,expires_at,used,username,password,firstname,lastname,phone,address1,city,state,postal_code,country,signup_ip) VALUES (?,?,?,?,0,?,?,?,?,?,?,?,?,?,?,?)`;
  await pool.execute(sql, [
    token,
    email,
    codeHash,
    expiresAt,
    fields.username,
    plainPassword,
    fields.firstname,
    fields.lastname,
    fields.phone || null,
    address1 || null,
    city || null,
    state || null,
    postalCode || null,
    country || null,
    signupIp || null
  ]);
}
async function fetchVerification(token) {
  if (!pool) throw new Error('Database not configured');
  const [rows] = await pool.execute('SELECT * FROM email_verifications WHERE token=?', [token]);
  return rows[0];
}
async function fetchVerificationLatestByEmail(email) {
  if (!pool) throw new Error('Database not configured');
  const [rows] = await pool.execute('SELECT * FROM email_verifications WHERE email=? AND used=0 AND expires_at>=? ORDER BY expires_at DESC LIMIT 1', [email, Date.now()]);
  return rows[0];
}
async function markVerificationUsed(token) {
  if (!pool) throw new Error('Database not configured');
  await pool.execute('UPDATE email_verifications SET used=1 WHERE token=?', [token]);
}
async function purgeExpired() {
  if (!pool) return; try { await pool.execute('DELETE FROM email_verifications WHERE expires_at < ? OR used=1', [Date.now()]); } catch {}
}
async function saveUserRow({ username, email, firstname, lastname, phone, address1, city, state, postalCode, country, signupIp, passwordHash = null }) {
  if (!pool) return;
  const sql = `INSERT INTO signup_users (username, email, firstname, lastname, phone, address1, city, state, postal_code, country, signup_ip, password_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON DUPLICATE KEY UPDATE
                 firstname=VALUES(firstname),
                 lastname=VALUES(lastname),
                 phone=VALUES(phone),
                 address1=COALESCE(VALUES(address1), address1),
                 city=COALESCE(VALUES(city), city),
                 state=COALESCE(VALUES(state), state),
                 postal_code=COALESCE(VALUES(postal_code), postal_code),
                 country=COALESCE(VALUES(country), country),
                 signup_ip=COALESCE(VALUES(signup_ip), signup_ip),
                 password_hash=COALESCE(VALUES(password_hash), password_hash)`;
  await pool.execute(sql, [
    username,
    email,
    firstname || null,
    lastname || null,
    phone || null,
    address1 || null,
    city || null,
    state || null,
    postalCode || null,
    country || null,
    signupIp || null,
    passwordHash
  ]);
}
async function checkAvailability({ username, email }) {
  if (!pool) return { usernameAvailable: true, emailAvailable: true };
  let usernameAvailable = true, emailAvailable = true;
  if (username) {
    const [r] = await pool.execute('SELECT 1 FROM users WHERE username=? LIMIT 1', [username]);
    usernameAvailable = r.length === 0;
  }
  if (email) {
    const [r2] = await pool.execute('SELECT 1 FROM users WHERE email=? LIMIT 1', [email]);
    emailAvailable = r2.length === 0;
  }
  return { usernameAvailable, emailAvailable };
}

function parseSmtp2goSendResult(payload) {
  try {
    const root = (payload && typeof payload === 'object') ? payload : {};
    const data = (root.data && typeof root.data === 'object') ? root.data : root;
    const succeeded = (typeof data.succeeded === 'number') ? data.succeeded : (typeof root.succeeded === 'number' ? root.succeeded : undefined);
    const failed = (typeof data.failed === 'number') ? data.failed : (typeof root.failed === 'number' ? root.failed : undefined);
    const requestId = root.request_id || root.requestId || data.request_id || data.requestId;
    const errors = Array.isArray(root.errors) ? root.errors : (Array.isArray(data.errors) ? data.errors : undefined);
    const failureReasons = Array.isArray(root.failures) ? root.failures : (Array.isArray(data.failures) ? data.failures : undefined);
    return { succeeded, failed, requestId, errors, failureReasons, raw: root };
  } catch {
    return { succeeded: undefined, failed: undefined, requestId: undefined, errors: undefined, failureReasons: undefined, raw: payload };
  }
}

async function smtp2goSendEmail(payload, { kind, to, subject } = {}) {
  const url = 'https://api.smtp2go.com/v3/email/send';
  let resp;
  try {
    // SMTP2GO may return HTTP 200 even when some recipients fail. We always
    // inspect the response body to ensure the send actually succeeded.
    resp = await axios.post(url, payload, { timeout: 15000, validateStatus: () => true });
  } catch (e) {
    const msg = e?.code === 'ECONNABORTED' ? 'SMTP2GO request timed out' : (e?.message || 'SMTP2GO request failed');
    if (DEBUG) console.warn('[smtp2go.send] network error', { kind, to, subject, code: e?.code, message: e?.message });
    throw new Error(msg);
  }

  const parsed = parseSmtp2goSendResult(resp.data);

  if (DEBUG) {
    console.log('[smtp2go.send]', {
      kind,
      to,
      subject,
      status: resp.status,
      requestId: parsed.requestId,
      succeeded: parsed.succeeded,
      failed: parsed.failed,
      hasErrors: Array.isArray(parsed.errors) ? parsed.errors.length : 0,
      hasFailures: Array.isArray(parsed.failureReasons) ? parsed.failureReasons.length : 0
    });
  }

  // Hard fail on non-2xx
  if (!(resp.status >= 200 && resp.status < 300)) {
    const details = (resp.data && typeof resp.data === 'object') ? JSON.stringify(resp.data) : String(resp.data || '');
    throw new Error(`SMTP2GO HTTP ${resp.status}${details ? `: ${details}` : ''}`);
  }

  // Fail fast if API reports recipient failures/errors in the body
  if (Array.isArray(parsed.errors) && parsed.errors.length) {
    throw new Error(`SMTP2GO send error: ${JSON.stringify(parsed.raw)}`);
  }
  if (typeof parsed.failed === 'number' && parsed.failed > 0) {
    throw new Error(`SMTP2GO send had failures (failed=${parsed.failed}): ${JSON.stringify(parsed.raw)}`);
  }
  if (typeof parsed.succeeded === 'number' && parsed.succeeded < 1) {
    throw new Error(`SMTP2GO send did not succeed (succeeded=${parsed.succeeded}): ${JSON.stringify(parsed.raw)}`);
  }

  return resp.data;
}

function smtp2goAttachmentFromBuffer(buf, { filename, mimeType } = {}) {
  if (!buf || !Buffer.isBuffer(buf)) return null;
  const safeName = String(filename || 'attachment')
    .replace(/[^a-z0-9._-]+/gi, '_')
    .slice(0, 120) || 'attachment';
  const mt = String(mimeType || 'application/octet-stream').trim() || 'application/octet-stream';
  return {
    filename: safeName,
    fileblob: buf.toString('base64'),
    mimetype: mt
  };
}

async function sendVerificationEmail(toEmail, code) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  if (!apiKey) throw new Error('SMTP2GO_API_KEY missing');
  const subject = 'Your Phone.System verification code';
  const minutes = Math.round(OTP_TTL_MS / 60000);
  const text = `Your verification code is ${code}. It expires in ${minutes} minutes.`;
  const html = `<p>Your verification code is <b>${code}</b>.</p><p>This code expires in ${minutes} minutes.</p>`;
  const payload = { api_key: apiKey, to: [toEmail], sender, subject, text_body: text, html_body: html };
  await smtp2goSendEmail(payload, { kind: 'verification', to: toEmail, subject });
}

async function sendWelcomeEmail(toEmail, username) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  if (!apiKey) throw new Error('SMTP2GO_API_KEY missing');
  const subject = 'Welcome to Phone.System';
  const portalUrl = process.env.PUBLIC_BASE_URL || '';
  const text = `Welcome to Phone.System!\n\nYour account (${username}) has been created. You can now log in to manage your AI agents, numbers, and billing.${portalUrl ? `\n\nLog in: ${portalUrl}/login` : ''}\n\nThank you,\nPhone.System`;
  const html = `
  <div style=\"font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;\">\r
    <h2>Welcome to Phone.System!</h2>\r
    <p>Your account <b>${username}</b> has been created.</p>\r
    <p>You can now log in to manage your AI agents, numbers, and billing.</p>\r
    ${portalUrl ? `<p style=\"margin-top:14px;\"><a href=\"${portalUrl}/login\" style=\"background:#4f46e5;color:#fff;padding:10px 14px;border-radius:8px;text-decoration:none;\">Log in to your account</a></p>` : ''}\r
    <p style=\"color:#555;margin-top:18px;\">Thank you for signing up.</p>\r
  </div>`;
  const adminEmail = process.env.ADMIN_NOTIFY_EMAIL;
  const to = [toEmail].filter(Boolean);
  const payload = {
    api_key: apiKey,
    to,
    ...(adminEmail ? { bcc: [adminEmail] } : {}),
    sender,
    subject,
    text_body: text,
    html_body: html
  };
  await smtp2goSendEmail(payload, { kind: 'welcome', to: toEmail, subject });
}

function normalizeSignupCountry(country) {
  const c = String(country || '').trim().toUpperCase();
  if (c === 'US' || c === 'USA') return 'US';
  if (c === 'CA' || c === 'CAN') return 'CA';
  return null;
}

function normalizePostalCode(raw, { country } = {}) {
  const s = String(raw || '').trim().toUpperCase();
  if (!s) return '';
  const c = String(country || '').trim().toUpperCase();
  if (c === 'CA') return s.replace(/[^A-Z0-9]/g, '');
  return s.replace(/\s+/g, '').replace(/[^A-Z0-9-]/g, '');
}

function formatSignupAddressLine({ address1, city, state, postalCode, country }) {
  const parts = [address1, city, state].map(v => String(v || '').trim()).filter(Boolean);
  const zip = String(postalCode || '').trim();
  const c = String(country || '').trim();
  let line = parts.join(', ');
  if (zip) line = line ? `${line} ${zip}` : zip;
  if (c) line = line ? `${line}, ${c}` : c;
  return line;
}

async function sendAdminSignupCopyEmail({
  email,
  username,
  firstname,
  lastname,
  phone,
  address1,
  city,
  state,
  postalCode,
  country,
  signupIp,
} = {}) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  const adminEmail = process.env.ADMIN_SIGNUP_NOTIFY_EMAIL || process.env.ADMIN_NOTIFY_EMAIL;
  if (!apiKey || !adminEmail) return;

  const esc = (v) => String(v ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');

  const name = [firstname, lastname].filter(Boolean).join(' ').trim();
  const addrLine = formatSignupAddressLine({ address1, city, state, postalCode, country });
  const subject = `New Phone.System signup: ${String(username || '').trim() || '-'} (${String(email || '').trim() || '-'})`;

  const lines = [
    'New Phone.System signup',
    '',
    name ? `Name: ${name}` : null,
    username ? `Username: ${username}` : null,
    email ? `Email: ${email}` : null,
    phone ? `Phone: ${phone}` : null,
    addrLine ? `Address: ${addrLine}` : null,
    signupIp ? `Signup IP: ${signupIp}` : null,
    
    '',
    `Time: ${new Date().toISOString()}`
  ].filter(Boolean);

  const text = lines.join('\n');

  const htmlRows = [
    name ? `<tr><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;"><b>Name</b></td><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;">${esc(name)}</td></tr>` : '',
    username ? `<tr><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;"><b>Username</b></td><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;">${esc(username)}</td></tr>` : '',
    email ? `<tr><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;"><b>Email</b></td><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;">${esc(email)}</td></tr>` : '',
    phone ? `<tr><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;"><b>Phone</b></td><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;">${esc(phone)}</td></tr>` : '',
    addrLine ? `<tr><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;"><b>Address</b></td><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;">${esc(addrLine)}</td></tr>` : '',
    signupIp ? `<tr><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;"><b>Signup IP</b></td><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;">${esc(signupIp)}</td></tr>` : '',
    ''
  ].filter(Boolean).join('');

  const html = `
  <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">\r
    <h2>New Phone.System signup</h2>\r
    <table style="border:1px solid #e5e7eb;border-radius:10px;border-collapse:separate;border-spacing:0;overflow:hidden;max-width:680px;background:#ffffff">\r
      <tbody>\r
        ${htmlRows}\r
      </tbody>\r
    </table>\r
    <p style="color:#6b7280;margin-top:12px;font-size:12px">Time: ${esc(new Date().toISOString())}</p>\r
  </div>`;

  const payload = { api_key: apiKey, to: [adminEmail], sender, subject, text_body: text, html_body: html };
  await smtp2goSendEmail(payload, { kind: 'admin-signup-copy', to: adminEmail, subject });
}

function buildRefillDescription(invoiceNumber) {
  const inv = String(invoiceNumber ?? '').trim();
  const line = `A.I Service Credit Phone.System Invoice Number ${inv}`.trim();
  return line.substring(0, 255);
}

// Send a refill receipt email when funds are added
async function sendRefillReceiptEmail({ toEmail, username, amount, description, invoiceNumber, paymentMethod, processorTransactionId }) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  if (!apiKey) throw new Error('SMTP2GO_API_KEY missing');

  const amt = Number(amount || 0);
  const safeUser = username || 'Customer';

  const inv = invoiceNumber != null ? String(invoiceNumber).trim() : '';
  const method = paymentMethod != null ? String(paymentMethod).trim() : '';
  const txn = processorTransactionId != null ? String(processorTransactionId).trim() : '';

  const subjectSuffix = inv ? ` (Invoice ${inv})` : '';
  const subject = `Payment receipt - $${amt.toFixed(2)} added to your Phone.System balance${subjectSuffix}`;

  const lines = [
    `Hello ${safeUser},`,
    '',
    `We have received your payment of $${amt.toFixed(2)}.`,
    inv ? `Invoice Number: ${inv}` : null,
    method ? `Payment Method: ${method}` : null,
    txn ? `Transaction ID: ${txn}` : null,
    description ? `Description: ${description}` : null,
    '',
    'You can view your updated balance in your Phone.System portal.',
    '',
    'Thank you for your business,',
    'Phone.System'
  ].filter(Boolean);

  const text = lines.join('\n');

  const htmlRows = [
    `<tr><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\"><b>Amount</b></td><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\">$${amt.toFixed(2)}</td></tr>`,
    inv ? `<tr><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\"><b>Invoice Number</b></td><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\">${inv}</td></tr>` : '',
    method ? `<tr><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\"><b>Payment Method</b></td><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\">${method}</td></tr>` : '',
    txn ? `<tr><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\"><b>Transaction ID</b></td><td style=\"padding:6px 10px;border-bottom:1px solid #e5e7eb;\">${txn}</td></tr>` : '',
    description ? `<tr><td style=\"padding:6px 10px;\"><b>Description</b></td><td style=\"padding:6px 10px;\">${description}</td></tr>` : ''
  ].filter(Boolean).join('');

  const html = `
  <div style=\"font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;\">\r
    <h2>Payment receipt</h2>\r
    <p>Hello ${safeUser},</p>\r
    <p>We have received your payment.</p>\r
    <table style=\"border:1px solid #e5e7eb;border-radius:10px;border-collapse:separate;border-spacing:0;overflow:hidden;max-width:560px;background:#ffffff\">\r
      <tbody>\r
        ${htmlRows}\r
      </tbody>\r
    </table>\r
    <p style=\"margin-top:16px;\">You can log in to your Phone.System portal to see your updated balance and billing history.</p>\r
    <p style=\"color:#555;margin-top:18px;\">Thank you for your business.</p>\r
  </div>`;

  const adminEmail = process.env.ADMIN_NOTIFY_EMAIL;
  const to = [toEmail].filter(Boolean);
  const payload = {
    api_key: apiKey,
    to,
    ...(adminEmail ? { bcc: [adminEmail] } : {}),
    sender,
    subject,
    text_body: text,
    html_body: html
  };
  await smtp2goSendEmail(payload, { kind: 'refill-receipt', to: toEmail, subject });
}

// Send a DID purchase receipt email when new numbers are purchased
// NOTE: This email only shows Phone.System markup monthly pricing, not wholesale amounts.
async function sendDidPurchaseReceiptEmail({ toEmail, displayName, items, totalAmount, orderReference }) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  if (!apiKey) throw new Error('SMTP2GO_API_KEY missing');

  const safeUser = displayName || 'Customer';
  const safeItems = Array.isArray(items) ? items : [];

  const primaryNumber = safeItems[0]?.number || '';
  const extraCount = Math.max(0, safeItems.length - 1);
  const subjectSuffix = primaryNumber
    ? extraCount > 0
      ? `${primaryNumber} + ${extraCount} more`
      : primaryNumber
    : `${safeItems.length} number(s)`;
  const subject = `Number purchase receipt - ${subjectSuffix}`;

  const numbersList = safeItems.length
    ? safeItems.map(item => {
        const num = item.number || '';
        const loc = item.location || 'Unknown location';
        const m = Number(item.monthlyPrice || 0);
        const s = Number(item.setupPrice || 0);
        const mFmt = `$${m.toFixed(2)}`;
        const sFmt = `$${s.toFixed(2)}`;
        return ` - ${num} — ${loc} — Monthly: ${mFmt}, Setup: ${sFmt}`;
      }).join('\n')
    : ' - (no numbers listed)';

  const orderLine = orderReference ? `Order: ${orderReference}\n` : '';

  const text = `Hello ${safeUser},\n\n` +
    `Thank you for your purchase. We have added the following number(s) to your Phone.System account:\n\n` +
    `${numbersList}\n\n` +
    `These numbers will be billed at the Phone.System monthly rates shown above.\n` +
    orderLine +
    `\nYou can now configure call routing for these numbers in your Phone.System portal.\n\n` +
    `Thank you for your business,\nPhone.System`;

  const htmlList = safeItems.length
    ? safeItems.map(item => {
        const num = item.number || '';
        const loc = item.location || 'Unknown location';
        const m = Number(item.monthlyPrice || 0);
        const s = Number(item.setupPrice || 0);
        const mFmt = `$${m.toFixed(2)}`;
        const sFmt = `$${s.toFixed(2)}`;
        return `<li><strong>${num}</strong>${loc ? ` — ${loc}` : ''} — Monthly: ${mFmt}, Setup: ${sFmt}</li>`;
      }).join('')
    : '<li>(no numbers listed)</li>';

  const html = `
  <div style=\"font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;\">\r
    <h2>Number purchase receipt</h2>\r
    <p>Hello ${safeUser},</p>\r
    <p>Thank you for your purchase. We have added the following number(s) to your Phone.System account:</p>\r
    <ul>${htmlList}</ul>\r
    ${orderReference ? `<p><strong>Order:</strong> ${orderReference}</p>` : ''}\r
    <p style=\"margin-top:16px;\">These numbers will be billed at the Phone.System monthly rates shown above.</p>\r
    <p style=\"margin-top:8px;\">You can now configure call routing for these numbers in your Phone.System portal.</p>\r
    <p style=\"color:#555;margin-top:18px;\">Thank you for your business.</p>\r
  </div>`;

  // DID purchase receipts go only to the customer, not the admin.
  const to = [toEmail].filter(Boolean);
  const payload = {
    api_key: apiKey,
    to,
    sender,
    subject,
    text_body: text,
    html_body: html
  };
  await smtp2goSendEmail(payload, { kind: 'did-purchase-receipt', to: toEmail, subject });
}

async function sendNonpaymentGraceInitialEmail({ toEmail, displayName, numbers, graceDays, cancelAfter, portalUrl, productLabel }) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  if (!apiKey) return { ok: false, reason: 'SMTP2GO_API_KEY missing' };
  if (!toEmail) return { ok: false, reason: 'missing_to_email' };

  const safeUser = displayName || 'Customer';
  const safeNumbers = Array.isArray(numbers) ? numbers.filter(Boolean) : [];
  const count = safeNumbers.length;

  const dateStr = cancelAfter && Number.isFinite(new Date(cancelAfter).getTime())
    ? new Date(cancelAfter).toISOString().slice(0, 10)
    : '';

  const label = productLabel || 'number';
  const subject = `Action required: add funds to keep your Phone.System ${label}${count === 1 ? '' : 's'}`;

  const listText = count
    ? safeNumbers.map(n => ` - ${n}`).join('\n')
    : ' - (no numbers listed)';

  const safePortal = portalUrl || process.env.PUBLIC_BASE_URL || '';
  const portalLine = safePortal ? `\nAdd funds / manage billing: ${safePortal}\n` : '';

  const text = `Hello ${safeUser},\n\n` +
    `We could not process your monthly service fee due to insufficient balance.\n\n` +
    `Affected ${label}${count === 1 ? '' : 's'}:\n${listText}\n\n` +
    `Please add funds within ${graceDays} day${graceDays === 1 ? '' : 's'} to avoid losing your ${label}${count === 1 ? '' : 's'}.` +
    (dateStr ? `\nIf not paid, these ${label}${count === 1 ? '' : 's'} may be released on or after ${dateStr}.` : '') +
    `${portalLine}\n` +
    `Thank you,\nPhone.System`;

  const htmlList = count
    ? safeNumbers.map(n => `<li><strong>${String(n)}</strong></li>`).join('')
    : '<li>(no numbers listed)</li>';

  const html = `
  <div style=\"font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;\">\r
    <h2>Action required: add funds to keep your ${label}${count === 1 ? '' : 's'}</h2>\r
    <p>Hello ${safeUser},</p>\r
    <p>We could not process your monthly service fee due to <strong>insufficient balance</strong>.</p>\r
    <p><strong>Affected ${label}${count === 1 ? '' : 's'}:</strong></p>\r
    <ul>${htmlList}</ul>\r
    <p>Please add funds within <strong>${graceDays} day${graceDays === 1 ? '' : 's'}</strong> to avoid losing your ${label}${count === 1 ? '' : 's'}.</p>\r
    ${dateStr ? `<p>If not paid, these ${label}${count === 1 ? '' : 's'} may be released on or after <strong>${dateStr}</strong>.</p>` : ''}\r
    ${safePortal ? `<p><a href=\"${safePortal}\" style=\"background:#4f46e5;color:#fff;padding:10px 14px;border-radius:8px;text-decoration:none;\">Add funds / manage billing</a></p>` : ''}\r
    <p style=\"color:#555;margin-top:18px;\">Thank you.</p>\r
  </div>`;

  const payload = {
    api_key: apiKey,
    to: [toEmail],
    sender,
    subject,
    text_body: text,
    html_body: html
  };

  try {
    await smtp2goSendEmail(payload, { kind: 'nonpayment-grace-initial', to: toEmail, subject });
    return { ok: true };
  } catch (e) {
    if (DEBUG) console.warn('[email.nonpayment.initial] Failed to send:', e?.message || e);
    return { ok: false, reason: 'send_failed', error: e?.message || e };
  }
}

async function sendNonpaymentGraceReminderEmail({ toEmail, displayName, numbers, graceDays, cancelAfter, portalUrl, productLabel }) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  if (!apiKey) return { ok: false, reason: 'SMTP2GO_API_KEY missing' };
  if (!toEmail) return { ok: false, reason: 'missing_to_email' };

  const safeUser = displayName || 'Customer';
  const safeNumbers = Array.isArray(numbers) ? numbers.filter(Boolean) : [];
  const count = safeNumbers.length;

  const dateStr = cancelAfter && Number.isFinite(new Date(cancelAfter).getTime())
    ? new Date(cancelAfter).toISOString().slice(0, 10)
    : '';

  const label = productLabel || 'number';
  const subject = `Reminder: less than 24 hours left to keep your Phone.System ${label}${count === 1 ? '' : 's'}`;

  const listText = count
    ? safeNumbers.map(n => ` - ${n}`).join('\n')
    : ' - (no numbers listed)';

  const safePortal = portalUrl || process.env.PUBLIC_BASE_URL || '';
  const portalLine = safePortal ? `\nAdd funds / manage billing: ${safePortal}\n` : '';

  const text = `Hello ${safeUser},\n\n` +
    `This is a reminder that you have less than 24 hours left to add funds to keep your ${label}${count === 1 ? '' : 's'}.\n\n` +
    `Affected ${label}${count === 1 ? '' : 's'}:\n${listText}\n\n` +
    (dateStr ? `These ${label}${count === 1 ? '' : 's'} may be released on or after ${dateStr}.\n` : '') +
    `${portalLine}\n` +
    `Thank you,\nPhone.System`;

  const htmlList = count
    ? safeNumbers.map(n => `<li><strong>${String(n)}</strong></li>`).join('')
    : '<li>(no numbers listed)</li>';

  const html = `
  <div style=\"font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;\">\r
    <h2>Reminder: less than 24 hours left</h2>\r
    <p>Hello ${safeUser},</p>\r
    <p>This is a reminder that you have <strong>less than 24 hours</strong> left to add funds to keep your ${label}${count === 1 ? '' : 's'}.</p>\r
    <p><strong>Affected ${label}${count === 1 ? '' : 's'}:</strong></p>\r
    <ul>${htmlList}</ul>\r
    ${dateStr ? `<p>These ${label}${count === 1 ? '' : 's'} may be released on or after <strong>${dateStr}</strong>.</p>` : ''}\r
    ${safePortal ? `<p><a href=\"${safePortal}\" style=\"background:#4f46e5;color:#fff;padding:10px 14px;border-radius:8px;text-decoration:none;\">Add funds / manage billing</a></p>` : ''}\r
    <p style=\"color:#555;margin-top:18px;\">Thank you.</p>\r
  </div>`;

  const payload = {
    api_key: apiKey,
    to: [toEmail],
    sender,
    subject,
    text_body: text,
    html_body: html
  };

  try {
    await smtp2goSendEmail(payload, { kind: 'nonpayment-grace-reminder', to: toEmail, subject });
    return { ok: true };
  } catch (e) {
    if (DEBUG) console.warn('[email.nonpayment.reminder] Failed to send:', e?.message || e);
    return { ok: false, reason: 'send_failed', error: e?.message || e };
  }
}

// Health check endpoint for DigitalOcean
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy' });
});

// Used by signup flow to show the detected client IP (server-derived)
app.get('/api/client-ip', (req, res) => {
  res.set('Cache-Control', 'no-store');
  return res.json({ success: true, ip: getClientIp(req) });
});

// Optional debug endpoint (disabled in production)
if (DEBUG) {
  app.get('/debug/env', (req, res) => {
    res.json({
      NODE_ENV: process.env.NODE_ENV,
      PORT: process.env.PORT,
      DB_HOST: process.env.DB_HOST || null
    });
  });
}

// Redirect root to login (or dashboard if already authenticated)
app.get('/', (req, res) => {
  if (req.session && req.session.userId) return res.redirect('/dashboard');
  res.redirect('/login');
});

// Serve the signup page at /signup
app.get('/signup', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve sidebar icon assets
app.use('/Icon', express.static(path.join(__dirname, 'Icon')));

// Serve static files from public directory (after route handlers to prevent index.html from being served at root)
app.use(express.static('public'));

// Login + Dashboard
app.get('/login', (req, res) => {
  if (req.session && req.session.userId) return res.redirect('/dashboard');
  res.render('login', { error: null });
});
app.post('/login', loginLimiter, async (req, res) => {
  try {
    const { email, username, password } = req.body || {};
    const ident = (email || username || '').trim();
    const pwd = String(password || '');
    if (!ident || !pwd) return res.status(400).render('login', { error: 'Missing email/username or password' });
    if (!pool) return res.status(500).render('login', { error: 'Database not configured' });
    // Use new local users table
    const user = await getUserByUsernameOrEmail(ident);
    if (!user || !user.password_hash) return res.status(401).render('login', { error: 'Invalid credentials' });
    const ok = await bcrypt.compare(pwd, user.password_hash);
    if (!ok) return res.status(401).render('login', { error: 'Invalid credentials' });
    if (DEBUG) console.log('[login] User from DB:', { id: user.id, username: user.username, email: user.email });
    // Regenerate session to avoid session fixation / stale data reuse
    req.session.regenerate((err) => {
      if (err) { return res.status(500).render('login', { error: 'Session error' }); }
      req.session.userId = String(user.id);
      req.session.username = user.username;
      req.session.email = user.email;
      req.session.save((saveErr) => {
        if (saveErr) { return res.status(500).render('login', { error: 'Session save error' }); }
        if (DEBUG) console.log('[login.ok]', { sid: req.sessionID, userId: req.session.userId, username: req.session.username });
        return res.redirect('/dashboard');
      });
    });
  } catch (e) {
    if (DEBUG) console.warn('login error', e.message || e);
    return res.status(500).render('login', { error: 'Login failed' });
  }
});
app.post('/logout', (req, res) => { try { req.session.destroy(()=>{}); } catch {} res.redirect('/login'); });

// ========== Forgot Password ==========
const PASSWORD_RESET_TTL_MS = (parseInt(process.env.PASSWORD_RESET_TTL_MINUTES) || 15) * 60 * 1000;
const PASSWORD_RESET_MAX_ATTEMPTS = parseInt(process.env.PASSWORD_RESET_MAX_ATTEMPTS || '5', 10);

async function sendPasswordResetEmail(toEmail, code) {
  const apiKey = process.env.SMTP2GO_API_KEY;
  const sender = process.env.SMTP2GO_SENDER || `no-reply@${(process.env.SENDER_DOMAIN || 'Phone.System.net')}`;
  if (!apiKey) throw new Error('SMTP2GO_API_KEY missing');
  const subject = 'Phone.System Password Reset Code';
  const minutes = Math.round(PASSWORD_RESET_TTL_MS / 60000);
  const text = `Your password reset code is ${code}. It expires in ${minutes} minutes.\n\nIf you did not request this, please ignore this email.`;
  const html = `<p>Your password reset code is <b>${code}</b>.</p><p>This code expires in ${minutes} minutes.</p><p style="color:#666;">If you did not request this, please ignore this email.</p>`;
  const payload = { api_key: apiKey, to: [toEmail], sender, subject, text_body: text, html_body: html };
  await smtp2goSendEmail(payload, { kind: 'password-reset', to: toEmail, subject });
}

// Request password reset - sends code to email
app.post('/api/forgot-password', otpSendLimiter, async (req, res) => {
  try {
    const { email } = req.body || {};
    const emailTrimmed = String(email || '').trim().toLowerCase();
    
    if (!emailTrimmed) {
      return res.status(400).json({ success: false, message: 'Email is required' });
    }
    
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    // Find user by email from local users table
    const [rows] = await pool.execute(
      'SELECT id, email, username FROM users WHERE email = ? AND is_active = 1 LIMIT 1',
      [emailTrimmed]
    );
    const user = rows && rows[0];
    
    // Always return success to prevent email enumeration
    // But only actually send email if user exists
    if (!user) {
      if (DEBUG) console.log('[forgot-password] Email not found:', emailTrimmed);
      return res.json({ success: true, message: 'If an account exists with that email, a reset code has been sent.' });
    }
    
    // Generate token and code
    const token = crypto.randomBytes(16).toString('hex');
    const code = otp();
    const codeHash = sha256(code);
    const expiresAt = Date.now() + PASSWORD_RESET_TTL_MS;
    
    // Store reset token
    await pool.execute(
      `INSERT INTO password_resets (token, user_id, email, code_hash, expires_at, used, attempts)
       VALUES (?, ?, ?, ?, ?, 0, 0)`,
      [token, user.id, user.email, codeHash, expiresAt]
    );
    
    // Send email
    await sendPasswordResetEmail(user.email, code);
    if (DEBUG) console.log('[forgot-password] Reset code sent:', { email: user.email, token });
    
    return res.json({ success: true, token, message: 'If an account exists with that email, a reset code has been sent.' });
  } catch (e) {
    console.error('[forgot-password] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to process request' });
  }
});

// Complete password reset - verify code and update password
app.post('/api/reset-password', otpVerifyLimiter, async (req, res) => {
  try {
    const { token, code, password } = req.body || {};
    
    if (!token || !code || !password) {
      return res.status(400).json({ success: false, message: 'Token, code, and new password are required' });
    }
    
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    // Validate password format (same rules as signup)
    if (!passwordIsAlnumMax10(password)) {
      return res.status(400).json({ success: false, message: 'Password must be letters and numbers only, maximum 10 characters.' });
    }
    
    // Fetch reset record
    const [rows] = await pool.execute(
      'SELECT * FROM password_resets WHERE token = ? LIMIT 1',
      [token]
    );
    const rec = rows && rows[0];
    
    if (!rec) {
      return res.status(400).json({ success: false, message: 'Invalid or expired reset token' });
    }
    
    if (rec.used) {
      return res.status(400).json({ success: false, message: 'This reset code was already used' });
    }
    
    if (Date.now() > Number(rec.expires_at)) {
      await pool.execute('UPDATE password_resets SET used = 1 WHERE token = ?', [token]);
      return res.status(400).json({ success: false, message: 'Reset code expired. Please request a new one.' });
    }
    
    const attempts = Number(rec.attempts || 0);
    if (attempts >= PASSWORD_RESET_MAX_ATTEMPTS) {
      await pool.execute('UPDATE password_resets SET used = 1 WHERE token = ?', [token]);
      return res.status(400).json({ success: false, message: 'Too many invalid attempts. Please request a new reset code.' });
    }
    
    // Verify code
    if (sha256(code) !== rec.code_hash) {
      await pool.execute('UPDATE password_resets SET attempts = attempts + 1 WHERE token = ?', [token]);
      if (attempts + 1 >= PASSWORD_RESET_MAX_ATTEMPTS) {
        await pool.execute('UPDATE password_resets SET used = 1 WHERE token = ?', [token]);
        return res.status(400).json({ success: false, message: 'Too many invalid attempts. Please request a new reset code.' });
      }
      return res.status(400).json({ success: false, message: 'Invalid code' });
    }
    
    // Code is valid - update password in local users table
    const passwordHash = await bcrypt.hash(password, 12);
    
    await pool.execute(
      'UPDATE users SET password_hash = ?, updated_at = NOW() WHERE id = ?',
      [passwordHash, rec.user_id]
    );
    
    // Mark reset as used
    await pool.execute('UPDATE password_resets SET used = 1 WHERE token = ?', [token]);
    
    // Clean up old reset tokens for this user
    try {
      await pool.execute('DELETE FROM password_resets WHERE user_id = ? AND used = 1 AND token != ?', [rec.user_id, token]);
    } catch {}
    
    if (DEBUG) console.log('[reset-password] Password updated:', { userId: rec.user_id, email: rec.email });
    
    return res.json({ success: true, message: 'Password updated successfully. You can now log in.' });
  } catch (e) {
    console.error('[reset-password] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to reset password' });
  }
});

app.get('/dashboard', requireAuth, (req, res) => {
  const localMarkup = parseFloat(process.env.DID_LOCAL_MONTHLY_MARKUP || '10.20') || 0;
  const tollfreeMarkup = parseFloat(process.env.DID_TOLLFREE_MONTHLY_MARKUP || '25.20') || 0;

  const aiLocalFee = parseFloat(process.env.AI_DID_LOCAL_MONTHLY_FEE || process.env.DID_LOCAL_MONTHLY_MARKUP || '10.20') || 0;
  const aiTollfreeFee = parseFloat(process.env.AI_DID_TOLLFREE_MONTHLY_FEE || process.env.DID_TOLLFREE_MONTHLY_MARKUP || '25.20') || 0;

  const checkoutMin = Number.isFinite(CHECKOUT_MIN_AMOUNT) ? CHECKOUT_MIN_AMOUNT : 100;
  const checkoutMax = Number.isFinite(CHECKOUT_MAX_AMOUNT) ? CHECKOUT_MAX_AMOUNT : 500;
  res.render('dashboard', {
    username: req.session.username || '',
    localDidMarkup: localMarkup,
    tollfreeDidMarkup: tollfreeMarkup,
    aiLocalDidFee: aiLocalFee,
    aiTollfreeDidFee: aiTollfreeFee,
    checkoutMinAmount: checkoutMin,
    checkoutMaxAmount: checkoutMax,
  });
});

// ========== Admin Dashboard ==========
app.get('/admin', requireAdmin, (req, res) => {
  res.render('admin', {
    username: req.session.username || '',
  });
});

// Admin API: Get platform-wide statistics
app.get('/api/admin/stats', requireAdmin, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    // Helper to safely query - returns default if table doesn't exist or query fails
    const safeQuery = async (sql, params = [], defaultVal = {}) => {
      try {
        const [rows] = await pool.execute(sql, params);
        return rows;
      } catch (e) {
        if (DEBUG) console.warn('[admin.stats] Query failed:', e.message);
        return [defaultVal];
      }
    };

    // User stats from local users table
    const [userStatsRows] = await safeQuery(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 1 ELSE 0 END) as newThisMonth,
        SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as suspended
      FROM users
    `, [], { total: 0, newThisMonth: 0, suspended: 0 });
    const userStats = userStatsRows || { total: 0, newThisMonth: 0, suspended: 0 };

    // Active users (had activity in last 30 days via billing_history or ai_call_logs)
    const [activeUsersRows] = await safeQuery(`
      SELECT COUNT(DISTINCT user_id) as count FROM (
        SELECT user_id FROM billing_history WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        UNION
        SELECT user_id FROM ai_call_logs WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
      ) active
    `, [], { count: 0 });
    const activeUsers = activeUsersRows || { count: 0 };

    // Financial stats - use safeQuery for all to handle missing tables
    const [stripeStatsRows] = await safeQuery(`
      SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count 
      FROM stripe_payments WHERE status='completed' AND credited=1
    `, [], { total: 0, count: 0 });
    const stripeStats = stripeStatsRows || { total: 0, count: 0 };

    const [cryptoStatsRows] = await safeQuery(`
      SELECT COALESCE(SUM(price_amount), 0) as total, COUNT(*) as count 
      FROM nowpayments_payments WHERE payment_status IN ('finished','confirmed') AND credited=1
    `, [], { total: 0, count: 0 });
    const cryptoStats = cryptoStatsRows || { total: 0, count: 0 };

    const [squareStatsRows] = await safeQuery(`
      SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count 
      FROM square_payments WHERE status='completed' AND credited=1
    `, [], { total: 0, count: 0 });
    const squareStats = squareStatsRows || { total: 0, count: 0 };

    const [billcomStatsRows] = await safeQuery(`
      SELECT COUNT(*) as count FROM billcom_payments WHERE status='paid' AND credited=1
    `, [], { count: 0 });
    const billcomStats = billcomStatsRows || { count: 0 };

    // Pending payments - count each individually for resilience
    const [pendingStripe] = await safeQuery(`SELECT COUNT(*) as c FROM stripe_payments WHERE status='pending'`, [], { c: 0 });
    const [pendingCrypto] = await safeQuery(`SELECT COUNT(*) as c FROM nowpayments_payments WHERE payment_status='waiting'`, [], { c: 0 });
    const [pendingSquare] = await safeQuery(`SELECT COUNT(*) as c FROM square_payments WHERE status='pending'`, [], { c: 0 });
    const pendingPayments = { count: (Number(pendingStripe?.c) || 0) + (Number(pendingCrypto?.c) || 0) + (Number(pendingSquare?.c) || 0) };

    // Telephony stats
    const [didStatsRows] = await safeQuery(`
      SELECT COUNT(*) as total, SUM(CASE WHEN cancel_pending=0 THEN 1 ELSE 0 END) as active
      FROM user_dids
    `, [], { total: 0, active: 0 });
    const didStats = didStatsRows || { total: 0, active: 0 };

    const [aiNumberStatsRows] = await safeQuery(`
      SELECT COUNT(*) as total, SUM(CASE WHEN cancel_pending=0 THEN 1 ELSE 0 END) as active
      FROM ai_numbers
    `, [], { total: 0, active: 0 });
    const aiNumberStats = aiNumberStatsRows || { total: 0, active: 0 };

    const [trunkStatsRows] = await safeQuery(`SELECT COUNT(*) as total FROM user_trunks`, [], { total: 0 });
    const trunkStats = trunkStatsRows || { total: 0 };
    
    // Call stats (AI calls)
    const [aiCallStatsRows] = await safeQuery(`
      SELECT 
        COUNT(*) as totalCalls,
        COALESCE(SUM(CASE WHEN direction='inbound' THEN 1 ELSE 0 END), 0) as inboundCalls,
        COALESCE(SUM(CASE WHEN direction='outbound' THEN 1 ELSE 0 END), 0) as outboundCalls,
        COALESCE(SUM(duration), 0) as totalSeconds,
        COALESCE(SUM(price), 0) as totalRevenue
      FROM ai_call_logs
    `, [], { totalCalls: 0, inboundCalls: 0, outboundCalls: 0, totalSeconds: 0, totalRevenue: 0 });
    const aiCallStats = aiCallStatsRows || { totalCalls: 0, inboundCalls: 0, outboundCalls: 0, totalSeconds: 0, totalRevenue: 0 };

    // AI Platform stats
    const [agentStatsRows] = await safeQuery(`SELECT COUNT(*) as total FROM ai_agents`, [], { total: 0 });
    const agentStats = agentStatsRows || { total: 0 };

    const [conversationStatsRows] = await safeQuery(`
      SELECT COUNT(DISTINCT call_id) as total FROM ai_call_messages
    `, [], { total: 0 });
    const conversationStats = conversationStatsRows || { total: 0 };
    
    // Email stats
    const [emailStatsRows] = await safeQuery(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending
      FROM ai_email_sends
    `, [], { total: 0, completed: 0, failed: 0, pending: 0 });
    const emailStats = emailStatsRows || { total: 0, completed: 0, failed: 0, pending: 0 };
    
    // SMS stats
    const [smsStatsRows] = await safeQuery(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending
      FROM ai_sms_sends
    `, [], { total: 0, completed: 0, failed: 0, pending: 0 });
    const smsStats = smsStatsRows || { total: 0, completed: 0, failed: 0, pending: 0 };
    
    // Physical mail stats
    const [mailStatsRows] = await safeQuery(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status='submitted' THEN 1 ELSE 0 END) as submitted,
        SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending
      FROM ai_mail_sends
    `, [], { total: 0, completed: 0, submitted: 0, failed: 0, pending: 0 });
    const mailStats = mailStatsRows || { total: 0, completed: 0, submitted: 0, failed: 0, pending: 0 };

    // Meeting links (emails with meeting_room_url)
    const [meetingStatsRows] = await safeQuery(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
      FROM ai_email_sends WHERE meeting_room_url IS NOT NULL
    `, [], { total: 0, completed: 0, failed: 0 });
    const meetingStats = meetingStatsRows || { total: 0, completed: 0, failed: 0 };

    // Dialer stats
    const [dialerStatsRows] = await safeQuery(`
      SELECT 
        COUNT(*) as campaigns,
        SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) as running,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed
      FROM dialer_campaigns WHERE status != 'deleted'
    `, [], { campaigns: 0, running: 0, completed: 0 });
    const dialerStats = dialerStatsRows || { campaigns: 0, running: 0, completed: 0 };

    const [dialerLeadStatsRows] = await safeQuery(`
      SELECT COUNT(*) as total FROM dialer_leads
    `, [], { total: 0 });
    const dialerLeadStats = dialerLeadStatsRows || { total: 0 };

    // Recent activity (last 7 days revenue)
    const recentRevenue = await safeQuery(`
      SELECT DATE(created_at) as date, SUM(amount) as amount
      FROM billing_history
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND amount > 0
      GROUP BY DATE(created_at)
      ORDER BY date ASC
    `, [], null) || [];

    const totalRevenue = Number(stripeStats.total) + Number(cryptoStats.total) + Number(squareStats.total);

    return res.json({
      success: true,
      data: {
        users: {
          total: Number(userStats.total) || 0,
          active30d: Number(activeUsers.count) || 0,
          newThisMonth: Number(userStats.newThisMonth) || 0,
          suspended: Number(userStats.suspended) || 0
        },
        financial: {
          totalRevenue,
          revenueByMethod: {
            stripe: { total: Number(stripeStats.total) || 0, count: Number(stripeStats.count) || 0 },
            crypto: { total: Number(cryptoStats.total) || 0, count: Number(cryptoStats.count) || 0 },
            square: { total: Number(squareStats.total) || 0, count: Number(squareStats.count) || 0 },
            billcom: { count: Number(billcomStats.count) || 0 }
          },
          pendingPayments: Number(pendingPayments.count) || 0
        },
        telephony: {
          dids: { total: Number(didStats.total) || 0, active: Number(didStats.active) || 0 },
          aiNumbers: { total: Number(aiNumberStats.total) || 0, active: Number(aiNumberStats.active) || 0 },
          trunks: Number(trunkStats.total) || 0,
          calls: {
            total: Number(aiCallStats.totalCalls) || 0,
            inbound: Number(aiCallStats.inboundCalls) || 0,
            outbound: Number(aiCallStats.outboundCalls) || 0,
            minutes: Math.round((Number(aiCallStats.totalSeconds) || 0) / 60),
            revenue: Number(aiCallStats.totalRevenue) || 0
          }
        },
        ai: {
          agents: Number(agentStats.total) || 0,
          conversations: Number(conversationStats.total) || 0,
          emails: { total: Number(emailStats.total) || 0, completed: Number(emailStats.completed) || 0, failed: Number(emailStats.failed) || 0, pending: Number(emailStats.pending) || 0 },
          sms: { total: Number(smsStats.total) || 0, completed: Number(smsStats.completed) || 0, failed: Number(smsStats.failed) || 0, pending: Number(smsStats.pending) || 0 },
          mail: { total: Number(mailStats.total) || 0, completed: Number(mailStats.completed) || 0, submitted: Number(mailStats.submitted) || 0, failed: Number(mailStats.failed) || 0, pending: Number(mailStats.pending) || 0 },
          meetings: { total: Number(meetingStats.total) || 0, completed: Number(meetingStats.completed) || 0, failed: Number(meetingStats.failed) || 0 },
          dialer: { campaigns: Number(dialerStats.campaigns) || 0, running: Number(dialerStats.running) || 0, completed: Number(dialerStats.completed) || 0, leads: Number(dialerLeadStats.total) || 0 }
        },
        recentRevenue
      }
    });
  } catch (e) {
    console.error('[admin.stats] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch stats' });
  }
});

// Admin API: List all users with pagination and search
app.get('/api/admin/users', requireAdmin, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit || '25', 10)));
    const offset = page * limit;
    const search = (req.query.search || req.query.q || '').toString().trim();
    const sortBy = (req.query.sort || 'created_at').toString();
    const sortDir = (req.query.dir || 'DESC').toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    
    const allowedSorts = ['id', 'username', 'email', 'created_at', 'is_admin', 'suspended'];
    const sortCol = allowedSorts.includes(sortBy) ? sortBy : 'created_at';
    
    let whereClause = '';
    const params = [];
    if (search) {
      whereClause = 'WHERE username LIKE ? OR email LIKE ? OR firstname LIKE ? OR lastname LIKE ?';
      const searchPattern = `%${search}%`;
      params.push(searchPattern, searchPattern, searchPattern, searchPattern);
    }
    
    const [[countResult]] = await pool.execute(
      `SELECT COUNT(*) as total FROM signup_users ${whereClause}`,
      params
    );
    
    // Use query() instead of execute() for LIMIT/OFFSET - they need to be interpolated as integers
    const [users] = await pool.query(
      `SELECT id, username, email, firstname, lastname, phone, is_admin, suspended, created_at
       FROM signup_users ${whereClause}
       ORDER BY ${sortCol} ${sortDir}
       LIMIT ${Number(limit)} OFFSET ${Number(offset)}`,
      params
    );
    
    // Fetch additional data for each user
    const userIds = users.map(u => u.id);
    if (userIds.length > 0) {
      const placeholders = userIds.map(() => '?').join(',');
      
      // Get DID counts
      const [didCounts] = await pool.execute(
        `SELECT user_id, COUNT(*) as count FROM user_dids WHERE user_id IN (${placeholders}) GROUP BY user_id`,
        userIds
      );
      const didMap = Object.fromEntries(didCounts.map(r => [r.user_id, r.count]));
      
      // Get AI number counts
      const [aiNumCounts] = await pool.execute(
        `SELECT user_id, COUNT(*) as count FROM ai_numbers WHERE user_id IN (${placeholders}) GROUP BY user_id`,
        userIds
      );
      const aiNumMap = Object.fromEntries(aiNumCounts.map(r => [r.user_id, r.count]));
      
      // Get agent counts
      const [agentCounts] = await pool.execute(
        `SELECT user_id, COUNT(*) as count FROM ai_agents WHERE user_id IN (${placeholders}) GROUP BY user_id`,
        userIds
      );
      const agentMap = Object.fromEntries(agentCounts.map(r => [r.user_id, r.count]));
      
      // Attach counts to users
      for (const u of users) {
        u.dids = didMap[u.id] || 0;
        u.aiNumbers = aiNumMap[u.id] || 0;
        u.agents = agentMap[u.id] || 0;
      }
    }
    
    return res.json({
      success: true,
      data: {
        users,
        pagination: {
          page,
          limit,
          total: Number(countResult.total) || 0,
          totalPages: Math.ceil((Number(countResult.total) || 0) / limit)
        }
      }
    });
  } catch (e) {
    console.error('[admin.users] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch users' });
  }
});

// Admin API: Get user detail
app.get('/api/admin/users/:id', requireAdmin, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    const userId = req.params.id;
    const [[user]] = await pool.execute(
      `SELECT id, username, email, firstname, lastname, phone, address1, city, state, postal_code, country, is_admin, suspended, signup_ip, created_at
       FROM signup_users WHERE id = ? LIMIT 1`,
      [userId]
    );
    
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    
    // Fetch related data
    const [dids] = await pool.execute(
      `SELECT id, did_number, country, did_type, monthly_price, cancel_pending, created_at FROM user_dids WHERE user_id = ?`,
      [userId]
    );
    const [aiNumbers] = await pool.execute(
      `SELECT id, phone_number, agent_id, cancel_pending, created_at FROM ai_numbers WHERE user_id = ?`,
      [userId]
    );
    const [agents] = await pool.execute(
      `SELECT id, display_name, created_at FROM ai_agents WHERE user_id = ?`,
      [userId]
    );
    const [recentBilling] = await pool.execute(
      `SELECT id, amount, description, status, created_at FROM billing_history WHERE user_id = ? ORDER BY created_at DESC LIMIT 20`,
      [userId]
    );
    const [recentCalls] = await pool.execute(
      `SELECT id, direction, from_number, to_number, duration, price, status, time_start FROM ai_call_logs WHERE user_id = ? ORDER BY time_start DESC LIMIT 20`,
      [userId]
    );
    
    // Get balance from local database
    let balance = null;
    try {
      balance = await getUserBalance(userId);
      if (balance != null) balance = Number(balance);
    } catch (e) {
      if (DEBUG) console.warn('[admin.user.balance]', e.message || e);
    }
    
    return res.json({
      success: true,
      data: {
        user: { ...user, balance },
        dids,
        aiNumbers,
        agents,
        recentBilling,
        recentCalls
      }
    });
  } catch (e) {
    console.error('[admin.user.detail] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch user details' });
  }
});

// Admin API: Suspend user
app.post('/api/admin/users/:id/suspend', requireAdmin, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    const userId = req.params.id;
    const adminUserId = req.session.userId;
    
    // Prevent self-suspension
    if (String(userId) === String(adminUserId)) {
      return res.status(400).json({ success: false, message: 'Cannot suspend your own account' });
    }
    
    await pool.execute('UPDATE signup_users SET suspended = 1 WHERE id = ?', [userId]);
    if (DEBUG) console.log('[admin.suspend]', { userId, by: adminUserId });
    
    return res.json({ success: true, message: 'User suspended' });
  } catch (e) {
    console.error('[admin.suspend] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to suspend user' });
  }
});

// Admin API: Unsuspend user
app.post('/api/admin/users/:id/unsuspend', requireAdmin, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    const userId = req.params.id;
    await pool.execute('UPDATE signup_users SET suspended = 0 WHERE id = ?', [userId]);
    if (DEBUG) console.log('[admin.unsuspend]', { userId, by: req.session.userId });
    
    return res.json({ success: true, message: 'User unsuspended' });
  } catch (e) {
    console.error('[admin.unsuspend] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to unsuspend user' });
  }
});

// Admin API: Toggle admin status
app.post('/api/admin/users/:id/toggle-admin', requireAdmin, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    const userId = req.params.id;
    const adminUserId = req.session.userId;
    
    // Prevent self-demotion
    if (String(userId) === String(adminUserId)) {
      return res.status(400).json({ success: false, message: 'Cannot change your own admin status' });
    }
    
    const [[user]] = await pool.execute('SELECT is_admin FROM signup_users WHERE id = ? LIMIT 1', [userId]);
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    
    const newStatus = user.is_admin ? 0 : 1;
    await pool.execute('UPDATE signup_users SET is_admin = ? WHERE id = ?', [newStatus, userId]);
    if (DEBUG) console.log('[admin.toggle-admin]', { userId, newStatus, by: adminUserId });
    
    return res.json({ success: true, message: newStatus ? 'User is now an admin' : 'Admin privileges removed', isAdmin: !!newStatus });
  } catch (e) {
    console.error('[admin.toggle-admin] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to update admin status' });
  }
});

// Admin API: Adjust user balance (via billing system)
app.post('/api/admin/users/:id/adjust-balance', requireAdmin, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    const userId = req.params.id;
    const amount = parseFloat(req.body.amount);
    const description = (req.body.description || 'Admin balance adjustment').toString().slice(0, 255);
    
    if (!Number.isFinite(amount) || amount === 0) {
      return res.status(400).json({ success: false, message: 'Invalid amount' });
    }
    
    // Apply balance adjustment via local database
    let result;
    if (amount > 0) {
      result = await creditBalance(userId, amount, `[Admin] ${description}`);
    } else {
      result = await deductBalance(userId, Math.abs(amount), `[Admin] ${description}`);
    }
    
    if (DEBUG) console.log('[admin.adjust-balance]', { userId, amount, description, result, by: req.session.userId });
    
    if (!result || !result.success) {
      const reason = result?.error === 'insufficient_funds' 
        ? 'Insufficient balance for deduction' 
        : (result?.error || 'Balance adjustment failed');
      return res.status(500).json({ success: false, message: reason });
    }
    
    return res.json({ success: true, message: `Balance adjusted by $${amount.toFixed(2)}` });
  } catch (e) {
    console.error('[admin.adjust-balance] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to adjust balance' });
  }
});

// Admin API: Integration health check
app.get('/api/admin/integrations', requireAdmin, async (req, res) => {
  const integrations = {};
  
  // Helper for timeout
  const withTimeout = (promise, ms, fallback) => {
    return Promise.race([
      promise,
      new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), ms))
    ]).catch(() => fallback);
  };
  
  // Check Daily (Pipecat)
  try {
    if (process.env.DAILY_API_KEY) {
      integrations.daily = { status: 'ok', message: 'API key configured' };
    } else {
      integrations.daily = { status: 'warning', message: 'API key not configured' };
    }
  } catch (e) {
    integrations.daily = { status: 'error', message: e.message };
  }
  
  // Check Stripe
  try {
    if (process.env.STRIPE_SECRET_KEY) {
      integrations.stripe = { status: 'ok', message: 'Secret key configured' };
    } else {
      integrations.stripe = { status: 'warning', message: 'Secret key not configured' };
    }
  } catch (e) {
    integrations.stripe = { status: 'error', message: e.message };
  }
  
  // Check SMTP
  try {
    if (process.env.SMTP2GO_API_KEY) {
      integrations.smtp = { status: 'ok', message: 'SMTP2GO configured' };
    } else {
      integrations.smtp = { status: 'warning', message: 'SMTP not configured' };
    }
  } catch (e) {
    integrations.smtp = { status: 'error', message: e.message };
  }
  
  // Check Click2Mail
  try {
    if (CLICK2MAIL_USERNAME && CLICK2MAIL_PASSWORD) {
      integrations.click2mail = { status: 'ok', message: 'Credentials configured' };
    } else {
      integrations.click2mail = { status: 'warning', message: 'Credentials not configured' };
    }
  } catch (e) {
    integrations.click2mail = { status: 'error', message: e.message };
  }
  
  // Database check
  try {
    if (pool) {
      await pool.execute('SELECT 1');
      integrations.database = { status: 'ok', message: 'Connected' };
    } else {
      integrations.database = { status: 'error', message: 'Pool not initialized' };
    }
  } catch (e) {
    integrations.database = { status: 'error', message: e.message };
  }
  
  return res.json({ success: true, data: integrations });
});
// Guard: deleting the primary signup user is disabled via API as well
app.delete('/api/me', requireAuth, (req, res)=>{
  return res.status(403).json({ success:false, message:'Deleting the primary signup user is disabled.' });
});

// Admin: list users and balances from local database (guarded by ADMIN_TOKEN)
app.get('/api/admin/users-balances', async (req, res) => {
  try {
    const token = req.headers['x-admin-token'] || req.headers['authorization']?.replace(/^Bearer\s+/i,'');
    if (!process.env.ADMIN_TOKEN || token !== process.env.ADMIN_TOKEN) {
      return res.status(403).json({ success: false, message: 'Forbidden' });
    }
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const limit = Math.min(500, Math.max(1, parseInt(req.query.limit || '100', 10)));
    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const start = page * limit;
    const q = (req.query.q || '').toString().trim();
    let query = 'SELECT id, username, balance AS credit FROM signup_users';
    const params = [];
    if (q) {
      query += ' WHERE username LIKE ? OR email LIKE ?';
      params.push(`%${q}%`, `%${q}%`);
    }
    query += ` ORDER BY id DESC LIMIT ${limit} OFFSET ${start}`;
    const [raw] = await pool.execute(query, params);
    const rows = (raw || []).map(r => ({ id: r.id, username: r.username || '', credit: Number(r.credit ?? 0) }));
    return res.json({ success: true, rows, page, limit });
  } catch (e) {
    console.error('users-balances api error', e.message || e);
    return res.status(500).json({ success: false, message: 'Query failed' });
  }
});

// Protected API proxies
// Profile from local users table (name/email/phone/balance)
app.get('/api/me/profile', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    
    // Fetch user from local users table
    const user = await getUserById(userId);
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    
    // Get balance from local database
    let balance = user.balance || 0;
    
    // Cache balance in session
    try {
      req.session.balance = balance;
      req.session.balanceAt = Date.now();
    } catch {}
    
    // If the balance is NEGATIVE, immediately unassign AI numbers (DIDs) from agents
    if (Number.isFinite(balance) && balance < 0) {
      try {
        await unassignAiNumbersForUser({ localUserId: userId });
      } catch (e) {
        if (DEBUG) console.warn('[ai.numbers.unassignAll] failed during profile fetch:', e?.message || e);
      }
    }
    
    if (DEBUG) console.log('[profile] User data fetched:', { userId, username: user.username, balance });
    
    return res.json({
      success: true,
      data: {
        name: user.name || '',
        email: user.email || '',
        phone: user.phone || '',
        username: user.username || '',
        address1: user.address1 || '',
        city: user.city || '',
        state: user.state || '',
        postalCode: user.postal_code || '',
        country: user.country || '',
        signupIp: user.signup_ip || '',
        balance
      }
    });
  } catch (e) {
    if (DEBUG) console.error('[profile] Error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Profile fetch failed' });
  }
});

// Save user address fields (stored in signup_users)
app.put('/api/me/profile/address', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const address1Raw = (req.body && (req.body.address1 ?? req.body.address ?? req.body.street_address ?? req.body.streetAddress)) ?? '';
    const cityRaw = (req.body && (req.body.city ?? req.body.town ?? req.body.locality)) ?? '';
    const stateRaw = (req.body && (req.body.state ?? req.body.state_province ?? req.body.stateProvince ?? req.body.province)) ?? '';
    const postalRaw = (req.body && (req.body.postalCode ?? req.body.postal_code ?? req.body.zip ?? req.body.zip_code ?? req.body.zipCode)) ?? '';
    const countryRaw = (req.body && (req.body.country ?? req.body.countryCode ?? req.body.country_code)) ?? '';

    const address1 = String(address1Raw || '').trim();
    const city = String(cityRaw || '').trim();
    const state = String(stateRaw || '').trim();

    // Country is stored as 2-letter code (CHAR(2)). Allow empty.
    const rawCountry = String(countryRaw || '').trim();
    let country = '';
    if (rawCountry) {
      const up = rawCountry.toUpperCase();
      const mapped = normalizeSignupCountry(up);
      if (mapped) country = mapped;
      else if (/^[A-Z]{2}$/.test(up)) country = up;
      else {
        return res.status(400).json({ success: false, message: 'Country must be a 2-letter code like US or CA' });
      }
    }

    const postalCode = normalizePostalCode(postalRaw, { country });

    if (address1.length > 255) return res.status(400).json({ success: false, message: 'Street address is too long' });
    if (city.length > 100) return res.status(400).json({ success: false, message: 'City is too long' });
    if (state.length > 100) return res.status(400).json({ success: false, message: 'State/Province is too long' });
    if (postalCode.length > 32) return res.status(400).json({ success: false, message: 'ZIP/Postal Code is too long' });

    await pool.execute(
      'UPDATE users SET address1 = ?, city = ?, state = ?, postal_code = ?, country = ?, updated_at = NOW() WHERE id = ? LIMIT 1',
      [
        address1 ? address1 : null,
        city ? city : null,
        state ? state : null,
        postalCode ? postalCode : null,
        country ? country : null,
        userId
      ]
    );

    return res.json({
      success: true,
      data: { address1, city, state, postalCode, country }
    });
  } catch (e) {
    if (DEBUG) console.warn('[profile.address.put] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to save address' });
  }
});

// ========== Per-user SMTP settings (for AI agent document emails) ==========
function isValidEmail(email) {
  const s = String(email || '').trim();
  if (!s || s.length > 254) return false;
  // Simple pragmatic check
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
}

async function loadUserSmtpSettings(userId) {
  if (!pool) throw new Error('Database not configured');
  const [rows] = await pool.execute(
    'SELECT user_id, host, port, secure, username, password_enc, password_iv, password_tag, from_email, from_name, reply_to FROM user_smtp_settings WHERE user_id = ? LIMIT 1',
    [userId]
  );
  return rows && rows[0] ? rows[0] : null;
}

async function sendEmailViaUserSmtp({ userId, toEmail, subject, text, html, attachments }) {
  const settings = await loadUserSmtpSettings(userId);
  if (!settings) throw new Error('SMTP settings not configured');

  const key = getUserSmtpEncryptionKey();
  if (!key) throw new Error('USER_SMTP_ENCRYPTION_KEY not configured');

  const host = String(settings.host || '').trim();
  const port = parseInt(String(settings.port || '0'), 10);
  const secure = String(settings.secure || '0') === '1' || settings.secure === 1;
  const smtpUser = settings.username != null ? String(settings.username) : '';
  const smtpPass = decryptAes256Gcm(settings.password_enc, settings.password_iv, settings.password_tag, key);

  if (!host || !port) throw new Error('SMTP host/port missing');
  if (!smtpUser || !smtpPass) throw new Error('SMTP username/password missing');

  const fromEmail = String(settings.from_email || '').trim();
  const fromName = String(settings.from_name || '').trim();
  const replyTo = String(settings.reply_to || '').trim();

  if (!isValidEmail(fromEmail)) throw new Error('SMTP from_email invalid');

  const transporter = nodemailer.createTransport({
    host,
    port,
    secure,
    auth: { user: smtpUser, pass: smtpPass }
  });

  const from = fromName ? `"${fromName.replace(/\"/g, '')}" <${fromEmail}>` : fromEmail;

  return transporter.sendMail({
    from,
    to: String(toEmail || '').trim(),
    ...(replyTo ? { replyTo } : {}),
    subject: String(subject || '').slice(0, 255) || 'Document from Phone.System',
    ...(text ? { text: String(text) } : {}),
    ...(html ? { html: String(html) } : {}),
    ...(Array.isArray(attachments) && attachments.length ? { attachments } : {})
  });
}

app.get('/api/me/smtp-settings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const row = await loadUserSmtpSettings(userId);
    if (!row) return res.json({ success: true, data: null });
    return res.json({
      success: true,
      data: {
        host: row.host,
        port: row.port,
        secure: row.secure ? 1 : 0,
        username: row.username || '',
        from_email: row.from_email || '',
        from_name: row.from_name || '',
        reply_to: row.reply_to || '',
        has_password: !!(row.password_enc && row.password_iv && row.password_tag)
      }
    });
  } catch (e) {
    if (DEBUG) console.warn('[smtp.settings.get] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load SMTP settings' });
  }
});

app.put('/api/me/smtp-settings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;
    const body = req.body || {};

    const host = String(body.host || '').trim();
    const port = parseInt(String(body.port || '0'), 10);
    const secure = body.secure === true || body.secure === 1 || String(body.secure || '').trim() === '1';
    const username = String(body.username || '').trim();
    const password = (body.password != null ? String(body.password) : '');

    const fromEmail = String(body.from_email || body.fromEmail || '').trim();
    const fromName = String(body.from_name || body.fromName || '').trim();
    const replyTo = String(body.reply_to || body.replyTo || '').trim();

    if (!host) return res.status(400).json({ success: false, message: 'SMTP host is required' });
    if (!port || port < 1 || port > 65535) return res.status(400).json({ success: false, message: 'SMTP port is invalid' });
    if (!username) return res.status(400).json({ success: false, message: 'SMTP username is required' });
    if (!isValidEmail(fromEmail)) return res.status(400).json({ success: false, message: 'from_email is invalid' });
    if (replyTo && !isValidEmail(replyTo)) return res.status(400).json({ success: false, message: 'reply_to is invalid' });

    const existing = await loadUserSmtpSettings(userId);

    let passwordEnc = existing?.password_enc || null;
    let passwordIv = existing?.password_iv || null;
    let passwordTag = existing?.password_tag || null;

    if (password && password.trim()) {
      const key = getUserSmtpEncryptionKey();
      if (!key) return res.status(500).json({ success: false, message: 'USER_SMTP_ENCRYPTION_KEY not configured' });
      const enc = encryptAes256Gcm(password, key);
      passwordEnc = enc.enc;
      passwordIv = enc.iv;
      passwordTag = enc.tag;
    }

    if (!passwordEnc || !passwordIv || !passwordTag) {
      return res.status(400).json({ success: false, message: 'SMTP password is required' });
    }

    await pool.execute(
      `INSERT INTO user_smtp_settings (user_id, host, port, secure, username, password_enc, password_iv, password_tag, from_email, from_name, reply_to)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)
       ON DUPLICATE KEY UPDATE host=VALUES(host), port=VALUES(port), secure=VALUES(secure), username=VALUES(username),
         password_enc=VALUES(password_enc), password_iv=VALUES(password_iv), password_tag=VALUES(password_tag),
         from_email=VALUES(from_email), from_name=VALUES(from_name), reply_to=VALUES(reply_to)`,
      [userId, host, port, secure ? 1 : 0, username, passwordEnc, passwordIv, passwordTag, fromEmail, fromName || null, replyTo || null]
    );

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.warn('[smtp.settings.put] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to save SMTP settings' });
  }
});

app.post('/api/me/smtp-settings/test', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    // Default to the logged-in user email
    let toEmail = String(req.body?.to_email || req.body?.toEmail || req.session.email || '').trim();
    if (!toEmail || !isValidEmail(toEmail)) {
      return res.status(400).json({ success: false, message: 'Valid to_email is required' });
    }

    await sendEmailViaUserSmtp({
      userId,
      toEmail,
      subject: 'Phone.System SMTP test',
      text: 'This is a test email from Phone.System. Your SMTP settings are working.'
    });

    return res.json({ success: true });
  } catch (e) {
    const msg = e?.message || 'SMTP test failed';
    if (DEBUG) console.warn('[smtp.settings.test] error:', msg);
    return res.status(502).json({ success: false, message: msg });
  }
});

// ========== Per-user physical mail settings (Click2Mail return address) ==========
async function loadUserMailSettings(userId) {
  if (!pool) throw new Error('Database not configured');
  const [rows] = await pool.execute(
    'SELECT user_id, name, organization, address1, address2, city, state, postal_code, country FROM user_mail_settings WHERE user_id = ? LIMIT 1',
    [userId]
  );
  return rows && rows[0] ? rows[0] : null;
}

app.get('/api/me/ai/mail-settings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const row = await loadUserMailSettings(userId);
    if (!row) return res.json({ success: true, data: null });

    return res.json({
      success: true,
      data: {
        name: row.name || '',
        organization: row.organization || '',
        address1: row.address1 || '',
        address2: row.address2 || '',
        city: row.city || '',
        state: row.state || '',
        postal_code: row.postal_code || '',
        country: row.country || 'US'
      }
    });
  } catch (e) {
    if (DEBUG) console.warn('[mail.settings.get] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load mail settings' });
  }
});

app.put('/api/me/ai/mail-settings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const body = req.body || {};

    const name = String(body.name || '').trim().slice(0, 255) || null;
    const organization = String(body.organization || '').trim().slice(0, 255) || null;
    const address1 = String(body.address1 || '').trim().slice(0, 255);
    const address2 = String(body.address2 || '').trim().slice(0, 255) || null;
    const city = String(body.city || '').trim().slice(0, 100);
    const country = normalizeClick2MailCountry(body.country || 'US');
    const stateRaw = String(body.state || '').trim();
    const state = (country === 'US') ? normalizeUsState2(stateRaw) : stateRaw.slice(0, 64);
    const postalCode = normalizePostalCode(body.postal_code || body.postalCode || body.zip || '');

    if (!address1) return res.status(400).json({ success: false, message: 'address1 is required' });
    if (!city) return res.status(400).json({ success: false, message: 'city is required' });
    if (!state) return res.status(400).json({ success: false, message: 'state is required' });
    if (!postalCode) return res.status(400).json({ success: false, message: 'postal_code is required' });

    await pool.execute(
      `INSERT INTO user_mail_settings (user_id, name, organization, address1, address2, city, state, postal_code, country)
       VALUES (?,?,?,?,?,?,?,?,?)
       ON DUPLICATE KEY UPDATE
         name=VALUES(name), organization=VALUES(organization), address1=VALUES(address1), address2=VALUES(address2),
         city=VALUES(city), state=VALUES(state), postal_code=VALUES(postal_code), country=VALUES(country), updated_at=CURRENT_TIMESTAMP`,
      [userId, name, organization, address1, address2, city, state, postalCode, country]
    );

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.warn('[mail.settings.put] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to save mail settings' });
  }
});

// ========== Per-user AI payment settings (Square/Stripe for invoicing callers) ==========
async function loadUserAiPaymentSettings(userId) {
  if (!pool) throw new Error('Database not configured');
  const [rows] = await pool.execute(
    `SELECT user_id, square_access_token_enc, square_access_token_iv, square_access_token_tag,
            square_location_id, square_environment, stripe_secret_key_enc, stripe_secret_key_iv,
            stripe_secret_key_tag, preferred_provider
     FROM user_ai_payment_settings WHERE user_id = ? LIMIT 1`,
    [userId]
  );
  return rows && rows[0] ? rows[0] : null;
}

app.get('/api/me/ai/payment-settings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const row = await loadUserAiPaymentSettings(userId);
    if (!row) return res.json({ success: true, data: { user_id: userId } });

    return res.json({
      success: true,
      data: {
        user_id: userId,
        square_configured: !!(row.square_access_token_enc && row.square_access_token_iv && row.square_access_token_tag),
        square_location_id: row.square_location_id || '',
        square_environment: row.square_environment || 'production',
        stripe_configured: !!(row.stripe_secret_key_enc && row.stripe_secret_key_iv && row.stripe_secret_key_tag),
        preferred_provider: row.preferred_provider || null
      }
    });
  } catch (e) {
    if (DEBUG) console.warn('[ai.payment.settings.get] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load payment settings' });
  }
});

app.put('/api/me/ai/payment-settings/square', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const body = req.body || {};

    const accessToken = String(body.access_token || body.accessToken || '').trim();
    const locationId = String(body.location_id || body.locationId || '').trim().slice(0, 64) || null;
    const environment = String(body.environment || 'production').trim().toLowerCase();

    if (environment !== 'sandbox' && environment !== 'production') {
      return res.status(400).json({ success: false, message: 'environment must be sandbox or production' });
    }

    const existing = await loadUserAiPaymentSettings(userId);

    let tokenEnc = existing?.square_access_token_enc || null;
    let tokenIv = existing?.square_access_token_iv || null;
    let tokenTag = existing?.square_access_token_tag || null;

    if (accessToken) {
      const key = getUserSmtpEncryptionKey();
      if (!key) return res.status(500).json({ success: false, message: 'USER_SMTP_ENCRYPTION_KEY not configured' });
      const enc = encryptAes256Gcm(accessToken, key);
      tokenEnc = enc.enc;
      tokenIv = enc.iv;
      tokenTag = enc.tag;
    }

    await pool.execute(
      `INSERT INTO user_ai_payment_settings (user_id, square_access_token_enc, square_access_token_iv, square_access_token_tag, square_location_id, square_environment)
       VALUES (?,?,?,?,?,?)
       ON DUPLICATE KEY UPDATE
         square_access_token_enc = VALUES(square_access_token_enc),
         square_access_token_iv = VALUES(square_access_token_iv),
         square_access_token_tag = VALUES(square_access_token_tag),
         square_location_id = VALUES(square_location_id),
         square_environment = VALUES(square_environment),
         updated_at = CURRENT_TIMESTAMP`,
      [userId, tokenEnc, tokenIv, tokenTag, locationId, environment]
    );

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.warn('[ai.payment.settings.square.put] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to save Square settings' });
  }
});

app.put('/api/me/ai/payment-settings/stripe', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const body = req.body || {};

    const secretKey = String(body.secret_key || body.secretKey || '').trim();

    const existing = await loadUserAiPaymentSettings(userId);

    let keyEnc = existing?.stripe_secret_key_enc || null;
    let keyIv = existing?.stripe_secret_key_iv || null;
    let keyTag = existing?.stripe_secret_key_tag || null;

    if (secretKey) {
      const key = getUserSmtpEncryptionKey();
      if (!key) return res.status(500).json({ success: false, message: 'USER_SMTP_ENCRYPTION_KEY not configured' });
      const enc = encryptAes256Gcm(secretKey, key);
      keyEnc = enc.enc;
      keyIv = enc.iv;
      keyTag = enc.tag;
    }

    await pool.execute(
      `INSERT INTO user_ai_payment_settings (user_id, stripe_secret_key_enc, stripe_secret_key_iv, stripe_secret_key_tag)
       VALUES (?,?,?,?)
       ON DUPLICATE KEY UPDATE
         stripe_secret_key_enc = VALUES(stripe_secret_key_enc),
         stripe_secret_key_iv = VALUES(stripe_secret_key_iv),
         stripe_secret_key_tag = VALUES(stripe_secret_key_tag),
         updated_at = CURRENT_TIMESTAMP`,
      [userId, keyEnc, keyIv, keyTag]
    );

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.warn('[ai.payment.settings.stripe.put] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to save Stripe settings' });
  }
});

app.put('/api/me/ai/payment-settings/preferred', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const body = req.body || {};

    const provider = String(body.provider || '').trim().toLowerCase() || null;
    if (provider && provider !== 'square' && provider !== 'stripe') {
      return res.status(400).json({ success: false, message: 'provider must be square or stripe' });
    }

    await pool.execute(
      `INSERT INTO user_ai_payment_settings (user_id, preferred_provider)
       VALUES (?,?)
       ON DUPLICATE KEY UPDATE preferred_provider = VALUES(preferred_provider), updated_at = CURRENT_TIMESTAMP`,
      [userId, provider]
    );

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.warn('[ai.payment.settings.preferred.put] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to save preferred provider' });
  }
});

// AI Payment Link creation (called by AI agent at runtime)
async function createUserSquarePaymentLink({ userId, amount, description, customerEmail }) {
  const settings = await loadUserAiPaymentSettings(userId);
  if (!settings || !settings.square_access_token_enc) {
    throw new Error('Square is not configured');
  }

  const key = getUserSmtpEncryptionKey();
  if (!key) throw new Error('Encryption key not configured');

  const accessToken = decryptAes256Gcm(
    settings.square_access_token_enc,
    settings.square_access_token_iv,
    settings.square_access_token_tag,
    key
  );

  if (!accessToken) throw new Error('Square access token missing');

  const env = settings.square_environment || 'production';
  const baseUrl = env === 'sandbox' ? 'https://connect.squareupsandbox.com' : 'https://connect.squareup.com';

  // Auto-discover location if not set
  let locationId = settings.square_location_id;
  if (!locationId) {
    const locResp = await axios.get(`${baseUrl}/v2/locations`, {
      headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
      timeout: 15000,
      validateStatus: () => true
    });
    if (locResp.status >= 200 && locResp.status < 300 && locResp.data?.locations?.length) {
      locationId = locResp.data.locations[0].id;
    }
    if (!locationId) throw new Error('No Square location found');
  }

  const amountCents = Math.round(Number(amount) * 100);
  if (!amountCents || amountCents < 100) throw new Error('Amount must be at least $1.00');

  const idempotencyKey = crypto.randomUUID();
  const payload = {
    idempotency_key: idempotencyKey,
    quick_pay: {
      name: description || 'Payment',
      price_money: { amount: amountCents, currency: 'USD' },
      location_id: locationId
    },
    checkout_options: {
      allow_tipping: false,
      ask_for_shipping_address: false
    },
    ...(customerEmail ? { pre_populated_data: { buyer_email: customerEmail } } : {})
  };

  const resp = await axios.post(`${baseUrl}/v2/online-checkout/payment-links`, payload, {
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json', 'Square-Version': '2024-01-18' },
    timeout: 20000,
    validateStatus: () => true
  });

  if (resp.status < 200 || resp.status >= 300) {
    const msg = resp.data?.errors?.[0]?.detail || resp.data?.message || 'Square API error';
    throw new Error(msg);
  }

  const paymentLink = resp.data?.payment_link;
  if (!paymentLink?.url) throw new Error('Square did not return a payment link');

  return {
    url: paymentLink.url,
    id: paymentLink.id,
    order_id: paymentLink.order_id
  };
}

async function createUserStripePaymentLink({ userId, amount, description, customerEmail }) {
  const settings = await loadUserAiPaymentSettings(userId);
  if (!settings || !settings.stripe_secret_key_enc) {
    throw new Error('Stripe is not configured');
  }

  const key = getUserSmtpEncryptionKey();
  if (!key) throw new Error('Encryption key not configured');

  const secretKey = decryptAes256Gcm(
    settings.stripe_secret_key_enc,
    settings.stripe_secret_key_iv,
    settings.stripe_secret_key_tag,
    key
  );

  if (!secretKey) throw new Error('Stripe secret key missing');

  const amountCents = Math.round(Number(amount) * 100);
  if (!amountCents || amountCents < 50) throw new Error('Amount must be at least $0.50');

  const stripe = Stripe(secretKey);

  // Create a price on the fly
  const price = await stripe.prices.create({
    unit_amount: amountCents,
    currency: 'usd',
    product_data: {
      name: description || 'Payment'
    }
  });

  // Create payment link
  const paymentLink = await stripe.paymentLinks.create({
    line_items: [{ price: price.id, quantity: 1 }],
    ...(customerEmail ? { metadata: { customer_email: customerEmail } } : {})
  });

  return {
    url: paymentLink.url,
    id: paymentLink.id
  };
}

// API endpoint for AI agent to create payment links
app.post('/api/ai/agent/:token/payment-link', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const token = String(req.params.token || '').trim();
    if (!token) return res.status(400).json({ success: false, message: 'Missing token' });

    // Verify agent token and get user
    const tokenHash = sha256(token);
    const [agentRows] = await pool.execute(
      'SELECT a.*, u.id as owner_user_id FROM ai_agents a JOIN signup_users u ON a.user_id = u.id WHERE a.agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = agentRows && agentRows[0] ? agentRows[0] : null;
    if (!agent) {
      return res.status(401).json({ success: false, message: 'Invalid agent token' });
    }

    const userId = agent.owner_user_id || agent.user_id;
    const body = req.body || {};

    const amount = parseFloat(body.amount);
    if (!Number.isFinite(amount) || amount < 0.50) {
      return res.status(400).json({ success: false, message: 'amount must be at least 0.50' });
    }
    if (amount > 10000) {
      return res.status(400).json({ success: false, message: 'amount cannot exceed 10000' });
    }

    const description = String(body.description || 'Payment').trim().slice(0, 500);
    const customerEmail = body.customer_email ? String(body.customer_email).trim() : null;
    const customerPhone = body.customer_phone ? String(body.customer_phone).trim() : null;
    const callId = body.call_id ? String(body.call_id).trim() : null;
    const callDomain = body.call_domain ? String(body.call_domain).trim() : null;
    const providerOverride = body.provider ? String(body.provider).trim().toLowerCase() : null;

    // Determine which provider to use
    const settings = await loadUserAiPaymentSettings(userId);
    let provider = providerOverride || settings?.preferred_provider || null;

    // Auto-select if not specified
    if (!provider) {
      if (settings?.square_access_token_enc) provider = 'square';
      else if (settings?.stripe_secret_key_enc) provider = 'stripe';
    }

    if (!provider) {
      return res.status(400).json({ success: false, message: 'No payment provider configured. Configure Square or Stripe in Tools.' });
    }

    let result;
    if (provider === 'square') {
      result = await createUserSquarePaymentLink({ userId, amount, description, customerEmail });
    } else if (provider === 'stripe') {
      result = await createUserStripePaymentLink({ userId, amount, description, customerEmail });
    } else {
      return res.status(400).json({ success: false, message: 'Invalid provider' });
    }

    // Record the payment request in the database
    const amountCents = Math.round(amount * 100);
    const [insertResult] = await pool.execute(
      `INSERT INTO user_payment_requests 
       (user_id, provider, provider_payment_id, provider_checkout_id, amount_cents, currency, description, customer_email, customer_phone, payment_url, status, call_id, call_domain)
       VALUES (?, ?, ?, ?, ?, 'USD', ?, ?, ?, ?, 'pending', ?, ?)`,
      [
        userId,
        provider,
        result.id || null,
        result.order_id || result.checkout_id || null,
        amountCents,
        description,
        customerEmail,
        customerPhone,
        result.url,
        callId,
        callDomain
      ]
    );
    const requestId = insertResult?.insertId || null;

    return res.json({
      success: true,
      data: {
        request_id: requestId,
        payment_url: result.url,
        payment_link_id: result.id,
        provider,
        amount,
        description,
        status: 'pending'
      }
    });
  } catch (e) {
    const msg = e?.message || 'Failed to create payment link';
    if (DEBUG) console.warn('[ai.payment-link] error:', msg);
    return res.status(500).json({ success: false, message: msg });
  }
});

// API endpoint for AI agent to check payment status
app.get('/api/ai/agent/:token/payment-status/:requestId', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const token = String(req.params.token || '').trim();
    const requestId = parseInt(req.params.requestId, 10);
    if (!token) return res.status(400).json({ success: false, message: 'Missing token' });
    if (!requestId) return res.status(400).json({ success: false, message: 'Missing request ID' });

    // Verify agent token and get user
    const tokenHash = sha256(token);
    const [agentRows] = await pool.execute(
      'SELECT a.*, u.id as owner_user_id FROM ai_agents a JOIN signup_users u ON a.user_id = u.id WHERE a.agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = agentRows && agentRows[0] ? agentRows[0] : null;
    if (!agent) {
      return res.status(401).json({ success: false, message: 'Invalid agent token' });
    }

    const userId = agent.owner_user_id || agent.user_id;

    // Get the payment request
    const [rows] = await pool.execute(
      'SELECT id, provider, amount_cents, currency, description, status, paid_at, created_at FROM user_payment_requests WHERE id = ? AND user_id = ? LIMIT 1',
      [requestId, userId]
    );
    const request = rows && rows[0] ? rows[0] : null;
    if (!request) {
      return res.status(404).json({ success: false, message: 'Payment request not found' });
    }

    return res.json({
      success: true,
      data: {
        request_id: request.id,
        provider: request.provider,
        amount: request.amount_cents / 100,
        currency: request.currency,
        description: request.description,
        status: request.status,
        paid_at: request.paid_at,
        created_at: request.created_at
      }
    });
  } catch (e) {
    const msg = e?.message || 'Failed to check payment status';
    if (DEBUG) console.warn('[ai.payment-status] error:', msg);
    return res.status(500).json({ success: false, message: msg });
  }
});

// ========== User Payment Webhooks (Square/Stripe for AI agent invoicing) ==========

// Square webhook for user payment links
// Configure in Square Dashboard: Webhooks -> Add Endpoint
// Events: payment.completed, payment.failed, order.updated
app.post('/webhooks/user-square/:userId', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = parseInt(req.params.userId, 10);
    if (!userId) return res.status(400).json({ success: false, message: 'Invalid user ID' });

    const body = req.body || {};
    const eventType = String(body.type || '').toLowerCase();
    const data = body.data?.object || body.data || {};

    if (DEBUG) console.log('[user-square.webhook] Received:', { userId, eventType, dataType: typeof data });

    // Handle payment.completed or payment.updated events
    if (eventType.includes('payment')) {
      const payment = data.payment || data;
      const orderId = String(payment.order_id || '').trim();
      const paymentId = String(payment.id || '').trim();
      const status = String(payment.status || '').toLowerCase();

      if (!orderId && !paymentId) {
        return res.status(200).json({ success: true, ignored: true, reason: 'no order_id or payment_id' });
      }

      let newStatus = null;
      if (status === 'completed' || status === 'approved') newStatus = 'completed';
      else if (status === 'failed' || status === 'canceled') newStatus = 'failed';

      if (newStatus) {
        // Try to find by order_id (checkout_id) or payment_id
        const [rows] = await pool.execute(
          `SELECT id, status FROM user_payment_requests 
           WHERE user_id = ? AND provider = 'square' 
           AND (provider_checkout_id = ? OR provider_payment_id = ?) 
           AND status = 'pending' LIMIT 1`,
          [userId, orderId || null, paymentId || null]
        );
        const request = rows && rows[0] ? rows[0] : null;

        if (request) {
          await pool.execute(
            'UPDATE user_payment_requests SET status = ?, provider_payment_id = ?, paid_at = ? WHERE id = ?',
            [newStatus, paymentId || null, newStatus === 'completed' ? new Date() : null, request.id]
          );
          if (DEBUG) console.log('[user-square.webhook] Updated payment request:', { requestId: request.id, newStatus });
        }
      }
    }

    // Handle order.updated for checkout link payments
    if (eventType.includes('order')) {
      const order = data.order || data;
      const orderId = String(order.id || '').trim();
      const state = String(order.state || '').toLowerCase();

      if (orderId && (state === 'completed' || state === 'canceled')) {
        const newStatus = state === 'completed' ? 'completed' : 'failed';
        const [rows] = await pool.execute(
          `SELECT id, status FROM user_payment_requests 
           WHERE user_id = ? AND provider = 'square' AND provider_checkout_id = ? AND status = 'pending' LIMIT 1`,
          [userId, orderId]
        );
        const request = rows && rows[0] ? rows[0] : null;

        if (request) {
          await pool.execute(
            'UPDATE user_payment_requests SET status = ?, paid_at = ? WHERE id = ?',
            [newStatus, newStatus === 'completed' ? new Date() : null, request.id]
          );
          if (DEBUG) console.log('[user-square.webhook] Updated payment request via order:', { requestId: request.id, newStatus });
        }
      }
    }

    return res.status(200).json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[user-square.webhook] Error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Webhook handling failed' });
  }
});

// Stripe webhook for user payment links
// Configure in Stripe Dashboard: Developers -> Webhooks -> Add Endpoint
// Events: checkout.session.completed, checkout.session.expired, payment_intent.succeeded, payment_intent.payment_failed
app.post('/webhooks/user-stripe/:userId', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = parseInt(req.params.userId, 10);
    if (!userId) return res.status(400).json({ success: false, message: 'Invalid user ID' });

    // For user webhooks, we don't verify signature since each user has their own Stripe account
    // The webhook URL contains the user ID which provides some security
    const body = req.body || {};
    const eventType = String(body.type || '').toLowerCase();
    const data = body.data?.object || {};

    if (DEBUG) console.log('[user-stripe.webhook] Received:', { userId, eventType });

    // Handle checkout.session events
    if (eventType.startsWith('checkout.session.')) {
      const sessionId = String(data.id || '').trim();
      const paymentLinkId = String(data.payment_link || '').trim();
      const paymentStatus = String(data.payment_status || '').toLowerCase();
      const paymentIntentId = data.payment_intent ? String(data.payment_intent) : null;

      if (!paymentLinkId && !sessionId) {
        return res.status(200).json({ success: true, ignored: true, reason: 'no payment_link or session' });
      }

      let newStatus = null;
      if (eventType === 'checkout.session.completed' && paymentStatus === 'paid') {
        newStatus = 'completed';
      } else if (eventType === 'checkout.session.expired') {
        newStatus = 'expired';
      }

      if (newStatus && paymentLinkId) {
        const [rows] = await pool.execute(
          `SELECT id, status FROM user_payment_requests 
           WHERE user_id = ? AND provider = 'stripe' AND provider_payment_id = ? AND status = 'pending' LIMIT 1`,
          [userId, paymentLinkId]
        );
        const request = rows && rows[0] ? rows[0] : null;

        if (request) {
          await pool.execute(
            'UPDATE user_payment_requests SET status = ?, paid_at = ? WHERE id = ?',
            [newStatus, newStatus === 'completed' ? new Date() : null, request.id]
          );
          if (DEBUG) console.log('[user-stripe.webhook] Updated payment request:', { requestId: request.id, newStatus });
        }
      }
    }

    // Handle payment_intent events as fallback
    if (eventType.startsWith('payment_intent.')) {
      const intentId = String(data.id || '').trim();
      const status = String(data.status || '').toLowerCase();
      const metadata = data.metadata || {};

      let newStatus = null;
      if (eventType === 'payment_intent.succeeded' || status === 'succeeded') {
        newStatus = 'completed';
      } else if (eventType === 'payment_intent.payment_failed' || status === 'canceled') {
        newStatus = 'failed';
      }

      // Payment intents don't directly link to our payment_link_id, so this is best-effort
      if (newStatus && metadata.payment_request_id) {
        const requestId = parseInt(metadata.payment_request_id, 10);
        if (requestId) {
          const [rows] = await pool.execute(
            'SELECT id, status FROM user_payment_requests WHERE id = ? AND user_id = ? AND status = "pending" LIMIT 1',
            [requestId, userId]
          );
          const request = rows && rows[0] ? rows[0] : null;

          if (request) {
            await pool.execute(
              'UPDATE user_payment_requests SET status = ?, paid_at = ? WHERE id = ?',
              [newStatus, newStatus === 'completed' ? new Date() : null, request.id]
            );
            if (DEBUG) console.log('[user-stripe.webhook] Updated payment request via intent:', { requestId: request.id, newStatus });
          }
        }
      }
    }

    return res.status(200).json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[user-stripe.webhook] Error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Webhook handling failed' });
  }
});

// ========== Payment Requests History ==========
app.get('/api/me/ai/payment-requests', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const statusFilter = String(req.query.status || '').trim().toLowerCase();
    const providerFilter = String(req.query.provider || '').trim().toLowerCase();
    const page = Math.max(0, parseInt(req.query.page) || 0);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 25));
    const offset = page * limit;

    let whereClauses = ['user_id = ?'];
    let params = [userId];

    if (statusFilter && ['pending', 'completed', 'failed', 'expired', 'cancelled'].includes(statusFilter)) {
      whereClauses.push('status = ?');
      params.push(statusFilter);
    }
    if (providerFilter && ['square', 'stripe'].includes(providerFilter)) {
      whereClauses.push('provider = ?');
      params.push(providerFilter);
    }

    const whereClause = whereClauses.join(' AND ');

    const [countRows] = await pool.execute(
      `SELECT COUNT(*) as total FROM user_payment_requests WHERE ${whereClause}`,
      params
    );
    const total = countRows && countRows[0] ? Number(countRows[0].total) : 0;

    // Use query() instead of execute() to avoid MySQL prepared statement issues with LIMIT/OFFSET
    const [rows] = await pool.query(
      `SELECT id, provider, amount_cents, currency, description, customer_email, customer_phone,
              payment_url, status, call_id, call_domain, paid_at, created_at
       FROM user_payment_requests
       WHERE ${whereClause}
       ORDER BY created_at DESC
       LIMIT ${parseInt(limit, 10)} OFFSET ${parseInt(offset, 10)}`,
      params
    );

    // Get payment stats for charts
    const [statsRows] = await pool.execute(
      `SELECT status, COUNT(*) as count, SUM(amount_cents) as total_cents
       FROM user_payment_requests WHERE user_id = ? GROUP BY status`,
      [userId]
    );
    const stats = {};
    for (const r of (statsRows || [])) {
      stats[r.status] = { count: Number(r.count) || 0, total_cents: Number(r.total_cents) || 0 };
    }

    return res.json({
      success: true,
      data: rows || [],
      total,
      page,
      limit,
      stats
    });
  } catch (e) {
    if (DEBUG) console.warn('[ai.payment-requests.list] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load payment requests' });
  }
});

// ========== AI document templates (DOCX) ==========
const docTemplateUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 } // 10MB
});

app.get('/api/me/ai/doc-templates', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const [rows] = await pool.execute(
      'SELECT id, name, original_filename, mime_type, size_bytes, created_at, updated_at FROM ai_doc_templates WHERE user_id = ? ORDER BY created_at DESC',
      [userId]
    );
    return res.json({ success: true, data: rows || [] });
  } catch (e) {
    if (DEBUG) console.warn('[ai.doc.templates.list] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load templates' });
  }
});

app.post('/api/me/ai/doc-templates', requireAuth, docTemplateUpload.single('file'), async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const file = req.file;
    if (!file || !file.buffer) {
      return res.status(400).json({ success: false, message: 'Missing file upload (field name: file)' });
    }

    const original = String(file.originalname || '').trim();
    if (!/\.docx$/i.test(original)) {
      return res.status(400).json({ success: false, message: 'Only .docx templates are supported' });
    }

    const name = String((req.body && (req.body.name || req.body.display_name)) || original.replace(/\.docx$/i, '')).trim();
    if (!name) return res.status(400).json({ success: false, message: 'Template name is required' });

    const mime = String(file.mimetype || 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    const sizeBytes = file.size != null ? Number(file.size) : (file.buffer ? file.buffer.length : null);

    const [ins] = await pool.execute(
      'INSERT INTO ai_doc_templates (user_id, name, original_filename, mime_type, size_bytes, doc_blob) VALUES (?,?,?,?,?,?)',
      [userId, name, original || null, mime || null, sizeBytes || null, file.buffer]
    );

    return res.json({ success: true, data: { id: ins.insertId, name } });
  } catch (e) {
    const msg = e?.code === 'ER_DUP_ENTRY' ? 'A template with that name already exists' : (e?.message || 'Upload failed');
    if (DEBUG) console.warn('[ai.doc.templates.upload] error:', msg);
    return res.status(400).json({ success: false, message: msg });
  }
});

app.delete('/api/me/ai/doc-templates/:id', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const id = String(req.params.id || '').trim();
    if (!id) return res.status(400).json({ success: false, message: 'Missing template id' });

    const [rows] = await pool.execute('SELECT id FROM ai_doc_templates WHERE id = ? AND user_id = ? LIMIT 1', [id, userId]);
    if (!rows || !rows.length) return res.status(404).json({ success: false, message: 'Template not found' });

    await pool.execute('DELETE FROM ai_doc_templates WHERE id = ? AND user_id = ?', [id, userId]);

    // If any agent uses this as default, clear it
    try { await pool.execute('UPDATE ai_agents SET default_doc_template_id = NULL WHERE user_id = ? AND default_doc_template_id = ?', [userId, id]); } catch {}

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.warn('[ai.doc.templates.delete] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Delete failed' });
  }
});

function normalizeTemplateVariables(obj) {
  if (!obj || typeof obj !== 'object') return {};
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    const key = String(k || '').trim();
    if (!key) continue;
    // Keep values as strings to avoid docxtemplater surprises
    out[key] = (v == null) ? '' : String(v);
  }
  return out;
}

function renderDocxTemplate({ templateBuffer, variables }) {
  const data = normalizeTemplateVariables(variables);
  const zip = new PizZip(templateBuffer);
  const doc = new Docxtemplater(zip, {
    paragraphLoop: true,
    linebreaks: true,
    delimiters: { start: '[[', end: ']]' },
    // If a template contains a placeholder that isn't present in variables,
    // substitute an empty string instead of throwing.
    nullGetter: () => ''
  });
  doc.render(data);
  return doc.getZip().generate({ type: 'nodebuffer', compression: 'DEFLATE' });
}

async function convertDocxToPdfBuffer(docxBuffer, { baseName = 'document' } = {}) {
  const candidates = [];
  const configured = String(process.env.LIBREOFFICE_BIN || process.env.SOFFICE_PATH || '').trim();
  if (configured) candidates.push(configured);

  // Common install locations (helps when LibreOffice isn't on PATH)
  try {
    if (process.platform === 'win32') {
      const winPaths = [
        'C:\\Program Files\\LibreOffice\\program\\soffice.exe',
        'C:\\Program Files (x86)\\LibreOffice\\program\\soffice.exe'
      ];
      for (const p of winPaths) {
        try { if (fs.existsSync(p)) candidates.push(p); } catch {}
      }
    } else if (process.platform === 'darwin') {
      const macPath = '/Applications/LibreOffice.app/Contents/MacOS/soffice';
      try { if (fs.existsSync(macPath)) candidates.push(macPath); } catch {}
    }
  } catch {}

  candidates.push('soffice');
  candidates.push('libreoffice');

  const tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'Phone.System-doc-'));
  const safeBase = String(baseName || 'document').replace(/[^a-z0-9_-]+/gi, '_').slice(0, 48) || 'document';
  const inputPath = path.join(tmpDir, `${safeBase}.docx`);
  const outputDir = tmpDir;
  const expectedPdf = path.join(outputDir, `${safeBase}.pdf`);

  try {
    await fs.promises.writeFile(inputPath, docxBuffer);

    const args = [
      '--headless',
      '--nologo',
      '--nolockcheck',
      '--norestore',
      '--convert-to',
      'pdf',
      '--outdir',
      outputDir,
      inputPath
    ];

    for (const bin of candidates) {
      try {
        await execFileAsync(bin, args, { timeout: 30000, windowsHide: true });
        if (fs.existsSync(expectedPdf)) {
          return await fs.promises.readFile(expectedPdf);
        }
      } catch (e) {
        // try next candidate
        if (DEBUG) console.warn('[doc.convert] LibreOffice convert attempt failed:', { bin, message: e?.message || e });
      }
    }

    return null;
  } finally {
    try { await fs.promises.rm(tmpDir, { recursive: true, force: true }); } catch {}
  }
}

async function generateSimpleLetterPdfBuffer({ subject, body, includeBlankAddressPage }) {
  const subj = String(subject || '').trim();
  const text = String(body || '').trim();

  if (!text) throw new Error('Letter body is required');
  if (!PDFDocument) throw new Error('PDF generator not installed (missing pdfkit dependency)');

  const doc = new PDFDocument({
    size: 'LETTER',
    margin: 72 // 1 inch
  });

  const chunks = [];
  doc.on('data', (d) => chunks.push(d));

  const done = new Promise((resolve, reject) => {
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
  });

  // Click2Mail "Address on Separate Page" layout works best with a blank first page.
  if (includeBlankAddressPage) {
    // Ensure the first page exists but remains blank.
    doc.font('Helvetica').fontSize(1).fillColor('white').text(' ', 0, 0);
    doc.fillColor('black');
    doc.addPage();
  }

  // Header (optional)
  if (subj) {
    doc.font('Helvetica-Bold').fontSize(14).text(subj, { align: 'left' });
    doc.moveDown(0.75);
  }

  // Body
  doc.font('Helvetica').fontSize(11).text(text, {
    align: 'left',
    lineGap: 4
  });

  doc.end();
  return done;
}

function parseBearerToken(authHeader) {
  const s = String(authHeader || '').trim();
  if (!s) return '';
  const m = s.match(/^Bearer\s+(.+)$/i);
  if (!m) return '';
  return String(m[1] || '').trim();
}

// Agent-auth endpoint called by the Pipecat agent runtime.
// Sends a templated document via the owning user's SMTP settings.
app.post('/api/ai/agent/send-document', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const token = parseBearerToken(req.headers.authorization);
    if (!token) return res.status(401).json({ success: false, message: 'Missing Authorization bearer token' });

    const tokenHash = sha256(token);
    const [arows] = await pool.execute(
      'SELECT id, user_id, default_doc_template_id FROM ai_agents WHERE agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(401).json({ success: false, message: 'Invalid token' });

    const userId = Number(agent.user_id);
    const agentId = Number(agent.id);

    const toEmail = String(req.body?.to_email || req.body?.toEmail || '').trim();
    if (!toEmail || !isValidEmail(toEmail)) {
      return res.status(400).json({ success: false, message: 'Valid to_email is required' });
    }

    const variables = (req.body && (req.body.variables || req.body.vars)) || {};

    const callId = String(req.body?.call_id || req.body?.callId || '').trim() || null;
    const callDomain = String(req.body?.call_domain || req.body?.callDomain || '').trim() || null;

    // Resolve template id (explicit wins; otherwise agent default)
    const templateRaw = (req.body?.template_id !== undefined) ? req.body.template_id : req.body?.templateId;
    let templateId = null;
    if (templateRaw != null && String(templateRaw).trim()) {
      const tid = parseInt(String(templateRaw).trim(), 10);
      if (!Number.isFinite(tid) || tid <= 0) {
        return res.status(400).json({ success: false, message: 'template_id is invalid' });
      }
      templateId = tid;
    } else if (agent.default_doc_template_id) {
      templateId = Number(agent.default_doc_template_id);
    }

    if (!templateId) {
      return res.status(400).json({ success: false, message: 'No template selected (set a default template for this agent or pass template_id)' });
    }

    const subjectRaw = String(req.body?.subject || '').trim();
    const subject = (subjectRaw ? subjectRaw.slice(0, 255) : 'Your document');

    let dedupeKey = String(req.body?.dedupe_key || req.body?.dedupeKey || '').trim();
    if (dedupeKey) {
      if (!/^[a-f0-9]{64}$/i.test(dedupeKey)) {
        return res.status(400).json({ success: false, message: 'dedupe_key must be a 64-char hex sha256' });
      }
      dedupeKey = dedupeKey.toLowerCase();
    } else {
      dedupeKey = sha256(
        `send-document|agent:${agentId}|call:${callDomain || ''}|${callId || ''}|template:${templateId}|to:${toEmail.toLowerCase()}`
      );
    }

    // Idempotency + audit row
    let sendRowId = null;
    try {
      const [ins] = await pool.execute(
        `INSERT INTO ai_email_sends (user_id, agent_id, template_id, dedupe_key, call_id, call_domain, to_email, subject, status, attempt_count)
         VALUES (?,?,?,?,?,?,?,?, 'pending', 1)`,
        [userId, agentId, templateId, dedupeKey, callId, callDomain, toEmail, subject]
      );
      sendRowId = ins.insertId;
    } catch (e) {
      if (e?.code !== 'ER_DUP_ENTRY') throw e;
      const [erows] = await pool.execute(
        'SELECT id, status, attempt_count FROM ai_email_sends WHERE dedupe_key = ? LIMIT 1',
        [dedupeKey]
      );
      const existing = erows && erows[0];
      if (existing && existing.status === 'completed') {
        return res.json({ success: true, already_sent: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'pending') {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'failed') {
        sendRowId = existing.id;
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'pending\', attempt_count = attempt_count + 1, error = NULL, to_email = ?, subject = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [toEmail, subject, sendRowId]
          );
        } catch {}
      } else {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
    }

    // Load template
    const [trows] = await pool.execute(
      'SELECT id, name, doc_blob FROM ai_doc_templates WHERE id = ? AND user_id = ? LIMIT 1',
      [templateId, userId]
    );
    const tpl = trows && trows[0];
    if (!tpl) {
      if (sendRowId) {
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            ['Template not found', sendRowId]
          );
        } catch {}
      }
      return res.status(404).json({ success: false, message: 'Template not found' });
    }

    // Render + convert
    let docxOut;
    try {
      docxOut = renderDocxTemplate({ templateBuffer: tpl.doc_blob, variables });
    } catch (e) {
      const msg = e?.message || 'Template render failed';
      if (sendRowId) {
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [msg, sendRowId]
          );
        } catch {}
      }
      return res.status(400).json({ success: false, message: msg });
    }

    let pdfOut = null;
    try {
      pdfOut = await convertDocxToPdfBuffer(docxOut, { baseName: tpl.name || 'document' });
    } catch (e) {
      pdfOut = null;
    }

    const safeBase = String(tpl.name || 'document').replace(/[^a-z0-9_-]+/gi, '_').slice(0, 48) || 'document';
    const attachments = pdfOut
      ? [{ filename: `${safeBase}.pdf`, content: pdfOut, contentType: 'application/pdf' }]
      : [{ filename: `${safeBase}.docx`, content: docxOut, contentType: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' }];

    const emailCost = AI_EMAIL_SEND_COST;
    if (emailCost > 0) {
      const chargeDesc = `AI email send (document) - ${toEmail}`.substring(0, 255);
      const charge = await chargeAiEmailSendsRow({
        sendRowId,
        userId,
        amount: emailCost,
        description: chargeDesc,
        label: 'ai.email'
      });

      if (!charge || charge.ok !== true) {
        const reason = charge?.reason || 'charge_failed';
        const short = (reason === 'insufficient_funds') ? 'Insufficient balance' : reason;
        const errMsg = `Email charge failed: ${short}`;

        if (sendRowId) {
          try {
            await pool.execute(
              'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
              [errMsg, sendRowId]
            );
          } catch {}
        }

        return res.status(reason === 'insufficient_funds' ? 402 : 500).json({ success: false, message: errMsg });
      }
    }

    // Persist preview content (best-effort)
    if (sendRowId) {
      try {
        const attachmentsMeta = (attachments || []).map(a => ({
          filename: a && a.filename ? String(a.filename) : 'attachment',
          content_type: a && a.contentType ? String(a.contentType) : null,
          size_bytes: (a && a.content && Buffer.isBuffer(a.content)) ? a.content.length : null
        }));
        await pool.execute(
          'UPDATE ai_email_sends SET email_text = ?, email_html = NULL, attachments_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          ['Please find your document attached.', JSON.stringify(attachmentsMeta), sendRowId]
        );
      } catch {}
    }

    try {
      const info = await sendEmailViaUserSmtp({
        userId,
        toEmail,
        subject,
        text: 'Please find your document attached.',
        attachments
      });

      const messageId = info && info.messageId ? String(info.messageId) : null;

      if (sendRowId) {
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'completed\', smtp_message_id = COALESCE(?, smtp_message_id), error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [messageId, sendRowId]
          );
        } catch {}
      }

      return res.json({
        success: true,
        dedupe_key: dedupeKey,
        already_sent: false,
        sent_format: pdfOut ? 'pdf' : 'docx'
      });
    } catch (e) {
      const msg = e?.message || 'Email send failed';

      // Best-effort refund if we charged before sending.
      if (emailCost > 0) {
        try {
          await refundAiEmailSendsRow({
            sendRowId,
            userId,
            amount: emailCost,
            description: `Refund: AI email send failed (email_id ${sendRowId})`.substring(0, 255),
            label: 'ai.email'
          });
        } catch {}
      }

      if (sendRowId) {
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [msg, sendRowId]
          );
        } catch {}
      }
      return res.status(502).json({ success: false, message: msg });
    }
  } catch (e) {
    if (DEBUG) console.warn('[ai.agent.send-document] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to send document' });
  }
});

// Agent-auth endpoint called by the Pipecat agent runtime.
// Sends a custom email (text/html) via the owning user's SMTP settings (no template required).
app.post('/api/ai/agent/send-email', async (req, res) => {
  let sendRowId = null;
  let userId = null;
  const emailCost = AI_EMAIL_SEND_COST;
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const token = parseBearerToken(req.headers.authorization);
    if (!token) return res.status(401).json({ success: false, message: 'Missing Authorization bearer token' });

    const tokenHash = sha256(token);
    const [arows] = await pool.execute(
      'SELECT id, user_id FROM ai_agents WHERE agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(401).json({ success: false, message: 'Invalid token' });

    userId = Number(agent.user_id);
    const agentId = Number(agent.id);

    const toEmail = String(req.body?.to_email || req.body?.toEmail || '').trim();
    if (!toEmail || !isValidEmail(toEmail)) {
      return res.status(400).json({ success: false, message: 'Valid to_email is required' });
    }

    const callId = String(req.body?.call_id || req.body?.callId || '').trim() || null;
    const callDomain = String(req.body?.call_domain || req.body?.callDomain || '').trim() || null;

    const subjectRaw = String(req.body?.subject || '').trim();
    const subject = (subjectRaw ? subjectRaw.slice(0, 255) : 'Message from Phone.System');

    const textRaw = (req.body?.text != null) ? String(req.body.text) : String(req.body?.body || req.body?.message || '');
    const htmlRaw = (req.body?.html != null) ? String(req.body.html) : '';

    const text = String(textRaw || '').trim();
    const html = String(htmlRaw || '').trim();

    if (!text && !html) {
      return res.status(400).json({ success: false, message: 'text/body or html is required' });
    }

    let dedupeKey = String(req.body?.dedupe_key || req.body?.dedupeKey || '').trim();
    if (dedupeKey) {
      if (!/^[a-f0-9]{64}$/i.test(dedupeKey)) {
        return res.status(400).json({ success: false, message: 'dedupe_key must be a 64-char hex sha256' });
      }
      dedupeKey = dedupeKey.toLowerCase();
    } else {
      const contentHash = sha256(`subject:${subject}|text:${text}|html:${html}`);
      dedupeKey = sha256(
        `send-email|agent:${agentId}|call:${callDomain || ''}|${callId || ''}|to:${toEmail.toLowerCase()}|content:${contentHash}`
      );
    }

    // Idempotency + audit row (reuse ai_email_sends with template_id NULL)
    try {
      const [ins] = await pool.execute(
        `INSERT INTO ai_email_sends (user_id, agent_id, template_id, dedupe_key, call_id, call_domain, to_email, subject, status, attempt_count)
         VALUES (?,?,?,?,?,?,?,?, 'pending', 1)`,
        [userId, agentId, null, dedupeKey, callId, callDomain, toEmail, subject]
      );
      sendRowId = ins.insertId;
    } catch (e) {
      if (e?.code !== 'ER_DUP_ENTRY') throw e;
      const [erows] = await pool.execute(
        'SELECT id, status FROM ai_email_sends WHERE dedupe_key = ? LIMIT 1',
        [dedupeKey]
      );
      const existing = erows && erows[0];
      if (existing && existing.status === 'completed') {
        return res.json({ success: true, already_sent: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'pending') {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'failed') {
        sendRowId = existing.id;
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'pending\', attempt_count = attempt_count + 1, error = NULL, to_email = ?, subject = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [toEmail, subject, sendRowId]
          );
        } catch {}
      } else {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
    }

    // Persist preview content (best-effort)
    if (sendRowId) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET email_text = ?, email_html = ?, attachments_json = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [text || null, html || null, sendRowId]
        );
      } catch {}
    }

    if (emailCost > 0) {
      const chargeDesc = `AI email send - ${toEmail}`.substring(0, 255);
      const charge = await chargeAiEmailSendsRow({
        sendRowId,
        userId,
        amount: emailCost,
        description: chargeDesc,
        label: 'ai.email'
      });

      if (!charge || charge.ok !== true) {
        const reason = charge?.reason || 'charge_failed';
        const short = (reason === 'insufficient_funds') ? 'Insufficient balance' : reason;
        const errMsg = `Email charge failed: ${short}`;

        if (sendRowId) {
          try {
            await pool.execute(
              'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
              [errMsg, sendRowId]
            );
          } catch {}
        }

        return res.status(reason === 'insufficient_funds' ? 402 : 500).json({ success: false, message: errMsg });
      }
    }

    try {
      const info = await sendEmailViaUserSmtp({
        userId,
        toEmail,
        subject,
        ...(text ? { text } : {}),
        ...(html ? { html } : {})
      });

      const messageId = info && info.messageId ? String(info.messageId) : null;

      if (sendRowId) {
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'completed\', smtp_message_id = COALESCE(?, smtp_message_id), error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [messageId, sendRowId]
          );
        } catch {}
      }

      return res.json({ success: true, dedupe_key: dedupeKey, already_sent: false });
    } catch (e) {
      const msg = e?.message || 'Email send failed';

      // Best-effort refund if we charged before sending.
      if (emailCost > 0) {
        try {
          await refundAiEmailSendsRow({
            sendRowId,
            userId,
            amount: emailCost,
            description: `Refund: AI email send failed (email_id ${sendRowId})`.substring(0, 255),
            label: 'ai.email'
          });
        } catch {}
      }

      if (sendRowId) {
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [msg, sendRowId]
          );
        } catch {}
      }
      return res.status(502).json({ success: false, message: msg });
    }
  } catch (e) {
    const msg = e?.message || 'Failed to send email';
    if (DEBUG) console.warn('[ai.agent.send-email] error:', msg);

    // Best-effort refund if we already charged.
    if (sendRowId && pool && userId && emailCost > 0) {
      try {
        await refundAiEmailSendsRow({
          sendRowId,
          userId,
          amount: emailCost,
          description: `Refund: AI email send failed (email_id ${sendRowId})`.substring(0, 255),
          label: 'ai.email'
        });
      } catch {}
    }

    if (sendRowId && pool) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
    }
    return res.status(500).json({ success: false, message: msg });
  }
});

// SMS sending has been removed.
app.post('/api/ai/agent/send-sms', (req, res) => {
  return res.status(410).json({ success: false, message: 'SMS sending has been removed' });
});

// Agent-auth endpoint: check SMS delivery status (local DB)
app.get('/api/ai/agent/sms-status', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const token = parseBearerToken(req.headers.authorization);
    if (!token) return res.status(401).json({ success: false, message: 'Missing Authorization bearer token' });

    const tokenHash = sha256(token);
    const [arows] = await pool.execute(
      'SELECT id, user_id FROM ai_agents WHERE agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(401).json({ success: false, message: 'Invalid token' });

    const agentId = Number(agent.id);
    const userId = Number(agent.user_id);

    const providerMessageId = String(req.query.provider_message_id || req.query.providerMessageId || '').trim();
    if (!providerMessageId) {
      return res.status(400).json({ success: false, message: 'provider_message_id is required' });
    }

    const [rows] = await pool.execute(
      `SELECT id, created_at, updated_at,
              from_number, to_number, message_text,
              status, error,
              provider_message_id, provider_status, provider_code_id, provider_fragments_sent, provider_price, provider_time_start, provider_time_end,
              dlr_status, dlr_time_start
       FROM ai_sms_sends
       WHERE user_id = ? AND agent_id = ? AND provider_message_id = ?
       LIMIT 1`,
      [userId, agentId, providerMessageId]
    );

    const row = rows && rows[0];
    if (!row) {
      return res.status(404).json({ success: false, message: 'SMS not found' });
    }

    return res.json({ success: true, data: row });
  } catch (e) {
    if (DEBUG) console.warn('[ai.agent.sms-status] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load SMS status' });
  }
});

function parseClick2MailDateTimeToMysql(dtStr) {
  const s = String(dtStr || '').trim();
  if (!s) return null;
  try {
    const d = new Date(s);
    if (!Number.isNaN(d.getTime())) {
      // Convert to local-time-less MySQL DATETIME string
      const iso = new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 19);
      return iso.replace('T', ' ');
    }
  } catch {}
  return null;
}

function pickLatestTrackingItem(items) {
  const arr = Array.isArray(items) ? items : [];
  if (!arr.length) return null;

  // Prefer items with a parseable dateTime.
  const withDt = arr
    .map(it => {
      const dt = parseClick2MailDateTimeToMysql(it?.dateTime);
      return { it, dt };
    })
    .filter(x => !!x.dt);

  if (withDt.length) {
    withDt.sort((a, b) => String(a.dt).localeCompare(String(b.dt)));
    return withDt[withDt.length - 1].it;
  }

  return arr[arr.length - 1];
}

async function chargeAiEmailSendsRow({ sendRowId, userId, amount, description, label }) {
  const amt = Math.max(0, Number(amount) || 0);
  if (!pool) throw new Error('Database not configured');
  if (!sendRowId || !userId) return { ok: false, reason: 'missing_params' };
  if (!(amt > 0)) return { ok: true, skipped: true };

  // Skip if already charged
  try {
    const [rows] = await pool.execute(
      'SELECT billing_history_id FROM ai_email_sends WHERE id = ? LIMIT 1',
      [sendRowId]
    );
    const existing = rows && rows[0] && rows[0].billing_history_id ? Number(rows[0].billing_history_id) : null;
    if (existing) return { ok: true, already_charged: true, billingId: existing };
  } catch {}

  const charge = await deductBalance(userId, amt, description || label);

  if (!charge || charge.ok !== true) {
    return { ok: false, reason: charge?.reason || 'charge_failed', billingId: charge?.billingId || null, creditBefore: charge?.creditBefore };
  }

  const billingId = charge.billingId;

  // Best-effort persist charge metadata for refunds / idempotency
  try {
    await pool.execute(
      `UPDATE ai_email_sends
       SET billing_history_id = ?,
           refund_status = 'none',
           refund_amount = NULL,
           refund_billing_history_id = NULL,
           refund_error = NULL,
           refunded_at = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ? LIMIT 1`,
      [billingId, sendRowId]
    );
  } catch {}

  return { ok: true, billingId, amount: Math.abs(amt) };
}

async function refundAiEmailSendsRow({ sendRowId, userId, amount, description, label }) {
  const amt = Math.max(0, Number(amount) || 0);
  if (!pool) throw new Error('Database not configured');
  if (!sendRowId || !userId) return { ok: false, reason: 'missing_params' };
  if (!(amt > 0)) return { ok: true, skipped: true };

  // Best-effort claim to avoid double refunds
  let canRefund = true;
  try {
    const [r] = await pool.execute(
      `UPDATE ai_email_sends
       SET refund_status = 'pending',
           refund_amount = ?,
           refund_error = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?
         AND (refund_status = 'none' OR refund_status = 'failed')
         AND billing_history_id IS NOT NULL
       LIMIT 1`,
      [Math.abs(amt), sendRowId]
    );
    canRefund = !!(r && r.affectedRows);
  } catch {
    canRefund = true;
  }

  if (!canRefund) return { ok: true, skipped: true };

  const refund = await creditBalance(userId, Math.abs(amt), String(description || '').substring(0, 255));

  if (refund && refund.ok === true) {
    try {
      await pool.execute(
        `UPDATE ai_email_sends
         SET refund_status = 'completed',
             refund_billing_history_id = ?,
             refunded_at = NOW(),
             billing_history_id = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ? LIMIT 1`,
        [refund.billingId, sendRowId]
      );
    } catch {}
    return { ok: true, refundBillingId: refund.billingId, amount: Math.abs(amt) };
  }

  const err = refund?.reason || 'Refund failed';
  try {
    await pool.execute(
      `UPDATE ai_email_sends
       SET refund_status = 'failed',
           refund_error = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ? LIMIT 1`,
      [String(err || 'Refund failed').slice(0, 2000), sendRowId]
    );
  } catch {}

  return { ok: false, reason: err };
}

async function chargeAiSmsSendsRow({ sendRowId, userId, amount, description, label }) {
  const amt = Math.max(0, Number(amount) || 0);
  if (!pool) throw new Error('Database not configured');
  if (!sendRowId || !userId) return { ok: false, reason: 'missing_params' };
  if (!(amt > 0)) return { ok: true, skipped: true };

  // Skip if already charged
  try {
    const [rows] = await pool.execute(
      'SELECT billing_history_id FROM ai_sms_sends WHERE id = ? LIMIT 1',
      [sendRowId]
    );
    const existing = rows && rows[0] && rows[0].billing_history_id ? Number(rows[0].billing_history_id) : null;
    if (existing) return { ok: true, already_charged: true, billingId: existing };
  } catch {}

  const charge = await deductBalance(userId, amt, description || label);

  if (!charge || charge.ok !== true) {
    return { ok: false, reason: charge?.reason || 'charge_failed', billingId: charge?.billingId || null, creditBefore: charge?.creditBefore };
  }

  const billingId = charge.billingId;

  // Best-effort persist charge metadata for refunds / idempotency
  try {
    await pool.execute(
      `UPDATE ai_sms_sends
       SET billing_history_id = ?,
           refund_status = 'none',
           refund_amount = NULL,
           refund_billing_history_id = NULL,
           refund_error = NULL,
           refunded_at = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ? LIMIT 1`,
      [billingId, sendRowId]
    );
  } catch {}

  return { ok: true, billingId, amount: Math.abs(amt) };
}

async function refundAiSmsSendsRow({ sendRowId, userId, amount, description, label }) {
  const amt = Math.max(0, Number(amount) || 0);
  if (!pool) throw new Error('Database not configured');
  if (!sendRowId || !userId) return { ok: false, reason: 'missing_params' };
  if (!(amt > 0)) return { ok: true, skipped: true };

  // Best-effort claim to avoid double refunds
  let canRefund = true;
  try {
    const [r] = await pool.execute(
      `UPDATE ai_sms_sends
       SET refund_status = 'pending',
           refund_amount = ?,
           refund_error = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?
         AND (refund_status = 'none' OR refund_status = 'failed')
         AND billing_history_id IS NOT NULL
       LIMIT 1`,
      [Math.abs(amt), sendRowId]
    );
    canRefund = !!(r && r.affectedRows);
  } catch {
    canRefund = true;
  }

  if (!canRefund) return { ok: true, skipped: true };

  const refund = await creditBalance(userId, Math.abs(amt), String(description || '').substring(0, 255));

  if (refund && refund.ok === true) {
    try {
      await pool.execute(
        `UPDATE ai_sms_sends
         SET refund_status = 'completed',
             refund_billing_history_id = ?,
             refunded_at = NOW(),
             billing_history_id = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ? LIMIT 1`,
        [refund.billingId, sendRowId]
      );
    } catch {}
    return { ok: true, refundBillingId: refund.billingId, amount: Math.abs(amt) };
  }

  const err = refund?.reason || 'Refund failed';
  try {
    await pool.execute(
      `UPDATE ai_sms_sends
       SET refund_status = 'failed',
           refund_error = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ? LIMIT 1`,
      [String(err || 'Refund failed').slice(0, 2000), sendRowId]
    );
  } catch {}

  return { ok: false, reason: err };
}

// Agent-auth endpoint called by the Pipecat agent runtime.
// Sends a custom physical letter via Click2Mail (batch API) without DOCX templates.
app.post('/api/ai/agent/send-custom-physical-mail', async (req, res) => {
  let sendRowId = null;
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    if (!AI_PHYSICAL_MAIL_ENABLED) {
      return res.status(403).json({ success: false, message: 'Physical mail is disabled by configuration' });
    }

    const token = parseBearerToken(req.headers.authorization);
    if (!token) return res.status(401).json({ success: false, message: 'Missing Authorization bearer token' });

    const tokenHash = sha256(token);
    const [arows] = await pool.execute(
      'SELECT id, user_id FROM ai_agents WHERE agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(401).json({ success: false, message: 'Invalid token' });

    const userId = Number(agent.user_id);
    const agentId = Number(agent.id);

    // Recipient address
    const toName = String(req.body?.to_name || req.body?.toName || req.body?.name || '').trim() || null;
    const toOrg = String(req.body?.to_organization || req.body?.toOrganization || req.body?.organization || '').trim() || null;
    const toAddress1 = String(req.body?.to_address1 || req.body?.toAddress1 || req.body?.address1 || '').trim();
    const toAddress2 = String(req.body?.to_address2 || req.body?.toAddress2 || req.body?.address2 || '').trim() || null;
    const toAddress3 = String(req.body?.to_address3 || req.body?.toAddress3 || req.body?.address3 || '').trim() || null;
    const toCity = String(req.body?.to_city || req.body?.toCity || req.body?.city || '').trim();
    const toStateRaw = String(req.body?.to_state || req.body?.toState || req.body?.state || '').trim();
    const toPostalRaw = (req.body?.to_postal_code ?? req.body?.toPostalCode ?? req.body?.postal_code ?? req.body?.postalCode ?? req.body?.zip ?? '');
    const toCountryRaw = String(req.body?.to_country || req.body?.toCountry || req.body?.country || 'US').trim();

    const toCountry = normalizeClick2MailCountry(toCountryRaw || 'US');
    const toState = (toCountry === 'US') ? normalizeUsState2(toStateRaw) : toStateRaw;
    const toPostalCode = normalizePostalCode(toPostalRaw);

    if (!toAddress1) return res.status(400).json({ success: false, message: 'to_address1 is required' });
    if (!toCity) return res.status(400).json({ success: false, message: 'to_city is required' });
    if (!toState) return res.status(400).json({ success: false, message: 'to_state is required' });
    if (!toPostalCode) return res.status(400).json({ success: false, message: 'to_postal_code is required' });

    const bodyRaw = String(req.body?.body ?? req.body?.text ?? req.body?.message ?? '').trim();
    if (!bodyRaw) {
      return res.status(400).json({ success: false, message: 'body is required' });
    }

    const subjectRaw = String(req.body?.subject || '').trim();
    const subject = subjectRaw ? subjectRaw.slice(0, 255) : '';

    const callId = String(req.body?.call_id || req.body?.callId || '').trim() || null;
    const callDomain = String(req.body?.call_domain || req.body?.callDomain || '').trim() || null;

    // Default product options (can be overridden per-call if needed)
    const documentClass = String(req.body?.document_class || req.body?.documentClass || CLICK2MAIL_DEFAULT_DOCUMENT_CLASS).trim();
    const layout = String(req.body?.layout || CLICK2MAIL_DEFAULT_LAYOUT).trim();
    const productionTime = String(req.body?.production_time || req.body?.productionTime || CLICK2MAIL_DEFAULT_PRODUCTION_TIME).trim();
    const envelope = String(req.body?.envelope || CLICK2MAIL_DEFAULT_ENVELOPE).trim();
    const color = String(req.body?.color || CLICK2MAIL_DEFAULT_COLOR).trim();
    const paperType = String(req.body?.paper_type || req.body?.paperType || CLICK2MAIL_DEFAULT_PAPER_TYPE).trim();
    const printOption = String(req.body?.print_option || req.body?.printOption || CLICK2MAIL_DEFAULT_PRINT_OPTION).trim();
    const mailClass = String(req.body?.mail_class || req.body?.mailClass || CLICK2MAIL_DEFAULT_MAIL_CLASS).trim();
    const trackingType = String(req.body?.tracking_type || req.body?.trackingType || CLICK2MAIL_DEFAULT_TRACKING_TYPE).trim();

    let dedupeKey = String(req.body?.dedupe_key || req.body?.dedupeKey || '').trim();
    if (dedupeKey) {
      if (!/^[a-f0-9]{64}$/i.test(dedupeKey)) {
        return res.status(400).json({ success: false, message: 'dedupe_key must be a 64-char hex sha256' });
      }
      dedupeKey = dedupeKey.toLowerCase();
    } else {
      const addrKey = `${toAddress1}|${toCity}|${toState}|${toPostalCode}|${toCountry}`.toLowerCase();
      const contentHash = sha256(`subject:${subject}|body:${bodyRaw}`);
      dedupeKey = sha256(
        `send-custom-physical-mail|agent:${agentId}|call:${callDomain || ''}|${callId || ''}|to:${addrKey}|content:${contentHash}`
      );
    }

    // Idempotency + audit row
    try {
      const [ins] = await pool.execute(
        `INSERT INTO ai_mail_sends (
          user_id, agent_id, template_id, dedupe_key, call_id, call_domain,
          to_name, to_organization, to_address1, to_address2, to_address3,
          to_city, to_state, to_postal_code, to_country,
          provider, product, mail_class, tracking_type,
          status, attempt_count
        ) VALUES (
          ?,?,?,?,?,?,
          ?,?,?,?,?,
          ?,?,?,?,
          'click2mail', ?, ?, ?,
          'pending', 1
        )`,
        [
          userId, agentId, null, dedupeKey, callId, callDomain,
          toName, toOrg, toAddress1, toAddress2, toAddress3,
          toCity, toState, toPostalCode, toCountry,
          documentClass || null, mailClass || null, trackingType || null
        ]
      );
      sendRowId = ins.insertId;
    } catch (e) {
      if (e?.code !== 'ER_DUP_ENTRY') throw e;
      const [erows] = await pool.execute(
        'SELECT id, status, billing_history_id, batch_id FROM ai_mail_sends WHERE dedupe_key = ? LIMIT 1',
        [dedupeKey]
      );
      const existing = erows && erows[0];
      if (existing && (existing.status === 'completed' || existing.status === 'submitted')) {
        return res.json({ success: true, already_sent: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'pending') {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'failed') {
        sendRowId = existing.id;
        try {
          await pool.execute(
            'UPDATE ai_mail_sends SET status = \'pending\', attempt_count = attempt_count + 1, error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [sendRowId]
          );
        } catch {}
      } else {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
    }

    // Ensure return address is configured
    const returnAddrRow = await loadUserMailSettings(userId);
    if (!returnAddrRow) {
      const msg = 'Mail settings not configured (set a return address in AI Agent → Tools → Mail)';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(400).json({ success: false, message: msg });
    }

    // Address correction (recipient)
    let corrected = {
      name: toName || '',
      organization: toOrg || '',
      address1: toAddress1,
      address2: toAddress2 || '',
      address3: toAddress3 || '',
      city: toCity,
      state: toState,
      postalCode: toPostalCode,
      country: toCountry
    };

    let acExtracted = null;
    try {
      const acResp = await click2mailAddressCorrectionOne({
        name: corrected.name,
        organization: corrected.organization,
        address1: corrected.address1,
        address2: corrected.address2,
        address3: corrected.address3,
        city: corrected.city,
        state: corrected.state,
        postalCode: corrected.postalCode,
        country: corrected.country
      });
      acExtracted = extractClick2MailAddressCorrectionFromXml(acResp.parsed);

      if (acExtracted && acExtracted.address) {
        const a = acExtracted.address;
        corrected = {
          name: a.name || corrected.name,
          organization: a.organization || corrected.organization,
          address1: a.address1 || corrected.address1,
          address2: a.address2 || corrected.address2,
          address3: a.address3 || corrected.address3,
          city: a.city || corrected.city,
          state: a.state || corrected.state,
          postalCode: a.postalCode || corrected.postalCode,
          country: normalizeClick2MailCountry(a.country || corrected.country)
        };
      }

      if (acExtracted && acExtracted.nonmailable === true) {
        const msg = 'Recipient address is not mailable';
        try {
          await pool.execute(
            'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [msg, sendRowId]
          );
        } catch {}
        return res.status(400).json({ success: false, message: msg });
      }
    } catch (e) {
      const msg = e?.message || 'Address correction failed';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(502).json({ success: false, message: msg });
    }

    // Persist corrected address
    try {
      await pool.execute(
        `UPDATE ai_mail_sends
         SET corrected_to_name = ?, corrected_to_organization = ?,
             corrected_to_address1 = ?, corrected_to_address2 = ?, corrected_to_address3 = ?,
             corrected_to_city = ?, corrected_to_state = ?, corrected_to_postal_code = ?, corrected_to_country = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ? LIMIT 1`,
        [
          corrected.name || null,
          corrected.organization || null,
          corrected.address1 || null,
          corrected.address2 || null,
          corrected.address3 || null,
          corrected.city || null,
          corrected.state || null,
          corrected.postalCode || null,
          corrected.country || null,
          sendRowId
        ]
      );
    } catch {}

    // Generate PDF (required for Click2Mail)
    let pdfOut = null;
    let pageCount = 0;

    try {
      const includeBlankAddressPage = /separate\s*page/i.test(layout || '');
      pdfOut = await generateSimpleLetterPdfBuffer({
        subject: subject,
        body: bodyRaw,
        includeBlankAddressPage
      });
    } catch (e) {
      const msg = e?.message || 'PDF generation failed';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(500).json({ success: false, message: msg });
    }

    if (!pdfOut) {
      const msg = 'PDF generation failed';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(500).json({ success: false, message: msg });
    }

    pageCount = Math.max(1, estimatePdfPageCount(pdfOut) || 0);

    // Cost estimate
    let costAmount = null;
    let markupAmount = null;
    let totalCost = null;
    try {
      const isInternational = String(corrected.country || 'US').toUpperCase() !== 'US';
      const isNonStandard = (!isInternational && acExtracted && acExtracted.standard === false);

      const qtyStandard = (!isInternational && !isNonStandard) ? 1 : 0;
      const qtyNonStandard = isNonStandard ? 1 : 0;
      const qtyInternational = isInternational ? 1 : 0;

      const costParams = {
        documentClass,
        layout,
        productionTime,
        envelope,
        color,
        paperType,
        printOption,
        mailClass,
        quantity: String(qtyStandard),
        nonStandardQuantity: String(qtyNonStandard),
        internationalQuantity: String(qtyInternational),
        numberOfPages: String(pageCount),
        paymentType: 'User Credit'
      };

      const costResp = await click2mailCostEstimate(costParams);
      const extracted = extractClick2MailCostEstimateFromXml(costResp.parsed);
      if (extracted && Number.isFinite(Number(extracted.amount))) {
        costAmount = Number(extracted.amount);
      }

      if (!(costAmount != null && costAmount > 0)) {
        throw new Error('Unable to calculate cost estimate');
      }

      // Platform markup
      const pct = Math.max(0, Number(AI_MAIL_MARKUP_PERCENT || 0));
      const flat = Math.max(0, Number(AI_MAIL_MARKUP_FLAT || 0));
      markupAmount = flat + (pct > 0 ? (costAmount * pct / 100.0) : 0);
      totalCost = costAmount + markupAmount;

      if (!(totalCost != null && Number.isFinite(Number(totalCost)) && totalCost > 0)) {
        throw new Error('Unable to calculate total cost');
      }

      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET cost_estimate = ?, markup_amount = ?, total_cost = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [costAmount, markupAmount || 0, totalCost, sendRowId]
        );
      } catch {}
    } catch (e) {
      const msg = e?.message || 'Cost estimate failed';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(502).json({ success: false, message: msg });
    }

    // Charge the user (skip if already charged on a retry)
    let billingId = null;
    let amountToCharge = (totalCost != null && Number.isFinite(Number(totalCost))) ? Number(totalCost) : Number(costAmount || 0);
    if (!Number.isFinite(amountToCharge) || amountToCharge <= 0) amountToCharge = 0;

    try {
      const [rows] = await pool.execute('SELECT billing_history_id FROM ai_mail_sends WHERE id = ? LIMIT 1', [sendRowId]);
      billingId = rows && rows[0] && rows[0].billing_history_id ? Number(rows[0].billing_history_id) : null;
    } catch {}

    if (!billingId) {
      const descRaw = `AI physical mail - ${documentClass || 'Mail'} - ${corrected.city || ''} ${corrected.state || ''} ${corrected.postalCode || ''}`.trim();
      const description = descRaw.substring(0, 255);

      const charge = await deductBalance(userId, amountToCharge, description);

      if (!charge || charge.ok !== true) {
        const reason = charge?.reason === 'insufficient_funds' ? 'Insufficient balance' : (charge?.reason || 'Charge failed');
        const msg = `Mail charge failed: ${reason}`;
        try {
          await pool.execute(
            'UPDATE ai_mail_sends SET status = \'failed\', error = ?, billing_history_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [msg, charge?.billingId || null, sendRowId]
          );
        } catch {}
        return res.status(402).json({ success: false, message: msg });
      }

      billingId = charge.billingId;

      try {
        await pool.execute(
          `UPDATE ai_mail_sends
           SET billing_history_id = ?,
               refund_status = 'none',
               refund_amount = NULL,
               refund_billing_history_id = NULL,
               refund_error = NULL,
               refunded_at = NULL,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = ? LIMIT 1`,
          [billingId, sendRowId]
        );
      } catch {}
    }

    // Create + submit Click2Mail batch
    let batchId = null;
    let trackingNumber = null;
    let trackingStatus = null;
    let trackingStatusDate = null;
    let trackingStatusLocation = null;

    const pdfFilename = `mail-${sendRowId}.pdf`;
    const xmlFilename = `mail-${sendRowId}.xml`;

    const payloadLog = {
      product: { documentClass, layout, productionTime, envelope, color, paperType, printOption, mailClass },
      pageCount,
      cost_estimate: costAmount,
      markup_amount: markupAmount,
      total_cost: totalCost,
      billing_history_id: billingId,
      amount_charged: amountToCharge,
      content: {
        subject: subject || null,
        body_preview: String(bodyRaw || '').slice(0, 2000)
      }
    };

    try {
      const batch = await click2mailBatchCreate();
      batchId = batch.batchId;
      if (!batchId) throw new Error('Click2Mail batch id missing');

      await click2mailBatchUploadPdf(batchId, pdfOut, { filename: pdfFilename });

      const returnName = String(returnAddrRow.name || '').trim();
      const returnOrg = String(returnAddrRow.organization || '').trim();
      const returnAddress1 = String(returnAddrRow.address1 || '').trim();
      const returnAddress2 = String(returnAddrRow.address2 || '').trim();
      const returnCity = String(returnAddrRow.city || '').trim();
      const returnState = String(returnAddrRow.state || '').trim();
      const returnPostal = String(returnAddrRow.postal_code || '').trim();
      const returnCountry = normalizeClick2MailCountry(returnAddrRow.country || 'US');

      // Click2Mail batch XSD expects <name> as the first element within returnAddress/address.
      const returnNameFinal = (returnName || returnOrg || 'Sender').trim();
      const recipientNameFinal = String(corrected.name || corrected.organization || 'Recipient').trim();
      const recipientOrg = String(corrected.organization || '').trim();

      const batchXml =
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<batch xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">` +
          `<filename>${xmlEscape(pdfFilename)}</filename>` +
          `<appSignature>${xmlEscape(CLICK2MAIL_APP_SIGNATURE)}</appSignature>` +
          `<job>` +
            `<startingPage>1</startingPage>` +
            `<endingPage>${xmlEscape(String(pageCount))}</endingPage>` +
            `<printProductionOptions>` +
              `<documentClass>${xmlEscape(documentClass)}</documentClass>` +
              `<layout>${xmlEscape(layout)}</layout>` +
              `<productionTime>${xmlEscape(productionTime)}</productionTime>` +
              `<envelope>${xmlEscape(envelope)}</envelope>` +
              `<color>${xmlEscape(color)}</color>` +
              `<paperType>${xmlEscape(paperType)}</paperType>` +
              `<printOption>${xmlEscape(printOption)}</printOption>` +
              `<mailClass>${xmlEscape(mailClass)}</mailClass>` +
            `</printProductionOptions>` +
            `<returnAddress>` +
              `<name>${xmlEscape(returnNameFinal)}</name>` +
              `<organization>${xmlEscape(returnOrg)}</organization>` +
              `<address1>${xmlEscape(returnAddress1)}</address1>` +
              (returnAddress2 ? `<address2>${xmlEscape(returnAddress2)}</address2>` : '<address2></address2>') +
              `<city>${xmlEscape(returnCity)}</city>` +
              `<state>${xmlEscape(returnState)}</state>` +
              `<postalCode>${xmlEscape(returnPostal)}</postalCode>` +
              `<country>${xmlEscape(returnCountry)}</country>` +
            `</returnAddress>` +
            `<recipients>` +
              `<address>` +
                `<name>${xmlEscape(recipientNameFinal)}</name>` +
                `<organization>${xmlEscape(recipientOrg)}</organization>` +
                `<address1>${xmlEscape(corrected.address1)}</address1>` +
                (corrected.address2 ? `<address2>${xmlEscape(corrected.address2)}</address2>` : '<address2></address2>') +
                (corrected.address3 ? `<address3>${xmlEscape(corrected.address3)}</address3>` : '<address3></address3>') +
                `<city>${xmlEscape(corrected.city)}</city>` +
                `<state>${xmlEscape(corrected.state)}</state>` +
                `<postalCode>${xmlEscape(corrected.postalCode)}</postalCode>` +
                `<country>${xmlEscape(corrected.country)}</country>` +
                `<c2m_uniqueid/>` +
              `</address>` +
            `</recipients>` +
          `</job>` +
        `</batch>`;

      await click2mailBatchUploadXml(batchId, batchXml, { filename: xmlFilename });

      const submit = await click2mailBatchSubmit(batchId);

      // Best-effort: grab tracking (may not be available immediately)
      let tracking = null;
      try {
        const tr = await click2mailBatchTracking(batchId, trackingType || 'impb');
        tracking = tr?.extracted || null;
        const latest = tracking ? pickLatestTrackingItem(tracking.items) : null;
        if (latest && latest.barCode) {
          trackingNumber = String(latest.barCode).trim() || null;
          trackingStatus = String(latest.status || '').trim() || null;
          trackingStatusLocation = String(latest.statusLocation || '').trim() || null;
          trackingStatusDate = parseClick2MailDateTimeToMysql(latest.dateTime);
        }
      } catch {}

      payloadLog.click2mail = {
        batchCreate: { batchId },
        submitStatus: submit?.status,
        tracking
      };

      try {
        await pool.execute(
          `UPDATE ai_mail_sends
           SET batch_id = ?, status = 'submitted',
               tracking_number = ?, tracking_status = ?, tracking_status_date = ?, tracking_status_location = ?,
               raw_payload = ?, updated_at = CURRENT_TIMESTAMP
           WHERE id = ? LIMIT 1`,
          [
            batchId,
            trackingNumber,
            trackingStatus,
            trackingStatusDate,
            trackingStatusLocation,
            JSON.stringify(payloadLog),
            sendRowId
          ]
        );
      } catch {}

      return res.json({
        success: true,
        dedupe_key: dedupeKey,
        already_sent: false,
        batch_id: batchId,
        tracking_number: trackingNumber,
        cost_estimate: costAmount,
        markup_amount: markupAmount,
        total_cost: totalCost
      });
    } catch (e) {
      // Build a more helpful error message from Click2Mail (often returns XML details).
      const httpStatus = e?.response?.status;
      const respText = (typeof e?.response?.data === 'string') ? String(e.response.data) : '';

      let detail = '';
      try {
        const parsedErr = click2mailParseXml(respText);
        const d = String(pickFirst(parsedErr, [
          'error.description',
          'errors.error.description',
          'description',
          'message',
          'faultstring',
          'faultString'
        ]) || '').trim();
        if (d) detail = d;
      } catch {}

      let msg = detail || (e?.message || 'Click2Mail send failed');

      const responseSnippet = respText ? respText.slice(0, 2000) : '';
      const cleanedSnippet = responseSnippet
        ? responseSnippet
            .replace(/<[^>]+>/g, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .slice(0, 500)
        : '';

      if (!detail && cleanedSnippet) {
        // axios's generic message isn't very helpful; prefer provider response if available.
        if (/request failed with status code/i.test(msg) || msg === 'Click2Mail send failed') {
          msg = cleanedSnippet;
        } else if (!msg.includes(cleanedSnippet)) {
          msg = `${msg}: ${cleanedSnippet}`;
        }
      }

      if (httpStatus && !/\(HTTP\s*\d+\)/i.test(msg) && !/status code\s*\d+/i.test(msg)) {
        msg = `${msg} (HTTP ${httpStatus})`;
      }

      if (responseSnippet) {
        payloadLog.click2mail_error = { status: httpStatus || null, body: responseSnippet };
      } else if (httpStatus) {
        payloadLog.click2mail_error = { status: httpStatus };
      }

      // If we already charged the user, attempt an automatic refund.
      let refundAttempted = false;
      let refundOk = false;
      let refundBillingId = null;
      let refundError = null;

      try {
        if (billingId && amountToCharge && Number.isFinite(Number(amountToCharge)) && Number(amountToCharge) > 0) {
          refundAttempted = true;

          // Best-effort claim to avoid double refunds (uses refund_status state).
          let canRefund = true;
          try {
            const [r] = await pool.execute(
              `UPDATE ai_mail_sends
               SET refund_status = 'pending',
                   refund_amount = ?,
                   refund_error = NULL,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = ?
                 AND (refund_status = 'none' OR refund_status = 'failed')
                 AND billing_history_id IS NOT NULL
               LIMIT 1`,
              [Math.abs(Number(amountToCharge)), sendRowId]
            );
            canRefund = !!(r && r.affectedRows);
          } catch {
            canRefund = true;
          }

          if (canRefund) {
            {
              const refundDescRaw = `Refund: AI mail send failed (mail_id ${sendRowId})`;
              const refundDescription = refundDescRaw.substring(0, 255);

              const refund = await creditBalance(userId, Math.abs(Number(amountToCharge)), refundDescription);

              if (refund && refund.ok === true) {
                refundOk = true;
                refundBillingId = refund.billingId;
              } else {
                refundOk = false;
                refundError = refund?.reason || 'Refund failed';
              }
            }

            // Persist refund state (best-effort)
            try {
              if (refundOk) {
                await pool.execute(
                  `UPDATE ai_mail_sends
                   SET refund_status = 'completed',
                       refund_billing_history_id = ?,
                       refunded_at = NOW(),
                       billing_history_id = NULL,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = ? LIMIT 1`,
                  [refundBillingId, sendRowId]
                );
              } else {
                await pool.execute(
                  `UPDATE ai_mail_sends
                   SET refund_status = 'failed',
                       refund_error = ?,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = ? LIMIT 1`,
                  [String(refundError || 'Refund failed').slice(0, 2000), sendRowId]
                );
              }
            } catch {}
          } else {
            refundAttempted = false;
          }
        }
      } catch (refundErr) {
        refundAttempted = true;
        refundOk = false;
        refundError = String(refundErr?.message || refundErr);
        try {
          await pool.execute(
            `UPDATE ai_mail_sends
             SET refund_status = 'failed',
                 refund_error = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = ? LIMIT 1`,
            [String(refundError || 'Refund failed').slice(0, 2000), sendRowId]
          );
        } catch {}
      }

      if (refundAttempted) {
        payloadLog.refund = {
          attempted: true,
          ok: refundOk,
          amount: Math.abs(Number(amountToCharge || 0)) || 0,
          refund_billing_history_id: refundBillingId || null,
          error: refundOk ? null : (refundError || null)
        };
      }

      const msgForRow = msg;

      if (DEBUG) console.warn('[ai.agent.send-custom-physical-mail] error:', msgForRow);

      try {
        await pool.execute(
          `UPDATE ai_mail_sends
           SET status = 'failed',
               error = ?,
               batch_id = COALESCE(batch_id, ?),
               raw_payload = ?,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = ? LIMIT 1`,
          [String(msgForRow).slice(0, 2000), batchId, JSON.stringify(payloadLog), sendRowId]
        );
      } catch {}

      return res.status(502).json({
        success: false,
        message: msgForRow,
        charged: !!billingId,
        charge_billing_history_id: billingId || null,
        refunded: refundOk,
        refund_billing_history_id: refundBillingId || null
      });
    }
  } catch (e) {
    const msg = e?.message || 'Failed to send physical mail';
    if (DEBUG) console.warn('[ai.agent.send-custom-physical-mail] error:', msg);
    if (sendRowId && pool) {
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
    }
    return res.status(500).json({ success: false, message: msg });
  }
});

// Agent-auth endpoint called by the Pipecat agent runtime.
// Sends a physical letter via Click2Mail (batch API).
app.post('/api/ai/agent/send-physical-mail', async (req, res) => {
  let sendRowId = null;
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    if (!AI_PHYSICAL_MAIL_ENABLED) {
      return res.status(403).json({ success: false, message: 'Physical mail is disabled by configuration' });
    }

    const token = parseBearerToken(req.headers.authorization);
    if (!token) return res.status(401).json({ success: false, message: 'Missing Authorization bearer token' });

    const tokenHash = sha256(token);
    const [arows] = await pool.execute(
      'SELECT id, user_id, default_doc_template_id FROM ai_agents WHERE agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(401).json({ success: false, message: 'Invalid token' });

    const userId = Number(agent.user_id);
    const agentId = Number(agent.id);

    // Recipient address
    const toName = String(req.body?.to_name || req.body?.toName || req.body?.name || '').trim() || null;
    const toOrg = String(req.body?.to_organization || req.body?.toOrganization || req.body?.organization || '').trim() || null;
    const toAddress1 = String(req.body?.to_address1 || req.body?.toAddress1 || req.body?.address1 || '').trim();
    const toAddress2 = String(req.body?.to_address2 || req.body?.toAddress2 || req.body?.address2 || '').trim() || null;
    const toAddress3 = String(req.body?.to_address3 || req.body?.toAddress3 || req.body?.address3 || '').trim() || null;
    const toCity = String(req.body?.to_city || req.body?.toCity || req.body?.city || '').trim();
    const toStateRaw = String(req.body?.to_state || req.body?.toState || req.body?.state || '').trim();
    const toPostalRaw = (req.body?.to_postal_code ?? req.body?.toPostalCode ?? req.body?.postal_code ?? req.body?.postalCode ?? req.body?.zip ?? '');
    const toCountryRaw = String(req.body?.to_country || req.body?.toCountry || req.body?.country || 'US').trim();

    const toCountry = normalizeClick2MailCountry(toCountryRaw || 'US');
    const toState = (toCountry === 'US') ? normalizeUsState2(toStateRaw) : toStateRaw;
    const toPostalCode = normalizePostalCode(toPostalRaw);

    if (!toAddress1) return res.status(400).json({ success: false, message: 'to_address1 is required' });
    if (!toCity) return res.status(400).json({ success: false, message: 'to_city is required' });
    if (!toState) return res.status(400).json({ success: false, message: 'to_state is required' });
    if (!toPostalCode) return res.status(400).json({ success: false, message: 'to_postal_code is required' });

    let variables = (req.body && (req.body.variables || req.body.vars)) || {};
    if (!variables || typeof variables !== 'object') variables = {};

    const callId = String(req.body?.call_id || req.body?.callId || '').trim() || null;
    const callDomain = String(req.body?.call_domain || req.body?.callDomain || '').trim() || null;

    // Resolve template id (explicit wins; otherwise agent default)
    const templateRaw = (req.body?.template_id !== undefined) ? req.body.template_id : req.body?.templateId;
    let templateId = null;
    if (templateRaw != null && String(templateRaw).trim()) {
      const tid = parseInt(String(templateRaw).trim(), 10);
      if (!Number.isFinite(tid) || tid <= 0) {
        return res.status(400).json({ success: false, message: 'template_id is invalid' });
      }
      templateId = tid;
    } else if (agent.default_doc_template_id) {
      templateId = Number(agent.default_doc_template_id);
    }

    // Fallback: if this user only has one template and none was specified, auto-select it.
    if (!templateId) {
      const [cand] = await pool.execute(
        'SELECT id FROM ai_doc_templates WHERE user_id = ? ORDER BY created_at DESC LIMIT 2',
        [userId]
      );
      if (cand && cand.length === 1) {
        templateId = Number(cand[0].id);
      } else if (!cand || !cand.length) {
        return res.status(400).json({ success: false, message: 'No document templates are available. Upload a .docx template in the dashboard first.' });
      } else {
        return res.status(400).json({ success: false, message: 'No template selected (set a default template for this agent or pass template_id)' });
      }
    }

    // Default product options (can be overridden per-call if needed)
    const documentClass = String(req.body?.document_class || req.body?.documentClass || CLICK2MAIL_DEFAULT_DOCUMENT_CLASS).trim();
    const layout = String(req.body?.layout || CLICK2MAIL_DEFAULT_LAYOUT).trim();
    const productionTime = String(req.body?.production_time || req.body?.productionTime || CLICK2MAIL_DEFAULT_PRODUCTION_TIME).trim();
    const envelope = String(req.body?.envelope || CLICK2MAIL_DEFAULT_ENVELOPE).trim();
    const color = String(req.body?.color || CLICK2MAIL_DEFAULT_COLOR).trim();
    const paperType = String(req.body?.paper_type || req.body?.paperType || CLICK2MAIL_DEFAULT_PAPER_TYPE).trim();
    const printOption = String(req.body?.print_option || req.body?.printOption || CLICK2MAIL_DEFAULT_PRINT_OPTION).trim();
    const mailClass = String(req.body?.mail_class || req.body?.mailClass || CLICK2MAIL_DEFAULT_MAIL_CLASS).trim();
    const trackingType = String(req.body?.tracking_type || req.body?.trackingType || CLICK2MAIL_DEFAULT_TRACKING_TYPE).trim();

    let dedupeKey = String(req.body?.dedupe_key || req.body?.dedupeKey || '').trim();
    if (dedupeKey) {
      if (!/^[a-f0-9]{64}$/i.test(dedupeKey)) {
        return res.status(400).json({ success: false, message: 'dedupe_key must be a 64-char hex sha256' });
      }
      dedupeKey = dedupeKey.toLowerCase();
    } else {
      const addrKey = `${toAddress1}|${toCity}|${toState}|${toPostalCode}|${toCountry}`.toLowerCase();
      dedupeKey = sha256(
        `send-physical-mail|agent:${agentId}|call:${callDomain || ''}|${callId || ''}|template:${templateId}|to:${addrKey}`
      );
    }

    // Idempotency + audit row
    try {
      const [ins] = await pool.execute(
        `INSERT INTO ai_mail_sends (
          user_id, agent_id, template_id, dedupe_key, call_id, call_domain,
          to_name, to_organization, to_address1, to_address2, to_address3,
          to_city, to_state, to_postal_code, to_country,
          provider, product, mail_class, tracking_type,
          status, attempt_count
        ) VALUES (
          ?,?,?,?,?,?,
          ?,?,?,?,?,
          ?,?,?,?,
          'click2mail', ?, ?, ?,
          'pending', 1
        )`,
        [
          userId, agentId, templateId, dedupeKey, callId, callDomain,
          toName, toOrg, toAddress1, toAddress2, toAddress3,
          toCity, toState, toPostalCode, toCountry,
          documentClass || null, mailClass || null, trackingType || null
        ]
      );
      sendRowId = ins.insertId;
    } catch (e) {
      if (e?.code !== 'ER_DUP_ENTRY') throw e;
      const [erows] = await pool.execute(
        'SELECT id, status, billing_history_id, batch_id FROM ai_mail_sends WHERE dedupe_key = ? LIMIT 1',
        [dedupeKey]
      );
      const existing = erows && erows[0];
      if (existing && (existing.status === 'completed' || existing.status === 'submitted')) {
        return res.json({ success: true, already_sent: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'pending') {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'failed') {
        sendRowId = existing.id;
        try {
          await pool.execute(
            'UPDATE ai_mail_sends SET status = \'pending\', attempt_count = attempt_count + 1, error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [sendRowId]
          );
        } catch {}
      } else {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
    }

    // Ensure return address is configured
    const returnAddrRow = await loadUserMailSettings(userId);
    if (!returnAddrRow) {
      const msg = 'Mail settings not configured (set a return address in AI Agent → Tools → Mail)';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(400).json({ success: false, message: msg });
    }

    // Load template
    const [trows] = await pool.execute(
      'SELECT id, name, doc_blob FROM ai_doc_templates WHERE id = ? AND user_id = ? LIMIT 1',
      [templateId, userId]
    );
    const tpl = trows && trows[0];
    if (!tpl) {
      const msg = 'Template not found';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(404).json({ success: false, message: msg });
    }

    // We render the template after address correction so the PDF contains the corrected name/address.
    let pdfOut = null;
    let pageCount = 0;

    // Address correction (recipient)
    let corrected = {
      name: toName || '',
      organization: toOrg || '',
      address1: toAddress1,
      address2: toAddress2 || '',
      address3: toAddress3 || '',
      city: toCity,
      state: toState,
      postalCode: toPostalCode,
      country: toCountry
    };

    let acExtracted = null;
    try {
      const acResp = await click2mailAddressCorrectionOne({
        name: corrected.name,
        organization: corrected.organization,
        address1: corrected.address1,
        address2: corrected.address2,
        address3: corrected.address3,
        city: corrected.city,
        state: corrected.state,
        postalCode: corrected.postalCode,
        country: corrected.country
      });
      acExtracted = extractClick2MailAddressCorrectionFromXml(acResp.parsed);

      if (acExtracted && acExtracted.address) {
        const a = acExtracted.address;
        corrected = {
          name: a.name || corrected.name,
          organization: a.organization || corrected.organization,
          address1: a.address1 || corrected.address1,
          address2: a.address2 || corrected.address2,
          address3: a.address3 || corrected.address3,
          city: a.city || corrected.city,
          state: a.state || corrected.state,
          postalCode: a.postalCode || corrected.postalCode,
          country: normalizeClick2MailCountry(a.country || corrected.country)
        };
      }

      if (acExtracted && acExtracted.nonmailable === true) {
        const msg = 'Recipient address is not mailable';
        try {
          await pool.execute(
            'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [msg, sendRowId]
          );
        } catch {}
        return res.status(400).json({ success: false, message: msg });
      }
    } catch (e) {
      const msg = e?.message || 'Address correction failed';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(502).json({ success: false, message: msg });
    }

    // Persist corrected address
    try {
      await pool.execute(
        `UPDATE ai_mail_sends
         SET corrected_to_name = ?, corrected_to_organization = ?,
             corrected_to_address1 = ?, corrected_to_address2 = ?, corrected_to_address3 = ?,
             corrected_to_city = ?, corrected_to_state = ?, corrected_to_postal_code = ?, corrected_to_country = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ? LIMIT 1`,
        [
          corrected.name || null,
          corrected.organization || null,
          corrected.address1 || null,
          corrected.address2 || null,
          corrected.address3 || null,
          corrected.city || null,
          corrected.state || null,
          corrected.postalCode || null,
          corrected.country || null,
          sendRowId
        ]
      );
    } catch {}

    // Render docx + convert to PDF (required for Click2Mail)
    let docxOut;
    try {
      const baseVars = normalizeTemplateVariables(variables);

      const addr1 = String(corrected.address1 || '').trim();
      const addr2 = String(corrected.address2 || '').trim();
      const addr3 = String(corrected.address3 || '').trim();
      const city = String(corrected.city || '').trim();
      const state = String(corrected.state || '').trim();
      const postal = String(corrected.postalCode || '').trim();
      const country = String(corrected.country || '').trim();

      const addrLines = [];
      if (addr1) addrLines.push(addr1);
      if (addr2) addrLines.push(addr2);
      if (addr3) addrLines.push(addr3);
      const cityLine = [city, state, postal].filter(Boolean).join(' ');
      if (cityLine) addrLines.push(cityLine);
      const addrMulti = addrLines.join('\n');

      const existingName = String(baseVars.Name || baseVars.CallerName || '').trim();
      const existingOrg = String(baseVars.Organization || '').trim();
      const resolvedName = String(corrected.name || toName || existingName).trim();
      const resolvedOrg = String(corrected.organization || toOrg || existingOrg).trim();

      // Auto-inject recipient name/address variables so templates can reliably include them.
      // Only override Name/Organization when we have a non-empty value.
      const autoVars = {
        Address1: addr1,
        Address2: addr2,
        Address3: addr3,
        City: city,
        State: state,
        PostalCode: postal,
        Zip: postal,
        Country: country,
        Address: addrMulti,
        FullAddress: addrMulti,
        CallerAddress: addrMulti,
      };

      if (resolvedName) {
        autoVars.Name = resolvedName;
        autoVars.CallerName = resolvedName;
      }
      if (resolvedOrg) {
        autoVars.Organization = resolvedOrg;
      }

      const mergedVars = { ...baseVars, ...autoVars };
      docxOut = renderDocxTemplate({ templateBuffer: tpl.doc_blob, variables: mergedVars });
    } catch (e) {
      const msg = e?.message || 'Template render failed';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(400).json({ success: false, message: msg });
    }

    try {
      pdfOut = await convertDocxToPdfBuffer(docxOut, { baseName: tpl.name || 'document' });
    } catch {
      pdfOut = null;
    }

    if (!pdfOut) {
      const msg = 'PDF conversion failed (LibreOffice is required)';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(500).json({ success: false, message: msg });
    }

    pageCount = Math.max(1, estimatePdfPageCount(pdfOut) || 0);

    // Cost estimate
    let costAmount = null;
    let markupAmount = null;
    let totalCost = null;
    try {
      const isInternational = String(corrected.country || 'US').toUpperCase() !== 'US';
      const isNonStandard = (!isInternational && acExtracted && acExtracted.standard === false);

      const qtyStandard = (!isInternational && !isNonStandard) ? 1 : 0;
      const qtyNonStandard = isNonStandard ? 1 : 0;
      const qtyInternational = isInternational ? 1 : 0;

      const costParams = {
        documentClass,
        layout,
        productionTime,
        envelope,
        color,
        paperType,
        printOption,
        mailClass,
        quantity: String(qtyStandard),
        nonStandardQuantity: String(qtyNonStandard),
        internationalQuantity: String(qtyInternational),
        numberOfPages: String(pageCount),
        paymentType: 'User Credit'
      };

      const costResp = await click2mailCostEstimate(costParams);
      const extracted = extractClick2MailCostEstimateFromXml(costResp.parsed);
      if (extracted && Number.isFinite(Number(extracted.amount))) {
        costAmount = Number(extracted.amount);
      }

      if (!(costAmount != null && costAmount > 0)) {
        throw new Error('Unable to calculate cost estimate');
      }

      // Platform markup
      const pct = Math.max(0, Number(AI_MAIL_MARKUP_PERCENT || 0));
      const flat = Math.max(0, Number(AI_MAIL_MARKUP_FLAT || 0));
      markupAmount = flat + (pct > 0 ? (costAmount * pct / 100.0) : 0);
      totalCost = costAmount + markupAmount;

      if (!(totalCost != null && Number.isFinite(Number(totalCost)) && totalCost > 0)) {
        throw new Error('Unable to calculate total cost');
      }

      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET cost_estimate = ?, markup_amount = ?, total_cost = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [costAmount, markupAmount || 0, totalCost, sendRowId]
        );
      } catch {}
    } catch (e) {
      const msg = e?.message || 'Cost estimate failed';
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
      return res.status(502).json({ success: false, message: msg });
    }

    // Charge the user (skip if already charged on a retry)
    let billingId = null;
    let amountToCharge = (totalCost != null && Number.isFinite(Number(totalCost))) ? Number(totalCost) : Number(costAmount || 0);
    if (!Number.isFinite(amountToCharge) || amountToCharge <= 0) amountToCharge = 0;

    try {
      const [rows] = await pool.execute('SELECT billing_history_id FROM ai_mail_sends WHERE id = ? LIMIT 1', [sendRowId]);
      billingId = rows && rows[0] && rows[0].billing_history_id ? Number(rows[0].billing_history_id) : null;
    } catch {}

    if (!billingId) {
      const descRaw = `AI physical mail - ${documentClass || 'Mail'} - ${corrected.city || ''} ${corrected.state || ''} ${corrected.postalCode || ''}`.trim();
      const description = descRaw.substring(0, 255);

      const charge = await deductBalance(userId, amountToCharge, description);

      if (!charge || charge.ok !== true) {
        const reason = charge?.reason === 'insufficient_funds' ? 'Insufficient balance' : (charge?.reason || 'Charge failed');
        const msg = `Mail charge failed: ${reason}`;
        try {
          await pool.execute(
            'UPDATE ai_mail_sends SET status = \'failed\', error = ?, billing_history_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [msg, charge?.billingId || null, sendRowId]
          );
        } catch {}
        return res.status(402).json({ success: false, message: msg });
      }

      billingId = charge.billingId;

      try {
        await pool.execute(
          `UPDATE ai_mail_sends
           SET billing_history_id = ?,
               refund_status = 'none',
               refund_amount = NULL,
               refund_billing_history_id = NULL,
               refund_error = NULL,
               refunded_at = NULL,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = ? LIMIT 1`,
          [billingId, sendRowId]
        );
      } catch {}
    }

    // Create + submit Click2Mail batch
    let batchId = null;
    let trackingNumber = null;
    let trackingStatus = null;
    let trackingStatusDate = null;
    let trackingStatusLocation = null;

    const pdfFilename = `mail-${sendRowId}.pdf`;
    const xmlFilename = `mail-${sendRowId}.xml`;

    const payloadLog = {
      product: { documentClass, layout, productionTime, envelope, color, paperType, printOption, mailClass },
      pageCount,
      cost_estimate: costAmount,
      markup_amount: markupAmount,
      total_cost: totalCost,
      billing_history_id: billingId,
      amount_charged: amountToCharge
    };

    try {
      const batch = await click2mailBatchCreate();
      batchId = batch.batchId;
      if (!batchId) throw new Error('Click2Mail batch id missing');

      await click2mailBatchUploadPdf(batchId, pdfOut, { filename: pdfFilename });

      const returnName = String(returnAddrRow.name || '').trim();
      const returnOrg = String(returnAddrRow.organization || '').trim();
      const returnAddress1 = String(returnAddrRow.address1 || '').trim();
      const returnAddress2 = String(returnAddrRow.address2 || '').trim();
      const returnCity = String(returnAddrRow.city || '').trim();
      const returnState = String(returnAddrRow.state || '').trim();
      const returnPostal = String(returnAddrRow.postal_code || '').trim();
      const returnCountry = normalizeClick2MailCountry(returnAddrRow.country || 'US');

      // Click2Mail batch XSD expects <name> as the first element within returnAddress/address.
      const returnNameFinal = (returnName || returnOrg || 'Sender').trim();
      const recipientNameFinal = String(corrected.name || corrected.organization || 'Recipient').trim();
      const recipientOrg = String(corrected.organization || '').trim();

      const batchXml =
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<batch xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">` +
          `<filename>${xmlEscape(pdfFilename)}</filename>` +
          `<appSignature>${xmlEscape(CLICK2MAIL_APP_SIGNATURE)}</appSignature>` +
          `<job>` +
            `<startingPage>1</startingPage>` +
            `<endingPage>${xmlEscape(String(pageCount))}</endingPage>` +
            `<printProductionOptions>` +
              `<documentClass>${xmlEscape(documentClass)}</documentClass>` +
              `<layout>${xmlEscape(layout)}</layout>` +
              `<productionTime>${xmlEscape(productionTime)}</productionTime>` +
              `<envelope>${xmlEscape(envelope)}</envelope>` +
              `<color>${xmlEscape(color)}</color>` +
              `<paperType>${xmlEscape(paperType)}</paperType>` +
              `<printOption>${xmlEscape(printOption)}</printOption>` +
              `<mailClass>${xmlEscape(mailClass)}</mailClass>` +
            `</printProductionOptions>` +
            `<returnAddress>` +
              `<name>${xmlEscape(returnNameFinal)}</name>` +
              `<organization>${xmlEscape(returnOrg)}</organization>` +
              `<address1>${xmlEscape(returnAddress1)}</address1>` +
              (returnAddress2 ? `<address2>${xmlEscape(returnAddress2)}</address2>` : '<address2></address2>') +
              `<city>${xmlEscape(returnCity)}</city>` +
              `<state>${xmlEscape(returnState)}</state>` +
              `<postalCode>${xmlEscape(returnPostal)}</postalCode>` +
              `<country>${xmlEscape(returnCountry)}</country>` +
            `</returnAddress>` +
            `<recipients>` +
              `<address>` +
                `<name>${xmlEscape(recipientNameFinal)}</name>` +
                `<organization>${xmlEscape(recipientOrg)}</organization>` +
                `<address1>${xmlEscape(corrected.address1)}</address1>` +
                (corrected.address2 ? `<address2>${xmlEscape(corrected.address2)}</address2>` : '<address2></address2>') +
                (corrected.address3 ? `<address3>${xmlEscape(corrected.address3)}</address3>` : '<address3></address3>') +
                `<city>${xmlEscape(corrected.city)}</city>` +
                `<state>${xmlEscape(corrected.state)}</state>` +
                `<postalCode>${xmlEscape(corrected.postalCode)}</postalCode>` +
                `<country>${xmlEscape(corrected.country)}</country>` +
                `<c2m_uniqueid/>` +
              `</address>` +
            `</recipients>` +
          `</job>` +
        `</batch>`;

      await click2mailBatchUploadXml(batchId, batchXml, { filename: xmlFilename });

      const submit = await click2mailBatchSubmit(batchId);

      // Best-effort: grab tracking (may not be available immediately)
      let tracking = null;
      try {
        const tr = await click2mailBatchTracking(batchId, trackingType || 'impb');
        tracking = tr?.extracted || null;
        const latest = tracking ? pickLatestTrackingItem(tracking.items) : null;
        if (latest && latest.barCode) {
          trackingNumber = String(latest.barCode).trim() || null;
          trackingStatus = String(latest.status || '').trim() || null;
          trackingStatusLocation = String(latest.statusLocation || '').trim() || null;
          trackingStatusDate = parseClick2MailDateTimeToMysql(latest.dateTime);
        }
      } catch {}

      payloadLog.click2mail = {
        batchCreate: { batchId },
        submitStatus: submit?.status,
        tracking
      };

      try {
        await pool.execute(
          `UPDATE ai_mail_sends
           SET batch_id = ?, status = 'submitted',
               tracking_number = ?, tracking_status = ?, tracking_status_date = ?, tracking_status_location = ?,
               raw_payload = ?, updated_at = CURRENT_TIMESTAMP
           WHERE id = ? LIMIT 1`,
          [
            batchId,
            trackingNumber,
            trackingStatus,
            trackingStatusDate,
            trackingStatusLocation,
            JSON.stringify(payloadLog),
            sendRowId
          ]
        );
      } catch {}

      return res.json({
        success: true,
        dedupe_key: dedupeKey,
        already_sent: false,
        batch_id: batchId,
        tracking_number: trackingNumber,
        cost_estimate: costAmount,
        markup_amount: markupAmount,
        total_cost: totalCost
      });
    } catch (e) {
      // Build a more helpful error message from Click2Mail (often returns XML details).
      const httpStatus = e?.response?.status;
      const respText = (typeof e?.response?.data === 'string') ? String(e.response.data) : '';

      let detail = '';
      try {
        const parsedErr = click2mailParseXml(respText);
        const d = String(pickFirst(parsedErr, [
          'error.description',
          'errors.error.description',
          'description',
          'message',
          'faultstring',
          'faultString'
        ]) || '').trim();
        if (d) detail = d;
      } catch {}

      let msg = detail || (e?.message || 'Click2Mail send failed');

      const responseSnippet = respText ? respText.slice(0, 2000) : '';
      const cleanedSnippet = responseSnippet
        ? responseSnippet
            .replace(/<[^>]+>/g, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .slice(0, 500)
        : '';

      if (!detail && cleanedSnippet) {
        // axios's generic message isn't very helpful; prefer provider response if available.
        if (/request failed with status code/i.test(msg) || msg === 'Click2Mail send failed') {
          msg = cleanedSnippet;
        } else if (!msg.includes(cleanedSnippet)) {
          msg = `${msg}: ${cleanedSnippet}`;
        }
      }

      if (httpStatus && !/\(HTTP\s*\d+\)/i.test(msg) && !/status code\s*\d+/i.test(msg)) {
        msg = `${msg} (HTTP ${httpStatus})`;
      }

      if (responseSnippet) {
        payloadLog.click2mail_error = { status: httpStatus || null, body: responseSnippet };
      } else if (httpStatus) {
        payloadLog.click2mail_error = { status: httpStatus };
      }

      // If we already charged the user, attempt an automatic refund.
      let refundAttempted = false;
      let refundOk = false;
      let refundBillingId = null;
      let refundError = null;

      try {
        if (billingId && amountToCharge && Number.isFinite(Number(amountToCharge)) && Number(amountToCharge) > 0) {
          refundAttempted = true;

          // Best-effort claim to avoid double refunds (uses refund_status state).
          let canRefund = true;
          try {
            const [r] = await pool.execute(
              `UPDATE ai_mail_sends
               SET refund_status = 'pending',
                   refund_amount = ?,
                   refund_error = NULL,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = ?
                 AND (refund_status = 'none' OR refund_status = 'failed')
                 AND billing_history_id IS NOT NULL
               LIMIT 1`,
              [Math.abs(Number(amountToCharge)), sendRowId]
            );
            canRefund = !!(r && r.affectedRows);
          } catch {
            canRefund = true;
          }

          if (canRefund) {
            {
              const refundDescRaw = `Refund: AI mail send failed (mail_id ${sendRowId})`;
              const refundDescription = refundDescRaw.substring(0, 255);

              const refund = await creditBalance(userId, Math.abs(Number(amountToCharge)), refundDescription);

              if (refund && refund.ok === true) {
                refundOk = true;
                refundBillingId = refund.billingId;
              } else {
                refundOk = false;
                refundError = refund?.reason || 'Refund failed';
              }
            }

            // Persist refund state (best-effort)
            try {
              if (refundOk) {
                await pool.execute(
                  `UPDATE ai_mail_sends
                   SET refund_status = 'completed',
                       refund_billing_history_id = ?,
                       refunded_at = NOW(),
                       billing_history_id = NULL,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = ? LIMIT 1`,
                  [refundBillingId, sendRowId]
                );
              } else {
                await pool.execute(
                  `UPDATE ai_mail_sends
                   SET refund_status = 'failed',
                       refund_error = ?,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = ? LIMIT 1`,
                  [String(refundError || 'Refund failed').slice(0, 2000), sendRowId]
                );
              }
            } catch {}
          } else {
            refundAttempted = false;
          }
        }
      } catch (refundErr) {
        refundAttempted = true;
        refundOk = false;
        refundError = String(refundErr?.message || refundErr);
        try {
          await pool.execute(
            `UPDATE ai_mail_sends
             SET refund_status = 'failed',
                 refund_error = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = ? LIMIT 1`,
            [String(refundError || 'Refund failed').slice(0, 2000), sendRowId]
          );
        } catch {}
      }

      if (refundAttempted) {
        payloadLog.refund = {
          attempted: true,
          ok: refundOk,
          amount: Math.abs(Number(amountToCharge || 0)) || 0,
          refund_billing_history_id: refundBillingId || null,
          error: refundOk ? null : (refundError || null)
        };
      }

      const msgForRow = msg;

      if (DEBUG) console.warn('[ai.agent.send-physical-mail] error:', msgForRow);

      try {
        await pool.execute(
          `UPDATE ai_mail_sends
           SET status = 'failed',
               error = ?,
               batch_id = COALESCE(batch_id, ?),
               raw_payload = ?,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = ? LIMIT 1`,
          [String(msgForRow).slice(0, 2000), batchId, JSON.stringify(payloadLog), sendRowId]
        );
      } catch {}

      return res.status(502).json({
        success: false,
        message: msgForRow,
        charged: !!billingId,
        charge_billing_history_id: billingId || null,
        refunded: refundOk,
        refund_billing_history_id: refundBillingId || null
      });
    }
  } catch (e) {
    const msg = e?.message || 'Failed to send physical mail';
    if (DEBUG) console.warn('[ai.agent.send-physical-mail] error:', msg);
    if (sendRowId && pool) {
      try {
        await pool.execute(
          'UPDATE ai_mail_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
    }
    return res.status(500).json({ success: false, message: msg });
  }
});

function extractDailyRoomUrlFromPipecatStartResponse(obj, depth = 0) {
  if (!obj || typeof obj !== 'object' || depth > 3) return '';

  // Common variants
  const direct = obj.dailyRoomUrl || obj.daily_room_url || obj.room_url || obj.roomUrl || obj.dailyRoom || obj.daily_room;
  if (typeof direct === 'string' && direct.trim()) return String(direct).trim();

  // Nested objects
  if (direct && typeof direct === 'object') {
    const nested = direct.url || direct.room_url || direct.roomUrl || direct.dailyRoomUrl || direct.daily_room_url;
    if (typeof nested === 'string' && nested.trim()) return String(nested).trim();
  }

  // Sometimes wrapped
  if (obj.data && typeof obj.data === 'object') {
    const fromData = extractDailyRoomUrlFromPipecatStartResponse(obj.data, depth + 1);
    if (fromData) return fromData;
  }

  return '';
}

function extractDailyRoomTokenFromPipecatStartResponse(obj, depth = 0) {
  if (!obj || typeof obj !== 'object' || depth > 3) return '';

  // Common variants
  const direct = obj.token || obj.room_token || obj.roomToken || obj.dailyToken || obj.daily_token || obj.dailyRoomToken || obj.daily_room_token;
  if (typeof direct === 'string' && direct.trim()) return String(direct).trim();

  // Nested objects
  if (direct && typeof direct === 'object') {
    const nested = direct.token || direct.value || direct.jwt || direct.room_token || direct.roomToken;
    if (typeof nested === 'string' && nested.trim()) return String(nested).trim();
  }

  // Sometimes wrapped
  if (obj.data && typeof obj.data === 'object') {
    const fromData = extractDailyRoomTokenFromPipecatStartResponse(obj.data, depth + 1);
    if (fromData) return fromData;
  }

  return '';
}

function extractDailyRoomNameFromRoomUrl(roomUrl) {
  const raw = String(roomUrl || '').trim();
  if (!raw) return '';
  try {
    const u = new URL(raw);
    // Daily room URLs are typically https://<domain>.daily.co/<roomName>
    const p = String(u.pathname || '').replace(/^\/+/, '').replace(/\/+$/, '');
    if (!p) return '';
    const first = p.split('/')[0];
    return first ? decodeURIComponent(first) : '';
  } catch {
    return '';
  }
}

// Agent-auth endpoint called by the Pipecat agent runtime.
// Starts a new Daily room + Pipecat session (video meeting) and emails the room link.
app.post('/api/ai/agent/send-video-meeting-link', async (req, res) => {
  let sendRowId = null;
  let userId = null;
  const meetingCost = AI_VIDEO_MEETING_LINK_COST;
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const token = parseBearerToken(req.headers.authorization);
    if (!token) return res.status(401).json({ success: false, message: 'Missing Authorization bearer token' });

    const tokenHash = sha256(token);
    const [arows] = await pool.execute(
      'SELECT id, user_id, pipecat_agent_name FROM ai_agents WHERE agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(401).json({ success: false, message: 'Invalid token' });

    userId = Number(agent.user_id);
    const agentId = Number(agent.id);
    const agentName = String(agent.pipecat_agent_name || '').trim();
    if (!agentName) return res.status(500).json({ success: false, message: 'Agent not configured' });

    const toEmail = String(req.body?.to_email || req.body?.toEmail || '').trim();
    if (!toEmail || !isValidEmail(toEmail)) {
      return res.status(400).json({ success: false, message: 'Valid to_email is required' });
    }

    const videoProvider = String(process.env.VIDEO_AVATAR_PROVIDER || 'heygen').trim().toLowerCase() || 'heygen';
    if (videoProvider === 'akool') {
      if (!process.env.AKOOL_API_KEY) {
        return res.status(500).json({ success: false, message: 'AKOOL_API_KEY not configured' });
      }
      if (!process.env.AKOOL_AVATAR_ID) {
        return res.status(500).json({ success: false, message: 'AKOOL_AVATAR_ID not configured' });
      }
    } else if (videoProvider === 'heygen') {
      if (!process.env.HEYGEN_API_KEY) {
        return res.status(500).json({ success: false, message: 'HEYGEN_API_KEY not configured' });
      }
    } else {
      return res.status(500).json({
        success: false,
        message: `VIDEO_AVATAR_PROVIDER must be one of: heygen, akool (got: ${videoProvider || 'empty'})`
      });
    }

    const callId = String(req.body?.call_id || req.body?.callId || '').trim() || null;
    const callDomain = String(req.body?.call_domain || req.body?.callDomain || '').trim() || null;

    const subjectRaw = String(req.body?.subject || '').trim();
    const subject = (subjectRaw ? subjectRaw.slice(0, 255) : 'Your video meeting link');

    let dedupeKey = String(req.body?.dedupe_key || req.body?.dedupeKey || '').trim();
    if (dedupeKey) {
      if (!/^[a-f0-9]{64}$/i.test(dedupeKey)) {
        return res.status(400).json({ success: false, message: 'dedupe_key must be a 64-char hex sha256' });
      }
      dedupeKey = dedupeKey.toLowerCase();
    } else {
      dedupeKey = sha256(
        `send-video-meeting-link|agent:${agentId}|call:${callDomain || ''}|${callId || ''}|to:${toEmail.toLowerCase()}`
      );
    }

    // Idempotency + audit row (reuses ai_email_sends with template_id NULL)
    try {
      const [ins] = await pool.execute(
        `INSERT INTO ai_email_sends (user_id, agent_id, template_id, dedupe_key, call_id, call_domain, to_email, subject, status, attempt_count)
         VALUES (?,?,?,?,?,?,?,?, 'pending', 1)`,
        [userId, agentId, null, dedupeKey, callId, callDomain, toEmail, subject]
      );
      sendRowId = ins.insertId;
    } catch (e) {
      if (e?.code !== 'ER_DUP_ENTRY') throw e;
      const [erows] = await pool.execute(
        'SELECT id, status FROM ai_email_sends WHERE dedupe_key = ? LIMIT 1',
        [dedupeKey]
      );
      const existing = erows && erows[0];
      if (existing && existing.status === 'completed') {
        return res.json({ success: true, already_sent: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'pending') {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'failed') {
        sendRowId = existing.id;
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'pending\', attempt_count = attempt_count + 1, error = NULL, to_email = ?, subject = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [toEmail, subject, sendRowId]
          );
        } catch {}
      } else {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
    }

    if (meetingCost > 0) {
      const chargeDesc = `AI video meeting link - ${toEmail}`.substring(0, 255);
      const charge = await chargeAiEmailSendsRow({
        sendRowId,
        userId,
        amount: meetingCost,
        description: chargeDesc,
        label: 'ai.meeting'
      });

      if (!charge || charge.ok !== true) {
        const reason = charge?.reason || 'charge_failed';
        const short = (reason === 'insufficient_funds') ? 'Insufficient balance' : reason;
        const errMsg = `Meeting link charge failed: ${short}`;

        if (sendRowId) {
          try {
            await pool.execute(
              'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
              [errMsg, sendRowId]
            );
          } catch {}
        }

        return res.status(reason === 'insufficient_funds' ? 402 : 500).json({ success: false, message: errMsg });
      }
    }

    // Start a new Pipecat session (web video meeting). Pipecat creates a Daily room.
    const startBody = {
      createDailyRoom: true,
      body: {
        mode: 'video_meeting'
      }
    };

    const startResp = await pipecatApiCall({
      method: 'POST',
      path: `/public/${encodeURIComponent(agentName)}/start`,
      body: startBody
    });

    const roomUrl = extractDailyRoomUrlFromPipecatStartResponse(startResp);
    if (!roomUrl) {
      throw new Error('Pipecat start response did not include a Daily room URL');
    }

    // Rooms created by Pipecat/Daily are typically private; Daily Prebuilt requires a meeting
    // token to join private rooms. Provide a join URL with ?t=<token> when available.
    const roomToken = extractDailyRoomTokenFromPipecatStartResponse(startResp);

    let joinUrl = roomUrl;
    if (roomToken) {
      try {
        const u = new URL(roomUrl);
        u.searchParams.set('t', roomToken);
        joinUrl = u.toString();
      } catch {}
    }

    // Store meeting link metadata for the dashboard UI
    const roomName = extractDailyRoomNameFromRoomUrl(roomUrl);
    if (sendRowId) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET meeting_provider = ?, meeting_room_name = ?, meeting_room_url = ?, meeting_join_url = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          ['daily', roomName || null, roomUrl, joinUrl, sendRowId]
        );
      } catch {}
    }

    const text = `Here is your video meeting link:\n\n${joinUrl}\n\nOpen it in your browser to join.`;
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
        <p>Here is your video meeting link:</p>
        <p><a href="${joinUrl}">${joinUrl}</a></p>
        <p>Open it in your browser to join.</p>
      </div>`;

    // Persist preview content (best-effort)
    if (sendRowId) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET email_text = ?, email_html = ?, attachments_json = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [text || null, html || null, sendRowId]
        );
      } catch {}
    }

    const info = await sendEmailViaUserSmtp({
      userId,
      toEmail,
      subject,
      text,
      html
    });

    const messageId = info && info.messageId ? String(info.messageId) : null;

    if (sendRowId) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET status = \'completed\', smtp_message_id = COALESCE(?, smtp_message_id), error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [messageId, sendRowId]
        );
      } catch {}
    }

    return res.json({
      success: true,
      dedupe_key: dedupeKey,
      already_sent: false,
      room_url: roomUrl
    });
  } catch (e) {
    const msg = e?.message || 'Failed to send video meeting link';
    if (DEBUG) console.warn('[ai.agent.send-video-meeting-link] error:', e?.data || msg);

    // Best-effort refund if we already charged.
    if (sendRowId && pool && userId && meetingCost > 0) {
      try {
        await refundAiEmailSendsRow({
          sendRowId,
          userId,
          amount: meetingCost,
          description: `Refund: AI video meeting link failed (email_id ${sendRowId})`.substring(0, 255),
          label: 'ai.meeting'
        });
      } catch {}
    }

    if (sendRowId && pool) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
    }
    return res.status(502).json({ success: false, message: msg });
  }
});

// Agent-auth endpoint called by the Pipecat agent runtime.
// Logs conversation turns (caller/user + assistant) tied to a Daily dial-in session.
app.post('/api/ai/agent/log-message', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const token = parseBearerToken(req.headers.authorization);
    if (!token) return res.status(401).json({ success: false, message: 'Missing Authorization bearer token' });

    const tokenHash = sha256(token);
    const [arows] = await pool.execute(
      'SELECT id, user_id FROM ai_agents WHERE agent_action_token_hash = ? LIMIT 1',
      [tokenHash]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(401).json({ success: false, message: 'Invalid token' });

    const userId = Number(agent.user_id);
    const agentId = Number(agent.id);

    const callId = String(req.body?.call_id || req.body?.callId || '').trim();
    const callDomain = String(req.body?.call_domain || req.body?.callDomain || '').trim();

    const messageIdRaw = String(req.body?.message_id || req.body?.messageId || '').trim();
    const messageId = normalizeAiMessageId(messageIdRaw);

    const roleRaw = String(req.body?.role || '').trim().toLowerCase();
    const role = (roleRaw === 'assistant') ? 'assistant' : (roleRaw === 'user' ? 'user' : '');

    let content = String(req.body?.content || '').trim();

    if (!callId || !callDomain) {
      return res.status(400).json({ success: false, message: 'call_id and call_domain are required' });
    }
    if (callId.length > 64 || callDomain.length > 64) {
      return res.status(400).json({ success: false, message: 'call_id/call_domain are too long' });
    }
    if (!messageId) {
      return res.status(400).json({ success: false, message: 'message_id is required' });
    }
    if (!role) {
      return res.status(400).json({ success: false, message: 'role must be user or assistant' });
    }
    if (!content) {
      return res.json({ success: true, ignored: true });
    }

    // Hard cap message size to protect DB.
    if (content.length > 8000) content = content.slice(0, 8000);

    let inserted = false;
    try {
      const [ins] = await pool.execute(
        'INSERT IGNORE INTO ai_call_messages (message_id, user_id, agent_id, call_id, call_domain, role, content) VALUES (?,?,?,?,?,?,?)',
        [messageId, userId, agentId, callId, callDomain, role, content]
      );
      inserted = !!(ins && ins.affectedRows > 0);
    } catch (e) {
      if (DEBUG) console.warn('[ai.agent.log-message] insert failed:', e?.message || e);
      return res.status(500).json({ success: false, message: 'Failed to log message' });
    }

    return res.json({ success: true, inserted });
  } catch (e) {
    if (DEBUG) console.warn('[ai.agent.log-message] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to log message' });
  }
});

// ========== Dialer Campaigns ==========
function normalizeDialerPhone(raw) {
  const digits = digitsOnly(raw);
  if (!digits) return '';
  let normalized = digits;
  if (normalized.length === 10) {
    normalized = `1${normalized}`;
  }
  if (normalized.length < 8 || normalized.length > 15) return '';
  return `+${normalized}`;
}

async function ensureAiAgentForUser({ userId, agentId }) {
  if (!pool) throw new Error('Database not configured');
  if (!agentId) throw new Error('AI agent id is required');
  const [rows] = await pool.execute(
    'SELECT id, display_name FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1',
    [agentId, userId]
  );
  const row = rows && rows[0] ? rows[0] : null;
  if (!row) throw new Error('AI agent not found');
  return { id: String(row.id), name: String(row.display_name || `Agent ${row.id}`) };
}

async function fetchDialerCampaignById({ userId, campaignId }) {
  if (!pool) throw new Error('Database not configured');
  const [rows] = await pool.execute(
    'SELECT * FROM dialer_campaigns WHERE id = ? AND user_id = ? AND status <> \'deleted\' LIMIT 1',
    [campaignId, userId]
  );
  return rows && rows[0] ? rows[0] : null;
}

function buildDialerStatsSeed() {
  return {
    totalLeads: 0,
    pending: 0,
    inProgress: 0,
    completed: 0,
    failed: 0,
    answered: 0,
    voicemail: 0,
    transferred: 0
  };
}

async function hydrateDialerCampaignRows(userId, rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!pool) return safeRows;
  const ids = safeRows.map(r => r.id).filter(Boolean);
  if (!ids.length) return safeRows.map(r => ({ ...r, stats: buildDialerStatsSeed() }));
  const idsParam = ids.map((id) => Number(id));
  const leadStatsMap = new Map();
  try {
    const [leadStats] = await pool.query(
      `SELECT campaign_id,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
        SUM(CASE WHEN status IN ('queued','dialing') THEN 1 ELSE 0 END) AS in_progress_count,
        SUM(CASE WHEN status IN ('completed','answered','voicemail','transferred') THEN 1 ELSE 0 END) AS completed_count,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
        COUNT(*) AS total_count
       FROM dialer_leads
       WHERE user_id = ? AND campaign_id IN (?)
       GROUP BY campaign_id`,
      [userId, idsParam]
    );
    for (const s of leadStats || []) {
      leadStatsMap.set(Number(s.campaign_id), {
        total: Number(s.total_count || 0),
        pending: Number(s.pending_count || 0),
        inProgress: Number(s.in_progress_count || 0),
        completed: Number(s.completed_count || 0),
        failed: Number(s.failed_count || 0)
      });
    }
  } catch (e) {
    if (DEBUG) console.warn('[dialer.stats.leads] failed', e?.message || e);
  }

  const callStatsMap = new Map();
  try {
    const [callStats] = await pool.query(
      `SELECT campaign_id,
        SUM(CASE WHEN result = 'answered' THEN 1 ELSE 0 END) AS answered_count,
        SUM(CASE WHEN result = 'voicemail' THEN 1 ELSE 0 END) AS voicemail_count,
        SUM(CASE WHEN result = 'transferred' THEN 1 ELSE 0 END) AS transferred_count
       FROM dialer_call_logs
       WHERE user_id = ? AND campaign_id IN (?)
       GROUP BY campaign_id`,
      [userId, idsParam]
    );
    for (const s of callStats || []) {
      callStatsMap.set(Number(s.campaign_id), {
        answered: Number(s.answered_count || 0),
        voicemail: Number(s.voicemail_count || 0),
        transferred: Number(s.transferred_count || 0)
      });
    }
  } catch (e) {
    if (DEBUG) console.warn('[dialer.stats.calls] failed', e?.message || e);
  }

  return safeRows.map(row => {
    const leadStats = leadStatsMap.get(Number(row.id)) || {};
    const callStats = callStatsMap.get(Number(row.id)) || {};
    return {
      ...row,
      stats: {
        totalLeads: Number(leadStats.total || 0),
        pending: Number(leadStats.pending || 0),
        inProgress: Number(leadStats.inProgress || 0),
        completed: Number(leadStats.completed || 0),
        failed: Number(leadStats.failed || 0),
        answered: Number(callStats.answered || 0),
        voicemail: Number(callStats.voicemail || 0),
        transferred: Number(callStats.transferred || 0)
      }
    };
  });
}

function serializeDialerCampaign(row) {
  if (!row) return null;
  const stats = row.stats || buildDialerStatsSeed();
  const hasCampaignAudio = Boolean(row.campaign_audio_blob && row.campaign_audio_size);
  return {
    id: Number(row.id),
    name: row.name,
    status: row.status,
    ai_agent_id: row.ai_agent_id ? Number(row.ai_agent_id) : null,
    ai_agent_name: row.ai_agent_name || '',
    concurrency_limit: Number(row.concurrency_limit || 1),
    has_campaign_audio: hasCampaignAudio,
    campaign_audio_filename: hasCampaignAudio ? (row.campaign_audio_filename || null) : null,
    campaign_audio_size: hasCampaignAudio ? Number(row.campaign_audio_size || 0) : null,
    last_started_at: row.last_started_at ? new Date(row.last_started_at).toISOString() : null,
    last_paused_at: row.last_paused_at ? new Date(row.last_paused_at).toISOString() : null,
    created_at: row.created_at ? new Date(row.created_at).toISOString() : null,
    updated_at: row.updated_at ? new Date(row.updated_at).toISOString() : null,
    stats
  };
}

app.get('/api/me/dialer/campaigns', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const pageSize = Math.max(1, Math.min(100, parseInt(req.query.pageSize || '25', 10)));

    const [[countRow]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM dialer_campaigns WHERE user_id = ? AND status <> \'deleted\'',
      [userId]
    );
    const total = countRow && countRow.cnt != null ? Number(countRow.cnt) : 0;
    const offset = page * pageSize;

    const [rows] = await pool.query(
      `SELECT * FROM dialer_campaigns
       WHERE user_id = ? AND status <> 'deleted'
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [userId, pageSize, offset]
    );

    const hydrated = await hydrateDialerCampaignRows(userId, rows || []);
    const data = hydrated.map(serializeDialerCampaign);

    return res.json({ success: true, data, page, pageSize, total });
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaigns.list] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load campaigns' });
  }
});

app.post('/api/me/dialer/campaigns', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const name = String(req.body?.name || '').trim().slice(0, 191);
    const aiAgentId = Number(req.body?.ai_agent_id || req.body?.aiAgentId || 0) || null;
    const concurrencyRaw = parseInt(req.body?.concurrency_limit ?? req.body?.concurrencyLimit ?? 1, 10);
    
    if (!name) return res.status(400).json({ success: false, message: 'Campaign name is required' });
    // AI agent is optional now - if not provided, campaign must use audio-only mode

    const concurrency = Math.min(
      DIALER_MAX_CONCURRENCY,
      Math.max(DIALER_MIN_CONCURRENCY, Number.isFinite(concurrencyRaw) ? concurrencyRaw : DIALER_MIN_CONCURRENCY)
    );

    let agent = null;
    if (aiAgentId) {
      agent = await ensureAiAgentForUser({ userId, agentId: aiAgentId });
    }
    const [result] = await pool.execute(
      `INSERT INTO dialer_campaigns
        (user_id, name, ai_agent_id, ai_agent_name, concurrency_limit)
       VALUES (?, ?, ?, ?, ?)`,
      [userId, name, agent?.id || null, agent?.name || null, concurrency]
    );

    const campaignId = result && result.insertId ? Number(result.insertId) : null;
    const row = await fetchDialerCampaignById({ userId, campaignId });
    const hydrated = await hydrateDialerCampaignRows(userId, row ? [row] : []);
    const payload = hydrated.length ? serializeDialerCampaign(hydrated[0]) : null;
    return res.json({ success: true, data: payload });
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaigns.create] error:', e?.message || e);
    return res.status(500).json({ success: false, message: e?.message || 'Failed to create campaign' });
  }
});

app.post('/api/me/dialer/campaigns/:campaignId/leads-upload', requireAuth, dialerLeadUpload.single('file'), async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });

    const file = req.file;
    if (!file || !file.buffer) {
      return res.status(400).json({ success: false, message: 'Missing CSV file (field name: file)' });
    }
    const csvText = file.buffer.toString('utf8');
    const parsed = Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true,
      transformHeader: (hdr) => String(hdr || '').trim().toLowerCase()
    });
    if (parsed.errors && parsed.errors.length) {
      const firstErr = parsed.errors[0];
      return res.status(400).json({ success: false, message: `CSV parse error: ${firstErr.message}` });
    }
    const rows = Array.isArray(parsed.data) ? parsed.data : [];
    if (!rows.length) {
      return res.status(400).json({ success: false, message: 'CSV file is empty or missing headers' });
    }
    if (rows.length > DIALER_MAX_LEADS_PER_UPLOAD) {
      return res.status(400).json({ success: false, message: `Upload up to ${DIALER_MAX_LEADS_PER_UPLOAD} leads at a time.` });
    }

    const prepared = [];
    let invalid = 0;
    for (const row of rows) {
      const phoneRaw = row.phone || row.number || row.dial || row.destination || row.to;
      const normalized = normalizeDialerPhone(phoneRaw);
      if (!normalized) {
        invalid += 1;
        continue;
      }
      const name = String(row.name || row.full_name || row.contact || '').trim().slice(0, 191) || null;
      let metadata = null;
      if (row.metadata !== undefined && row.metadata !== null && row.metadata !== '') {
        if (typeof row.metadata === 'object') {
          try {
            metadata = JSON.stringify(row.metadata);
          } catch {
            metadata = JSON.stringify({ note: '[object]' });
          }
        } else if (typeof row.metadata === 'string') {
          try {
            metadata = JSON.stringify(JSON.parse(row.metadata));
          } catch {
            metadata = JSON.stringify({ note: row.metadata });
          }
        } else {
          metadata = JSON.stringify({ note: String(row.metadata) });
        }
      }
      prepared.push([campaignId, userId, normalized, name, metadata]);
    }

    if (!prepared.length) {
      return res.status(400).json({ success: false, message: 'No valid leads found in file.', invalid });
    }

    const [insertResult] = await pool.query(
      'INSERT IGNORE INTO dialer_leads (campaign_id, user_id, phone_number, lead_name, metadata) VALUES ?',
      [prepared]
    );
    const inserted = insertResult && insertResult.affectedRows ? Number(insertResult.affectedRows) : 0;
    const duplicates = prepared.length - inserted;
    await pool.execute('UPDATE dialer_campaigns SET updated_at = CURRENT_TIMESTAMP WHERE id = ? AND user_id = ?', [campaignId, userId]);

    return res.json({
      success: true,
      data: { inserted, duplicates, invalid }
    });
  } catch (e) {
    if (DEBUG) console.error('[dialer.leads.upload] error:', e?.message || e);
    return res.status(500).json({ success: false, message: e?.message || 'Failed to upload leads' });
  }
});

app.patch('/api/me/dialer/campaigns/:campaignId/status', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    const action = String(req.body?.action || '').trim().toLowerCase();
    if (!['start', 'pause', 'complete'].includes(action)) {
      return res.status(400).json({ success: false, message: 'Invalid action' });
    }
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });

    if (action === 'start') {
      const [leadRows] = await pool.execute(
        `SELECT COUNT(*) AS cnt FROM dialer_leads
         WHERE campaign_id = ? AND user_id = ? AND status IN ('pending','queued','dialing')`,
        [campaignId, userId]
      );
      const available = leadRows && leadRows[0] ? Number(leadRows[0].cnt || 0) : 0;
      if (!available) {
        return res.status(400).json({ success: false, message: 'Upload at least one pending lead before starting.' });
      }
      await pool.execute(
        'UPDATE dialer_campaigns SET status = ?, last_started_at = NOW(), updated_at = NOW() WHERE id = ? AND user_id = ?',
        ['running', campaignId, userId]
      );
    } else if (action === 'pause') {
      await pool.execute(
        'UPDATE dialer_campaigns SET status = ?, last_paused_at = NOW(), updated_at = NOW() WHERE id = ? AND user_id = ?',
        ['paused', campaignId, userId]
      );
    } else if (action === 'complete') {
      await pool.execute(
        'UPDATE dialer_campaigns SET status = ?, updated_at = NOW() WHERE id = ? AND user_id = ?',
        ['completed', campaignId, userId]
      );
    }

    const updated = await fetchDialerCampaignById({ userId, campaignId });
    const hydrated = await hydrateDialerCampaignRows(userId, updated ? [updated] : []);
    const payload = hydrated.length ? serializeDialerCampaign(hydrated[0]) : null;
    return res.json({ success: true, data: payload });
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaigns.status] error:', e?.message || e);
    return res.status(500).json({ success: false, message: e?.message || 'Failed to update campaign status' });
  }
});

app.delete('/api/me/dialer/campaigns/:campaignId', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });
    await pool.execute(
      'UPDATE dialer_campaigns SET status = \'deleted\', updated_at = NOW() WHERE id = ? AND user_id = ?',
      [campaignId, userId]
    );
    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaigns.delete] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to delete campaign' });
  }
});

app.get('/api/me/dialer/campaigns/:campaignId/leads', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });

    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const pageSize = Math.max(1, Math.min(200, parseInt(req.query.pageSize || '50', 10)));
    const offset = page * pageSize;

    const [[countRow]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM dialer_leads WHERE campaign_id = ? AND user_id = ?',
      [campaignId, userId]
    );
    const total = countRow && countRow.cnt != null ? Number(countRow.cnt || 0) : 0;

    const [rows] = await pool.execute(
      `SELECT id, phone_number, lead_name, status, attempt_count, last_call_at, created_at
       FROM dialer_leads
       WHERE campaign_id = ? AND user_id = ?
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [campaignId, userId, pageSize, offset]
    );

    const data = (rows || []).map(r => ({
      id: Number(r.id),
      phone_number: r.phone_number,
      lead_name: r.lead_name || '',
      status: r.status,
      attempt_count: Number(r.attempt_count || 0),
      last_call_at: r.last_call_at ? new Date(r.last_call_at).toISOString() : null,
      created_at: r.created_at ? new Date(r.created_at).toISOString() : null
    }));

    return res.json({ success: true, data, page, pageSize, total });
  } catch (e) {
    if (DEBUG) console.error('[dialer.leads.list] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load leads' });
  }
});

app.get('/api/me/dialer/campaigns/:campaignId/stats', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });
    const hydrated = await hydrateDialerCampaignRows(userId, [campaign]);
    const payload = hydrated.length ? serializeDialerCampaign(hydrated[0]) : null;
    return res.json({ success: true, data: payload ? payload.stats : buildDialerStatsSeed() });
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaigns.stats] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load stats' });
  }
});

// Get leads filtered by status (for viewing Done, Failed, Voicemail, Transferred numbers)
app.get('/api/me/dialer/campaigns/:campaignId/leads-by-status', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });
    
    const status = String(req.query.status || '').trim().toLowerCase();
    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const pageSize = Math.max(1, Math.min(100, parseInt(req.query.pageSize || '50', 10)));
    const offset = page * pageSize;
    
    // Map UI status names to database status values
    // Lead statuses: pending, queued, dialing, completed, failed
    // Call results: answered, voicemail, transferred, no-answer, busy, failed
    let whereStatus = [];
    let joinCallLogs = false;
    
    if (status === 'completed' || status === 'done') {
      whereStatus = ['completed'];
    } else if (status === 'failed') {
      whereStatus = ['failed'];
    } else if (status === 'pending') {
      whereStatus = ['pending', 'queued'];
    } else if (status === 'inprogress' || status === 'live') {
      whereStatus = ['dialing'];
    } else if (status === 'voicemail') {
      // Voicemail comes from call_logs result, not lead status
      joinCallLogs = true;
    } else if (status === 'transferred') {
      // Transferred comes from call_logs result
      joinCallLogs = true;
    } else {
      // Return all if no valid status filter
      whereStatus = [];
    }
    
    let rows = [];
    let total = 0;
    
    if (joinCallLogs && (status === 'voicemail' || status === 'transferred')) {
      // Query by call log result
      const [[countRow]] = await pool.execute(
        `SELECT COUNT(DISTINCT l.id) AS cnt 
         FROM dialer_leads l
         INNER JOIN dialer_call_logs c ON c.lead_id = l.id
         WHERE l.campaign_id = ? AND l.user_id = ? AND c.result = ?`,
        [campaignId, userId, status]
      );
      total = countRow?.cnt ? Number(countRow.cnt) : 0;
      
      [rows] = await pool.query(
        `SELECT DISTINCT l.id, l.phone_number, l.lead_name, l.status, l.attempt_count, l.last_call_at, l.created_at,
                c.result as call_result, c.duration_sec
         FROM dialer_leads l
         INNER JOIN dialer_call_logs c ON c.lead_id = l.id
         WHERE l.campaign_id = ? AND l.user_id = ? AND c.result = ?
         ORDER BY l.last_call_at DESC
         LIMIT ${Number(pageSize)} OFFSET ${Number(offset)}`,
        [campaignId, userId, status]
      );
    } else if (whereStatus.length > 0) {
      // Query by lead status
      const placeholders = whereStatus.map(() => '?').join(',');
      const [[countRow]] = await pool.execute(
        `SELECT COUNT(*) AS cnt FROM dialer_leads WHERE campaign_id = ? AND user_id = ? AND status IN (${placeholders})`,
        [campaignId, userId, ...whereStatus]
      );
      total = countRow?.cnt ? Number(countRow.cnt) : 0;
      
      [rows] = await pool.query(
        `SELECT id, phone_number, lead_name, status, attempt_count, last_call_at, created_at
         FROM dialer_leads
         WHERE campaign_id = ? AND user_id = ? AND status IN (${placeholders})
         ORDER BY last_call_at DESC, created_at DESC
         LIMIT ${Number(pageSize)} OFFSET ${Number(offset)}`,
        [campaignId, userId, ...whereStatus]
      );
    } else {
      // Return all leads
      const [[countRow]] = await pool.execute(
        'SELECT COUNT(*) AS cnt FROM dialer_leads WHERE campaign_id = ? AND user_id = ?',
        [campaignId, userId]
      );
      total = countRow?.cnt ? Number(countRow.cnt) : 0;
      
      [rows] = await pool.query(
        `SELECT id, phone_number, lead_name, status, attempt_count, last_call_at, created_at
         FROM dialer_leads
         WHERE campaign_id = ? AND user_id = ?
         ORDER BY last_call_at DESC, created_at DESC
         LIMIT ${Number(pageSize)} OFFSET ${Number(offset)}`,
        [campaignId, userId]
      );
    }
    
    const data = (rows || []).map(r => ({
      id: Number(r.id),
      phone_number: r.phone_number,
      lead_name: r.lead_name || '',
      status: r.status,
      call_result: r.call_result || null,
      duration_sec: r.duration_sec != null ? Number(r.duration_sec) : null,
      attempt_count: Number(r.attempt_count || 0),
      last_call_at: r.last_call_at ? new Date(r.last_call_at).toISOString() : null,
      created_at: r.created_at ? new Date(r.created_at).toISOString() : null
    }));
    
    return res.json({ success: true, data, page, pageSize, total, status: status || 'all' });
  } catch (e) {
    if (DEBUG) console.error('[dialer.leads.by-status] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load leads' });
  }
});

// Upload campaign audio (WAV)
const campaignAudioUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
  fileFilter: (req, file, cb) => {
    const mime = String(file.mimetype || '').toLowerCase();
    if (mime === 'audio/wav' || mime === 'audio/x-wav' || mime === 'audio/wave') {
      cb(null, true);
    } else {
      cb(new Error('Only WAV audio files are supported'));
    }
  }
});

app.post('/api/me/dialer/campaigns/:campaignId/audio', requireAuth, campaignAudioUpload.single('audio'), async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });
    
    const file = req.file;
    if (!file || !file.buffer) {
      return res.status(400).json({ success: false, message: 'Missing audio file (field name: audio)' });
    }
    
    const filename = String(file.originalname || 'campaign.wav').slice(0, 255);
    const mime = String(file.mimetype || 'audio/wav').slice(0, 128);
    const size = file.buffer.length;
    
    // Simple WAV validation: check for RIFF header
    if (file.buffer.length < 44) {
      return res.status(400).json({ success: false, message: 'Invalid WAV file: file too small' });
    }
    const header = file.buffer.toString('ascii', 0, 4);
    if (header !== 'RIFF') {
      return res.status(400).json({ success: false, message: 'Invalid WAV file: missing RIFF header' });
    }
    
    await pool.execute(
      `UPDATE dialer_campaigns
       SET campaign_audio_blob = ?,
           campaign_audio_filename = ?,
           campaign_audio_mime = ?,
           campaign_audio_size = ?,
           updated_at = NOW()
       WHERE id = ? AND user_id = ?`,
      [file.buffer, filename, mime, size, campaignId, userId]
    );
    
    return res.json({
      success: true,
      data: {
        filename,
        size,
        mime
      }
    });
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaign-audio.upload] error:', e?.message || e);
    return res.status(500).json({ success: false, message: e?.message || 'Failed to upload campaign audio' });
  }
});

// Download campaign audio (user endpoint)
app.get('/api/me/dialer/campaigns/:campaignId/audio', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    
    const [rows] = await pool.execute(
      `SELECT campaign_audio_blob, campaign_audio_filename, campaign_audio_mime
       FROM dialer_campaigns
       WHERE id = ? AND user_id = ? AND status <> 'deleted'
       LIMIT 1`,
      [campaignId, userId]
    );
    const row = rows && rows[0] ? rows[0] : null;
    
    if (!row || !row.campaign_audio_blob) {
      return res.status(404).json({ success: false, message: 'No campaign audio found' });
    }
    
    const filename = row.campaign_audio_filename || 'campaign.wav';
    const mime = row.campaign_audio_mime || 'audio/wav';
    const blob = row.campaign_audio_blob;
    
    res.setHeader('Content-Type', mime);
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', blob.length);
    return res.send(blob);
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaign-audio.download] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to download campaign audio' });
  }
});

// Delete campaign audio
app.delete('/api/me/dialer/campaigns/:campaignId/audio', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    
    const campaign = await fetchDialerCampaignById({ userId, campaignId });
    if (!campaign) return res.status(404).json({ success: false, message: 'Campaign not found' });
    
    await pool.execute(
      `UPDATE dialer_campaigns
       SET campaign_audio_blob = NULL,
           campaign_audio_filename = NULL,
           campaign_audio_mime = NULL,
           campaign_audio_size = NULL,
           updated_at = NOW()
       WHERE id = ? AND user_id = ?`,
      [campaignId, userId]
    );
    
    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[dialer.campaign-audio.delete] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to delete campaign audio' });
  }
});

// Bot endpoint: stream campaign audio (authenticated by PORTAL_AGENT_ACTION_TOKEN if set)
app.get('/api/ai/agent/campaigns/:campaignId/audio', async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    
    // Authenticate using PORTAL_AGENT_ACTION_TOKEN (optional - only enforced if token is configured)
    const authHeader = String(req.headers.authorization || '');
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
    const expectedToken = String(process.env.PORTAL_AGENT_ACTION_TOKEN || '').trim();
    
    if (DEBUG) {
      console.log('[ai.agent.campaigns.audio] Auth check:', {
        hasAuthHeader: !!authHeader,
        tokenLength: token.length,
        expectedTokenLength: expectedToken.length,
        tokenMatch: token === expectedToken,
        tokenRequired: !!expectedToken
      });
    }
    
    // Only enforce token authentication if PORTAL_AGENT_ACTION_TOKEN is configured
    if (expectedToken && token !== expectedToken) {
      if (DEBUG) console.log('[ai.agent.campaigns.audio] Auth failed: token mismatch');
      return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    
    const campaignId = Number(req.params.campaignId || 0);
    if (!campaignId) return res.status(400).json({ success: false, message: 'Invalid campaign id' });
    
    const [rows] = await pool.execute(
      `SELECT campaign_audio_blob, campaign_audio_filename, campaign_audio_mime
       FROM dialer_campaigns
       WHERE id = ? AND status <> 'deleted'
       LIMIT 1`,
      [campaignId]
    );
    const row = rows && rows[0] ? rows[0] : null;
    
    if (!row || !row.campaign_audio_blob) {
      return res.status(404).json({ success: false, message: 'No campaign audio found' });
    }
    
    const mime = row.campaign_audio_mime || 'audio/wav';
    const blob = row.campaign_audio_blob;
    
    res.setHeader('Content-Type', mime);
    res.setHeader('Content-Length', blob.length);
    return res.send(blob);
  } catch (e) {
    if (DEBUG) console.error('[ai.agent.campaigns.audio] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to stream campaign audio' });
  }
});

// Debug endpoint: manually update call status for testing
app.post('/api/me/dialer/debug/update-call-status', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const callId = String(req.body?.call_id || '').trim();
    const status = String(req.body?.status || '').trim();
    const result = String(req.body?.result || '').trim();
    const durationSec = req.body?.duration_sec != null ? Number(req.body.duration_sec) : null;

    if (!callId) return res.status(400).json({ success: false, message: 'call_id is required' });

    // Find the call log by call_id
    const [logRows] = await pool.execute(
      'SELECT id, lead_id, campaign_id, user_id FROM dialer_call_logs WHERE call_id = ? ORDER BY id DESC LIMIT 1',
      [callId]
    );
    const logRow = logRows && logRows[0] ? logRows[0] : null;

    if (!logRow) {
      return res.status(404).json({ success: false, message: 'Call log not found' });
    }

    // Verify ownership
    if (Number(logRow.user_id) !== Number(userId)) {
      return res.status(403).json({ success: false, message: 'Unauthorized' });
    }

    // Update call log
    await pool.execute(
      `UPDATE dialer_call_logs
       SET status = COALESCE(?, status),
           result = COALESCE(?, result),
           duration_sec = COALESCE(?, duration_sec)
       WHERE id = ? LIMIT 1`,
      [status || null, result || null, durationSec, logRow.id]
    );

    // Update lead if result is specified
    if (result && logRow.lead_id) {
      const leadStatus = ['answered', 'voicemail', 'transferred'].includes(result)
        ? result
        : (result === 'failed' ? 'failed' : 'completed');
      await pool.execute(
        'UPDATE dialer_leads SET status = ? WHERE id = ? AND user_id = ? LIMIT 1',
        [leadStatus, logRow.lead_id, userId]
      );
    }

    return res.json({
      success: true,
      message: 'Call status updated',
      data: { callId, logId: logRow.id, leadId: logRow.lead_id }
    });
  } catch (e) {
    if (DEBUG) console.error('[dialer.debug.update-call-status] error:', e?.message || e);
    return res.status(500).json({ success: false, message: e?.message || 'Failed to update call status' });
  }
});

function buildDialerCallIdentifiers({ campaignId, leadId }) {
  const ts = Date.now().toString(36);
  const callId = `d${campaignId}l${leadId}-${ts}`.slice(0, 64);
  const callDomain = `dialer-${campaignId}`.slice(0, 64);
  return { callId, callDomain };
}

function buildDialerDailyRoomProperties() {
  const props = {
    enable_dialout: true
  };
  if (DIALER_DAILY_ROOM_TTL_SECONDS > 0) {
    props.exp = Math.floor(Date.now() / 1000) + DIALER_DAILY_ROOM_TTL_SECONDS;
  }
  return props;
}

async function resolveDialerCallerId({ userId, agentId }) {
  if (!pool) return '';
  try {
    const [rows] = await pool.execute(
      'SELECT daily_number_id FROM ai_numbers WHERE user_id = ? AND agent_id = ? AND cancel_pending = 0 ORDER BY updated_at DESC LIMIT 1',
      [userId, agentId]
    );
    const row = rows && rows[0] ? rows[0] : null;
    if (row && row.daily_number_id) return String(row.daily_number_id);
  } catch {}

  try {
    const [rows] = await pool.execute(
      'SELECT daily_number_id FROM ai_numbers WHERE user_id = ? AND cancel_pending = 0 ORDER BY updated_at DESC LIMIT 1',
      [userId]
    );
    const row = rows && rows[0] ? rows[0] : null;
    if (row && row.daily_number_id) return String(row.daily_number_id);
  } catch {}

  return '';
}

async function startDialerLeadCall({ campaign, lead }) {
  if (!pool || !campaign || !lead) return false;
  const campaignId = Number(campaign.id);
  const userId = Number(campaign.user_id);
  const leadId = Number(lead.id);
  const agentId = Number(campaign.ai_agent_id) || 0;

  // Check if campaign has audio (audio-only mode)
  // Note: Even audio-only campaigns require an AI agent for the Pipecat endpoint
  // The agent won't be used for AI - bot.py will play audio instead
  const hasCampaignAudio = Boolean(campaign.campaign_audio_blob || campaign.campaign_audio_size);
  const audioOnlyMode = hasCampaignAudio;  // Audio-only mode when campaign has audio

  let agent = null;
  if (agentId) {
    try {
      const [rows] = await pool.execute(
        'SELECT id, display_name, pipecat_agent_name, pipecat_region FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1',
        [agentId, userId]
      );
      agent = rows && rows[0] ? rows[0] : null;
    } catch (e) {
      if (DEBUG) console.warn('[dialer.worker] Failed to lookup AI agent:', e?.message || e);
      agent = null;
    }
  }

  // Even for audio-only campaigns, we need a valid Pipecat agent endpoint
  // The agent won't be used for AI, but we need the service to exist
  if (!agent || !agent.pipecat_agent_name) {
    try {
      await pool.execute(
        'UPDATE dialer_leads SET status = ? WHERE id = ? AND user_id = ? LIMIT 1',
        ['failed', leadId, userId]
      );
      await pool.execute(
        `UPDATE dialer_call_logs
         SET status = 'error', result = 'failed', notes = COALESCE(NULLIF(notes,''), ?)
         WHERE lead_id = ? ORDER BY id DESC LIMIT 1`,
        ['Missing Pipecat agent configuration - please select an AI agent (required for Pipecat endpoint)', leadId]
      );
    } catch {}
    return false;
  }

  if (audioOnlyMode && !hasCampaignAudio) {
    try {
      await pool.execute(
        'UPDATE dialer_leads SET status = ? WHERE id = ? AND user_id = ? LIMIT 1',
        ['failed', leadId, userId]
      );
      await pool.execute(
        `UPDATE dialer_call_logs
         SET status = 'error', result = 'failed', notes = COALESCE(NULLIF(notes,''), ?)
         WHERE lead_id = ? ORDER BY id DESC LIMIT 1`,
        ['Audio-only campaign requires audio file', leadId]
      );
    } catch {}
    return false;
  }

  const callerId = await resolveDialerCallerId({ userId, agentId });
  const { callId, callDomain } = buildDialerCallIdentifiers({ campaignId, leadId });
  const dialoutSettings = {
    phoneNumber: String(lead.phone_number || '').trim()
  };
  if (callerId) dialoutSettings.callerId = String(callerId);

  // Build campaign audio URL if in audio-only mode
  let campaignAudioUrl = '';
  if (audioOnlyMode) {
    const portalBase = String(process.env.PUBLIC_BASE_URL || process.env.PORTAL_BASE_URL || '').trim();
    if (portalBase) {
      campaignAudioUrl = `${portalBase}/api/ai/agent/campaigns/${campaignId}/audio`;
    }
  }

  const metadata = {
    campaign_id: campaignId,
    lead_id: leadId,
    phone_number: lead.phone_number,
    call_id: callId,
    call_domain: callDomain,
    dialout_settings: dialoutSettings,
    audio_only_mode: audioOnlyMode,
    campaign_audio_url: campaignAudioUrl || null
  };

  try {
    await pool.execute(
      `INSERT INTO dialer_call_logs
        (campaign_id, lead_id, user_id, ai_agent_id, call_id, status, metadata)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        campaignId,
        leadId,
        userId,
        agentId || null,  // Allow NULL for audio-only campaigns
        callId,
        'queued',
        JSON.stringify(metadata)
      ]
    );
  } catch (e) {
    if (DEBUG) console.warn('[dialer.worker] Failed to insert call log row:', e?.message || e);
  }

  const dailyRoomProperties = buildDialerDailyRoomProperties();

  const startBody = {
    createDailyRoom: true,
    dailyRoomProperties,
    body: {
      dialout_settings: [dialoutSettings],
      call_id: callId,
      call_domain: callDomain,
      mode: 'dialout',
      dialer: {
        campaign_id: campaignId,
        lead_id: leadId
      },
      audio_only_mode: audioOnlyMode,
      campaign_audio_url: campaignAudioUrl || null
    }
  };

  // Use the agent's Pipecat service name
  // For audio-only mode, the agent endpoint exists but bot.py will play audio instead of using AI
  const agentNameForStart = agent.pipecat_agent_name;

  try {
    await pipecatApiCall({
      method: 'POST',
      path: `/public/${encodeURIComponent(agentNameForStart)}/start`,
      body: startBody
    });

    try {
      await pool.execute(
        'UPDATE dialer_leads SET status = ?, last_call_at = NOW() WHERE id = ? AND user_id = ? LIMIT 1',
        ['dialing', leadId, userId]
      );
      await pool.execute(
        'UPDATE dialer_call_logs SET status = ? WHERE lead_id = ? ORDER BY id DESC LIMIT 1',
        ['dialing', leadId]
      );
    } catch {}

    return true;
  } catch (e) {
    const msg = e?.message || 'Failed to start Pipecat dial-out';
    if (DEBUG) console.warn('[dialer.worker] Pipecat dial-out failed:', msg);
    try {
      await pool.execute(
        'UPDATE dialer_leads SET status = ? WHERE id = ? AND user_id = ? LIMIT 1',
        ['failed', leadId, userId]
      );
      await pool.execute(
        `UPDATE dialer_call_logs
         SET status = 'error', result = 'failed', notes = COALESCE(NULLIF(notes,''), ?)
         WHERE lead_id = ? ORDER BY id DESC LIMIT 1`,
        [String(msg).slice(0, 500), leadId]
      );
    } catch {}
    return false;
  }
}

let dialerWorkerRunning = false;
async function runDialerWorkerTick() {
  if (!pool) return;
  if (dialerWorkerRunning) {
    if (DEBUG) console.log('[dialer.worker] tick skipped (already running)');
    return;
  }
  if (!PIPECAT_PUBLIC_API_KEY) {
    if (DEBUG) console.warn('[dialer.worker] PIPECAT_PUBLIC_API_KEY not set; skipping tick');
    return;
  }

  dialerWorkerRunning = true;
  try {
    const [campaigns] = await pool.query(
      `SELECT id, user_id, name, ai_agent_id, ai_agent_name, concurrency_limit,
              campaign_audio_blob, campaign_audio_size
       FROM dialer_campaigns
       WHERE status = 'running'
       ORDER BY id DESC
       LIMIT 200`
    );

    for (const c of campaigns || []) {
      const campaignId = Number(c.id);
      const userId = Number(c.user_id);
      const concurrency = Math.max(1, Number(c.concurrency_limit || 1));

      let inProgress = 0;
      try {
        const [[cntRow]] = await pool.execute(
          `SELECT COUNT(*) AS cnt
           FROM dialer_leads
           WHERE campaign_id = ? AND user_id = ? AND status IN ('queued','dialing')`,
          [campaignId, userId]
        );
        inProgress = cntRow && cntRow.cnt != null ? Number(cntRow.cnt || 0) : 0;
      } catch {}

      const available = Math.max(0, concurrency - inProgress);
      if (!available) continue;

      const limit = Math.min(available, 50);
      const [leads] = await pool.query(
        `SELECT id, phone_number, lead_name, metadata
         FROM dialer_leads
         WHERE campaign_id = ? AND user_id = ? AND status = 'pending'
         ORDER BY id ASC
         LIMIT ${parseInt(limit, 10)}`,
        [campaignId, userId]
      );

      for (const lead of leads || []) {
        const leadId = Number(lead.id);
        if (!leadId) continue;
        const [claim] = await pool.execute(
          `UPDATE dialer_leads
           SET status = 'queued', attempt_count = attempt_count + 1
           WHERE id = ? AND user_id = ? AND status = 'pending'`,
          [leadId, userId]
        );
        if (!claim || !claim.affectedRows) continue;

        await startDialerLeadCall({ campaign: c, lead });
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[dialer.worker] tick error:', e?.message || e);
  } finally {
    dialerWorkerRunning = false;
  }
}

function startDialerWorker() {
  const intervalMs = (DIALER_WORKER_INTERVAL_SECONDS || 0) * 1000;
  if (!intervalMs) {
    if (DEBUG) console.log('[dialer.worker] Disabled (no interval set)');
    return;
  }
  if (DEBUG) console.log('[dialer.worker] Enabled with interval (ms):', intervalMs);
  setInterval(() => {
    runDialerWorkerTick();
  }, intervalMs);
}

// ========== AI Agents ==========
const AI_MAX_AGENTS = Math.max(1, parseInt(process.env.AI_MAX_AGENTS || '5', 10) || 5);
const AI_MAX_NUMBERS = Math.max(1, parseInt(process.env.AI_MAX_NUMBERS || '5', 10) || 5);

// Per-agent background WAV upload (stored in DB, served via tokenized public URL)
const aiAgentBackgroundAudioUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 } // 5MB
});

function getPublicBaseUrlFromReq(req) {
  const envBase = String(process.env.PUBLIC_BASE_URL || process.env.PORTAL_BASE_URL || '').trim();
  const fromReq = `${req.protocol}://${req.get('host')}`;
  return String(envBase || fromReq).replace(/\/$/, '');
}

function looksLikeWav(buf) {
  if (!buf || !Buffer.isBuffer(buf) || buf.length < 12) return false;
  // RIFF....WAVE
  return buf.slice(0, 4).toString('ascii') === 'RIFF' && buf.slice(8, 12).toString('ascii') === 'WAVE';
}

function buildAgentBackgroundAudioPublicUrl({ baseUrl, agentId, token }) {
  const b = String(baseUrl || '').trim().replace(/\/$/, '');
  const t = String(token || '').trim();
  if (!b || !t) return '';
  const rel = `/api/public/ai/agents/${encodeURIComponent(String(agentId))}/background-audio.wav?token=${encodeURIComponent(t)}`;
  return b + rel;
}

async function getAgentBackgroundAudioUploadToken({ userId, agentId }) {
  if (!pool) return '';
  try {
    const [rows] = await pool.execute(
      'SELECT access_token FROM ai_agent_background_audio WHERE user_id = ? AND agent_id = ? LIMIT 1',
      [userId, agentId]
    );
    const row = rows && rows[0] ? rows[0] : null;
    return row && row.access_token ? String(row.access_token) : '';
  } catch {
    return '';
  }
}

async function resolveAgentBackgroundAudioUrl({ userId, agentId, fallbackUrl, publicBaseUrl }) {
  const token = await getAgentBackgroundAudioUploadToken({ userId, agentId });
  const base = String(publicBaseUrl || process.env.PUBLIC_BASE_URL || process.env.PORTAL_BASE_URL || '').trim();
  if (token && base) {
    return buildAgentBackgroundAudioPublicUrl({ baseUrl: base, agentId, token });
  }
  return String(fallbackUrl || '');
}

// Per-user AI transfer destination (used by the agent runtime via OPERATOR_NUMBER)
app.get('/api/me/ai/transfer-settings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const transfer_to_number = await getUserAiTransferDestination(userId);
    return res.json({ success: true, data: { transfer_to_number: transfer_to_number || '' } });
  } catch (e) {
    if (DEBUG) console.warn('[ai.transfer.get] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load transfer settings' });
  }
});

async function syncUserAiTransferDestinationToAgents({ userId, transferToNumber, publicBaseUrl }) {
  if (!pool) return;
  const transferVal = String(transferToNumber || '').trim();

  const [agents] = await pool.execute('SELECT * FROM ai_agents WHERE user_id = ? ORDER BY created_at DESC', [userId]);
  for (const agent of agents || []) {
    // Ensure token exists so we don't accidentally overwrite the secret set with a blank token.
    const portalToken = await getOrCreateAgentActionTokenPlain({
      agentRow: agent,
      userId,
      label: 'ai.transfer.sync'
    });

    const bgUrlResolved = await resolveAgentBackgroundAudioUrl({
      userId,
      agentId: agent.id,
      fallbackUrl: agent.background_audio_url,
      publicBaseUrl
    });

    // Per-agent override (falls back to user's default transfer setting)
    const agentTransfer = normalizeAiTransferDestination(agent.transfer_to_number);
    const operatorNumber = agentTransfer || transferVal;

    const secrets = buildPipecatSecrets({
      greeting: agent.greeting,
      prompt: agent.prompt,
      cartesiaVoiceId: agent.cartesia_voice_id,
      portalAgentActionToken: portalToken,
      operatorNumber,
      backgroundAudioUrl: bgUrlResolved,
      backgroundAudioGain: agent.background_audio_gain
    });

    await pipecatUpsertSecretSet(agent.pipecat_secret_set, secrets, agent.pipecat_region || PIPECAT_REGION);
    await pipecatUpdateAgent({
      agentName: agent.pipecat_agent_name,
      secretSetName: agent.pipecat_secret_set,
      regionOverride: agent.pipecat_region || PIPECAT_REGION
    });
  }
}

app.put('/api/me/ai/transfer-settings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const raw = (req.body && (req.body.transfer_to_number ?? req.body.transferToNumber)) ?? '';
    const normalized = normalizeAiTransferDestination(raw);

    if (normalized && !isValidAiTransferDestination(normalized)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid transfer destination. Use E.164 phone format like +18005551234 or a SIP URI like sip:agent@domain.'
      });
    }

    if (normalized && normalized.length > 64) {
      return res.status(400).json({ success: false, message: 'Transfer destination is too long' });
    }

    await pool.execute(
      'INSERT INTO user_ai_transfer_settings (user_id, transfer_to_number) VALUES (?, ?) ON DUPLICATE KEY UPDATE transfer_to_number = VALUES(transfer_to_number), updated_at = CURRENT_TIMESTAMP',
      [userId, normalized ? String(normalized) : null]
    );

    const publicBaseUrl = getPublicBaseUrlFromReq(req);

    try {
      await syncUserAiTransferDestinationToAgents({ userId, transferToNumber: normalized, publicBaseUrl });
    } catch (syncErr) {
      const msg = syncErr?.message || 'Failed to update AI agents in Pipecat';
      if (DEBUG) console.warn('[ai.transfer.sync] error:', msg);
      return res.status(502).json({ success: false, message: msg });
    }

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.warn('[ai.transfer.put] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to save transfer settings' });
  }
});

// AI SMS DID options and settings (removed)
app.get('/api/me/ai/sms/did-options', requireAuth, (req, res) => {
  return res.status(410).json({ success: false, message: 'SMS integration has been removed' });
});
app.get('/api/me/ai/sms-settings', requireAuth, (req, res) => {
  return res.status(410).json({ success: false, message: 'SMS integration has been removed' });
});
app.put('/api/me/ai/sms-settings', requireAuth, (req, res) => {
  return res.status(410).json({ success: false, message: 'SMS integration has been removed' });
});

// List AI SMS sends for the logged-in user.
app.get('/api/me/ai/sms', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const page = Math.max(0, parseInt(String(req.query.page || '0'), 10) || 0);
    const pageSizeRaw = parseInt(String(req.query.pageSize || '25'), 10) || 25;
    const pageSize = Math.max(1, Math.min(100, pageSizeRaw));
    const offset = page * pageSize;

    const agentIdRaw = String(req.query.agent_id || req.query.agentId || '').trim();
    const agentId = agentIdRaw ? agentIdRaw : '';

    const statusRaw = String(req.query.status || '').trim().toLowerCase();
    let status = '';
    if (statusRaw) {
      if (!['pending', 'completed', 'failed'].includes(statusRaw)) {
        return res.status(400).json({ success: false, message: 'status must be one of: pending, completed, failed' });
      }
      status = statusRaw;
    }

    const where = [`s.user_id = ?`];
    const params = [userId];

    if (agentId) {
      where.push('s.agent_id = ?');
      params.push(agentId);
    }

    if (status) {
      where.push('s.status = ?');
      params.push(status);
    }

    const whereSql = where.length ? ('WHERE ' + where.join(' AND ')) : '';

    const [cntRows] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM ai_sms_sends s
       ${whereSql}`,
      params
    );
    const total = Number(cntRows && cntRows[0] ? (cntRows[0].total || 0) : 0);

    const [rows] = await pool.query(
      `SELECT s.id, s.created_at, s.updated_at,
              s.agent_id,
              a.display_name AS agent_name,
              s.from_number, s.to_number,
              s.message_text,
              s.status,
              s.provider_message_id,
              s.provider_status,
              s.dlr_status,
              s.error
       FROM ai_sms_sends s
       LEFT JOIN ai_agents a ON a.id = s.agent_id
       ${whereSql}
       ORDER BY s.created_at DESC, s.id DESC
       LIMIT ${parseInt(pageSize, 10)} OFFSET ${parseInt(offset, 10)}`,
      params
    );

    return res.json({ success: true, data: rows || [], total });
  } catch (e) {
    if (DEBUG) console.warn('[ai.sms.list] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load SMS' });
  }
});

function slugifyName(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 32) || 'agent';
}

function assertConfiguredOrThrow(name, val) {
  if (!val) throw new Error(`${name} not configured`);
}

function buildPipecatSecrets({ greeting, prompt, cartesiaVoiceId, portalBaseUrl, portalAgentActionToken, operatorNumber, backgroundAudioUrl, backgroundAudioGain }) {
  const deepgram = process.env.DEEPGRAM_API_KEY || '';
  const cartesia = process.env.CARTESIA_API_KEY || '';
  const xai = process.env.XAI_API_KEY || '';
  const daily = process.env.DAILY_API_KEY || '';

  // Video avatar provider (video meetings only; used by the agent runtime)
  const videoAvatarProvider = String(process.env.VIDEO_AVATAR_PROVIDER || '').trim().toLowerCase();

  // Optional provider keys
  const heygen = process.env.HEYGEN_API_KEY || '';
  const heygenAvatarId = process.env.HEYGEN_AVATAR_ID || '';

  const akoolApiKey = process.env.AKOOL_API_KEY || '';
  const akoolAvatarId = process.env.AKOOL_AVATAR_ID || '';
  const akoolBaseUrl = process.env.AKOOL_API_BASE_URL || '';
  const akoolVoiceId = process.env.AKOOL_VOICE_ID || '';
  const akoolBackgroundId = process.env.AKOOL_BACKGROUND_ID || '';
  const akoolModeType = process.env.AKOOL_MODE_TYPE;
  const akoolDurationSeconds = process.env.AKOOL_DURATION_SECONDS;

  // Akool Vision Sense (two-way video)
  const akoolVisionEnabled = process.env.AKOOL_VISION_ENABLED || '';
  const akoolVisionFps = process.env.AKOOL_VISION_FPS;

  // Optional: attach participant camera frames to the LLM as image input
  const akoolVisionLlmEnabled = process.env.AKOOL_VISION_LLM_ENABLED || '';
  const akoolVisionLlmMaxAgeS = process.env.AKOOL_VISION_LLM_MAX_AGE_S;
  const akoolVisionLlmModel = process.env.AKOOL_VISION_LLM_MODEL;
  const akoolVisionLlmAttachMode = process.env.AKOOL_VISION_LLM_ATTACH_MODE;
  const akoolVisionLlmMaxDim = process.env.AKOOL_VISION_LLM_MAX_DIM;
  const akoolVisionLlmJpegQuality = process.env.AKOOL_VISION_LLM_JPEG_QUALITY;

  // Platform-owned keys are required.
  assertConfiguredOrThrow('DEEPGRAM_API_KEY', deepgram);
  assertConfiguredOrThrow('CARTESIA_API_KEY', cartesia);
  assertConfiguredOrThrow('XAI_API_KEY', xai);
  // Required for Daily dial-in handling inside the agent runtime.
  assertConfiguredOrThrow('DAILY_API_KEY', daily);

  const secrets = {
    DEEPGRAM_API_KEY: deepgram,
    CARTESIA_API_KEY: cartesia,
    CARTESIA_VOICE_ID: String(cartesiaVoiceId || ''),
    XAI_API_KEY: xai,
    DAILY_API_KEY: daily,
    // Used by the agent runtime for cold call transfers.
    OPERATOR_NUMBER: String(operatorNumber || ''),
    // Used by the agent runtime to add ambience while the bot speaks.
    BACKGROUND_AUDIO_URL: String(backgroundAudioUrl || ''),
    BACKGROUND_AUDIO_GAIN: (backgroundAudioGain == null ? '' : String(backgroundAudioGain)),
    AGENT_GREETING: String(greeting || ''),
    AGENT_PROMPT: String(prompt || '')
  };

  // Video meeting avatar provider
  if (videoAvatarProvider) {
    secrets.VIDEO_AVATAR_PROVIDER = videoAvatarProvider;
  }

  // Optional: HeyGen (video meetings)
  if (heygen) secrets.HEYGEN_API_KEY = heygen;
  if (heygenAvatarId) secrets.HEYGEN_AVATAR_ID = String(heygenAvatarId);

  // Optional: Akool (video meetings)
  if (akoolApiKey) secrets.AKOOL_API_KEY = akoolApiKey;
  if (akoolAvatarId) secrets.AKOOL_AVATAR_ID = String(akoolAvatarId);
  if (akoolBaseUrl) secrets.AKOOL_API_BASE_URL = String(akoolBaseUrl);
  if (akoolVoiceId) secrets.AKOOL_VOICE_ID = String(akoolVoiceId);
  if (akoolBackgroundId) secrets.AKOOL_BACKGROUND_ID = String(akoolBackgroundId);
  if (akoolModeType != null && String(akoolModeType).trim()) secrets.AKOOL_MODE_TYPE = String(akoolModeType);
  if (akoolDurationSeconds != null && String(akoolDurationSeconds).trim()) secrets.AKOOL_DURATION_SECONDS = String(akoolDurationSeconds);

  if (akoolVisionEnabled && String(akoolVisionEnabled).trim()) secrets.AKOOL_VISION_ENABLED = String(akoolVisionEnabled).trim();
  if (akoolVisionFps != null && String(akoolVisionFps).trim()) secrets.AKOOL_VISION_FPS = String(akoolVisionFps).trim();

  if (akoolVisionLlmEnabled && String(akoolVisionLlmEnabled).trim()) secrets.AKOOL_VISION_LLM_ENABLED = String(akoolVisionLlmEnabled).trim();
  if (akoolVisionLlmMaxAgeS != null && String(akoolVisionLlmMaxAgeS).trim()) secrets.AKOOL_VISION_LLM_MAX_AGE_S = String(akoolVisionLlmMaxAgeS).trim();
  if (akoolVisionLlmModel != null && String(akoolVisionLlmModel).trim()) secrets.AKOOL_VISION_LLM_MODEL = String(akoolVisionLlmModel).trim();
  if (akoolVisionLlmAttachMode != null && String(akoolVisionLlmAttachMode).trim()) secrets.AKOOL_VISION_LLM_ATTACH_MODE = String(akoolVisionLlmAttachMode).trim();
  if (akoolVisionLlmMaxDim != null && String(akoolVisionLlmMaxDim).trim()) secrets.AKOOL_VISION_LLM_MAX_DIM = String(akoolVisionLlmMaxDim).trim();
  if (akoolVisionLlmJpegQuality != null && String(akoolVisionLlmJpegQuality).trim()) secrets.AKOOL_VISION_LLM_JPEG_QUALITY = String(akoolVisionLlmJpegQuality).trim();

  const physicalMailEnabled = String(process.env.AI_PHYSICAL_MAIL_ENABLED || '').trim();
  if (physicalMailEnabled) secrets.AI_PHYSICAL_MAIL_ENABLED = physicalMailEnabled;

  const portalBase = String(portalBaseUrl || process.env.PORTAL_BASE_URL || process.env.PUBLIC_BASE_URL || '').trim();
  if (portalBase) secrets.PORTAL_BASE_URL = portalBase;

  const token = String(portalAgentActionToken || '').trim();
  if (token) secrets.PORTAL_AGENT_ACTION_TOKEN = token;

  return secrets;
}

// Ensure we can safely include PORTAL_AGENT_ACTION_TOKEN in Pipecat secret set updates.
// - If token is present, decrypt and return it.
// - If token is missing, generate + persist a new one and return it.
// This prevents older agents (created before USER_SMTP_ENCRYPTION_KEY was configured) from
// having broken transcript logging and other portal callbacks.
async function getOrCreateAgentActionTokenPlain({ agentRow, userId, label }) {
  const agent = (agentRow && typeof agentRow === 'object') ? agentRow : null;
  const uid = userId != null ? Number(userId) : null;
  const agentId = agent && agent.id != null ? Number(agent.id) : null;
  const tag = String(label || 'ai.agent.token');

  if (!pool) return '';
  if (!agentId || !uid) return '';

  const hasEnc = !!(agent.agent_action_token_enc && agent.agent_action_token_iv && agent.agent_action_token_tag);

  // If a token exists but we can't decrypt it, do NOT proceed with secret set updates
  // (we'd overwrite the existing token with an empty value).
  if (hasEnc) {
    const key = getUserSmtpEncryptionKey();
    if (!key) {
      throw new Error('USER_SMTP_ENCRYPTION_KEY not configured');
    }
    try {
      const tok = decryptAes256Gcm(agent.agent_action_token_enc, agent.agent_action_token_iv, agent.agent_action_token_tag, key);
      const trimmed = String(tok || '').trim();
      if (trimmed) return trimmed;
    } catch (e) {
      if (DEBUG) console.warn(`[${tag}] Failed to decrypt agent action token; rotating:`, e?.message || e);
      // Fallthrough to rotate
    }
  }

  const key = getUserSmtpEncryptionKey();
  if (!key) {
    if (DEBUG) console.warn(`[${tag}] USER_SMTP_ENCRYPTION_KEY not configured; agent action token will not be set`);
    return '';
  }

  const tokenPlain = crypto.randomBytes(32).toString('base64url');
  const tokenHash = sha256(tokenPlain);
  const enc = encryptAes256Gcm(tokenPlain, key);

  await pool.execute(
    'UPDATE ai_agents SET agent_action_token_hash = ?, agent_action_token_enc = ?, agent_action_token_iv = ?, agent_action_token_tag = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND user_id = ? LIMIT 1',
    [tokenHash, enc.enc, enc.iv, enc.tag, agentId, uid]
  );

  // Mutate the passed object so callers in the same request can use the updated fields.
  try {
    agentRow.agent_action_token_hash = tokenHash;
    agentRow.agent_action_token_enc = enc.enc;
    agentRow.agent_action_token_iv = enc.iv;
    agentRow.agent_action_token_tag = enc.tag;
  } catch {}

  if (DEBUG) console.log(`[${tag}] Generated new agent action token`, { agentId, userId: uid });

  return tokenPlain;
}

function normalizeAiTransferDestination(s) {
  // Allow either E.164 numbers like +18005551234 or SIP URIs like sip:agent@domain
  let v = String(s || '').trim();
  if (!v) return '';

  // If it looks like a SIP URI / address, preserve it (but remove whitespace).
  // IMPORTANT: don't strip '.' here, or we'd corrupt domains like example.com.
  if (/^sip:/i.test(v) || v.includes('@')) {
    return v.replace(/\s+/g, '');
  }

  // Otherwise, treat as a phone number and strip common formatting chars.
  v = v.replace(/[\s\-().]/g, '');
  return v;
}

function isValidAiTransferDestination(s) {
  const v = String(s || '').trim();
  if (!v) return true;
  if (/^sip:/i.test(v)) return true;
  if (v.includes('@')) return true;
  // Require E.164-style phone number
  return /^\+\d{8,16}$/.test(v);
}

function normalizeSmsE164Digits(val) {
  return String(val || '').replace(/\D/g, '');
}

function isValidSmsE164Digits(digits) {
  // E.164 max is 15 digits (without the leading '+').
  return /^\d{8,15}$/.test(String(digits || ''));
}

async function getUserAiTransferDestination(userId) {
  try {
    if (!pool) return '';
    const [rows] = await pool.execute(
      'SELECT transfer_to_number FROM user_ai_transfer_settings WHERE user_id = ? LIMIT 1',
      [userId]
    );
    const row = rows && rows[0] ? rows[0] : null;
    return row && row.transfer_to_number ? String(row.transfer_to_number) : '';
  } catch {
    return '';
  }
}

function parseJsonArrayValue(v) {
  try {
    if (v == null) return [];
    if (Array.isArray(v)) return v;
    if (typeof v === 'string') {
      const s = v.trim();
      if (!s) return [];
      const parsed = JSON.parse(s);
      return Array.isArray(parsed) ? parsed : [];
    }
    return [];
  } catch {
    return [];
  }
}

async function getUserAiSmsSettings(userId) {
  try {
    if (!pool) return { allowedDidIds: [], defaultDidId: '' };
    const [rows] = await pool.execute(
      'SELECT allowed_did_ids, default_did_id FROM user_ai_sms_settings WHERE user_id = ? LIMIT 1',
      [userId]
    );
    const row = rows && rows[0] ? rows[0] : null;

    const allowedRaw = row ? row.allowed_did_ids : null;
    const allowed = parseJsonArrayValue(allowedRaw)
      .map(x => String(x || '').trim())
      .filter(Boolean);

    const allowedUniq = Array.from(new Set(allowed)).slice(0, 50);
    const def = row && row.default_did_id ? String(row.default_did_id).trim() : '';

    return {
      allowedDidIds: allowedUniq,
      defaultDidId: def
    };
  } catch {
    return { allowedDidIds: [], defaultDidId: '' };
  }
}

function pipecatSecretsObjectToArray(secretsObj) {
  const obj = (secretsObj && typeof secretsObj === 'object') ? secretsObj : {};
  return Object.entries(obj).map(([k, v]) => ({
    secretKey: String(k),
    secretValue: String(v ?? '')
  }));
}

function buildPipecatSecretSetName(agentName) {
  // Secret set names are length-limited in Pipecat Cloud.
  const suffix = '-secrets';
  const maxLen = 63;
  const maxBase = Math.max(1, maxLen - suffix.length);
  let base = String(agentName || 'agent');
  if (base.length > maxBase) base = base.slice(0, maxBase);
  base = base.replace(/-+$/g, '');
  return (base + suffix).slice(0, maxLen);
}

async function pipecatUpsertSecretSet(setName, secretsObj, regionOverride) {
  const region = String(regionOverride || PIPECAT_REGION || 'us-west');
  const secrets = pipecatSecretsObjectToArray(secretsObj);
  return pipecatApiCall({
    method: 'PUT',
    path: `/secrets/${encodeURIComponent(setName)}`,
    body: { region, secrets }
  });
}

async function pipecatCreateAgent({ agentName, secretSetName, regionOverride }) {
  assertConfiguredOrThrow('PIPECAT_AGENT_IMAGE', PIPECAT_AGENT_IMAGE);

  const autoScaling = buildPipecatAutoScaling();
  const enableKrisp = parseOptionalBoolEnv(PIPECAT_ENABLE_KRISP);
  const region = String(regionOverride || PIPECAT_REGION || 'us-west');

  const body = {
    serviceName: agentName,
    image: PIPECAT_AGENT_IMAGE,
    nodeType: 'arm',
    region,
    secretSet: secretSetName,
    agentProfile: PIPECAT_AGENT_PROFILE,
    ...(autoScaling ? { autoScaling } : {}),
    ...(enableKrisp !== undefined ? { enableKrisp } : {}),
    enableManagedKeys: false,
    ...(PIPECAT_IMAGE_PULL_SECRET_SET ? { imagePullSecretSet: PIPECAT_IMAGE_PULL_SECRET_SET } : {})
  };

  return pipecatApiCall({ method: 'POST', path: '/agents', body });
}

async function pipecatUpdateAgent({ agentName, secretSetName, regionOverride }) {
  assertConfiguredOrThrow('PIPECAT_AGENT_IMAGE', PIPECAT_AGENT_IMAGE);
  // Pipecat Cloud update endpoint expects a subset of the agent configuration.
  const autoScaling = buildPipecatAutoScaling();
  const enableKrisp = parseOptionalBoolEnv(PIPECAT_ENABLE_KRISP);

  const body = {
    image: PIPECAT_AGENT_IMAGE,
    nodeType: 'arm',
    secretSet: secretSetName,
    agentProfile: PIPECAT_AGENT_PROFILE,
    ...(autoScaling ? { autoScaling } : {}),
    ...(enableKrisp !== undefined ? { enableKrisp } : {}),
    enableManagedKeys: false,
    ...(PIPECAT_IMAGE_PULL_SECRET_SET ? { imagePullSecretSet: PIPECAT_IMAGE_PULL_SECRET_SET } : {})
  };
  return pipecatApiCall({ method: 'POST', path: `/agents/${encodeURIComponent(agentName)}`, body });
}

async function pipecatDeleteAgent(agentName) {
  try {
    await pipecatApiCall({ method: 'DELETE', path: `/agents/${encodeURIComponent(agentName)}` });
    return true;
  } catch (e) {
    if (e && e.status === 404) return true;
    throw e;
  }
}

async function pipecatDeleteSecretSet(setName) {
  try {
    await pipecatApiCall({ method: 'DELETE', path: `/secrets/${encodeURIComponent(setName)}` });
    return true;
  } catch (e) {
    if (e && e.status === 404) return true;
    throw e;
  }
}

function buildPipecatDialinWebhookUrl({ agentName }) {
  assertConfiguredOrThrow('PIPECAT_ORG_ID', PIPECAT_ORG_ID);
  // Pipecat public webhook URL used by Daily pinless dial-in config.
  return `${PIPECAT_DIALIN_WEBHOOK_BASE.replace(/\/$/, '')}/${encodeURIComponent(PIPECAT_ORG_ID)}/${encodeURIComponent(agentName)}/dialin`;
}

function buildDailyRoomCreationApiUrl({ agentName }) {
  const publicBase = String(process.env.PUBLIC_BASE_URL || '').trim();
  const localBase = String(AI_DIALIN_WEBHOOK_BASE || '').trim() || (publicBase ? joinUrl(publicBase, '/webhooks/daily/dialin') : '');

  if (!localBase) {
    // Fallback to Pipecat-managed webhook (no local logging).
    return buildPipecatDialinWebhookUrl({ agentName });
  }

  const url = `${String(localBase).replace(/\/$/, '')}/${encodeURIComponent(agentName)}`;

  if (!DAILY_DIALIN_WEBHOOK_TOKEN) return url;

  // Append token as a query param (Daily will call room_creation_api exactly as configured).
  try {
    const u = new URL(url);
    u.searchParams.set('token', DAILY_DIALIN_WEBHOOK_TOKEN);
    return u.toString();
  } catch {
    return url;
  }
}

function parseDailyPhoneNumberBuyResult(buy, fallbackNumber) {
  const dailyNumberId = buy?.id
    || buy?.data?.id
    || buy?.data?.number_id
    || buy?.number_id
    || buy?.data?.phone_number_id
    || buy?.phone_number_id;

  const phoneNumber = buy?.number
    || buy?.data?.number
    || buy?.phone_number
    || buy?.data?.phone_number
    || fallbackNumber;

  if (!dailyNumberId || !phoneNumber) {
    if (DEBUG) console.warn('[ai.number.buy] Unexpected Daily response:', buy);
    throw new Error('Phone number purchase succeeded but the response was missing id/number');
  }

  return { dailyNumberId: String(dailyNumberId), phoneNumber: String(phoneNumber) };
}

async function dailyBuySpecificPhoneNumber(number) {
  const desired = String(number || '').trim();
  if (!desired) throw new Error('Phone number is required');
  const buy = await dailyApiCall({ method: 'POST', path: '/buy-phone-number', body: { number: desired } });
  return parseDailyPhoneNumberBuyResult(buy, desired);
}

async function dailyBuyAnyAvailableNumber() {
  // Daily Phone Numbers
  // GET /list-available-numbers -> { total_count, data: [{ number, region, ... }] }
  let pickNumber = null;

  try {
    const avail = await dailyApiCall({ method: 'GET', path: '/list-available-numbers' });
    const items = Array.isArray(avail?.data)
      ? avail.data
      : (Array.isArray(avail?.numbers) ? avail.numbers : (Array.isArray(avail) ? avail : []));

    const pick = (Array.isArray(items) && items.length > 0) ? (items[0] || {}) : null;
    pickNumber = pick ? (pick.number || pick.phone_number || pick.phoneNumber) : null;
  } catch (e) {
    if (DEBUG) console.warn('[ai.number.buy] list-available-numbers failed:', e?.message || e);
    pickNumber = null;
  }

  // POST /buy-phone-number: pass { number } if we found one; otherwise omit to buy a random CA number.
  const buyBody = pickNumber ? { number: String(pickNumber) } : {};
  const buy = await dailyApiCall({ method: 'POST', path: '/buy-phone-number', body: buyBody });

  return parseDailyPhoneNumberBuyResult(buy, pickNumber);
}

async function dailyReleasePhoneNumber(dailyNumberId) {
  if (!dailyNumberId) return true;
  try {
    await dailyApiCall({ method: 'DELETE', path: `/release-phone-number/${encodeURIComponent(String(dailyNumberId))}` });
    return true;
  } catch (e) {
    // Treat missing numbers as already released.
    if (e && (e.status === 404 || e.status === 410)) return true;
    throw e;
  }
}

function normalizePhoneNumber(s) {
  return String(s || '').replace(/\s+/g, '');
}

async function dailyFindDialinConfigByPhoneNumber(phoneNumber) {
  try {
    // Query directly by phone number (avoid pagination issues).
    const list = await dailyApiCall({
      method: 'GET',
      path: '/domain-dialin-config',
      params: {
        phone_number: String(normalizePhoneNumber(phoneNumber)),
        limit: 1
      }
    });

    const arr = Array.isArray(list?.data)
      ? list.data
      : (Array.isArray(list) ? list : []);

    const item = arr[0];
    const id = item?.id || item?.config_id || item?.domain_dialin_config_id;
    const type = item?.type || item?.dialin_type || item?.dialinType || null;

    if (!id) return null;
    return { dialinConfigId: String(id), type: type ? String(type) : null };
  } catch (e) {
    if (DEBUG) console.warn('[ai.dialin.list] Failed to list domain dial-in configs:', e?.message || e);
  }
  return null;
}

async function dailyUpsertDialinConfig({ phoneNumber, roomCreationApi }) {
  // Daily Domain Dial-In Config
  // POST /domain-dialin-config body params include: phone_number, room_creation_api, name_prefix, type, ...
  const normalizedPhoneNumber = normalizePhoneNumber(phoneNumber);
  const existing = await dailyFindDialinConfigByPhoneNumber(normalizedPhoneNumber);

  const payload = {
    // Daily requires explicitly specifying the dial-in config type.
    // For inbound agent calls we use pinless dial-in.
    type: 'pinless_dialin',
    phone_number: String(normalizedPhoneNumber),
    room_creation_api: String(roomCreationApi),
    name_prefix: 'Phone.System'
  };

  // If a config exists with a different type (e.g. pin_dialin), delete and recreate.
  if (existing?.dialinConfigId && existing?.type && existing.type !== 'pinless_dialin') {
    if (DEBUG) console.warn('[ai.dialin.upsert] Existing dial-in config has incompatible type; recreating', {
      phoneNumber: normalizedPhoneNumber,
      existingType: existing.type,
      existingId: existing.dialinConfigId
    });
    try { await dailyDeleteDialinConfig(existing.dialinConfigId); } catch {}
    const created = await dailyApiCall({ method: 'POST', path: '/domain-dialin-config', body: payload });
    const createdId = created?.id || created?.data?.id;
    if (!createdId) {
      if (DEBUG) console.warn('[ai.dialin.create] Unexpected Daily response:', created);
      throw new Error('Failed to create dial-in config (missing id)');
    }
    return { dialinConfigId: String(createdId) };
  }

  if (existing?.dialinConfigId) {
    try {
      const updated = await dailyApiCall({
        method: 'PUT',
        path: `/domain-dialin-config/${encodeURIComponent(existing.dialinConfigId)}`,
        body: payload
      });
      const id = updated?.id || updated?.data?.id || existing.dialinConfigId;
      return { dialinConfigId: String(id) };
    } catch (e) {
      // Some Daily accounts/configs reject updates depending on existing state.
      // Fall back to delete+recreate for resilience.
      if (DEBUG) console.warn('[ai.dialin.upsert] Update failed; recreating', {
        phoneNumber: normalizedPhoneNumber,
        existingId: existing.dialinConfigId,
        status: e?.status,
        message: e?.message
      });
      try { await dailyDeleteDialinConfig(existing.dialinConfigId); } catch {}
      const created = await dailyApiCall({ method: 'POST', path: '/domain-dialin-config', body: payload });
      const createdId = created?.id || created?.data?.id;
      if (!createdId) {
        if (DEBUG) console.warn('[ai.dialin.create] Unexpected Daily response:', created);
        throw new Error('Failed to create dial-in config (missing id)');
      }
      return { dialinConfigId: String(createdId) };
    }
  }

  const created = await dailyApiCall({ method: 'POST', path: '/domain-dialin-config', body: payload });
  const createdId = created?.id || created?.data?.id;
  if (!createdId) {
    if (DEBUG) console.warn('[ai.dialin.create] Unexpected Daily response:', created);
    throw new Error('Failed to create dial-in config (missing id)');
  }
  return { dialinConfigId: String(createdId) };
}

async function dailyDeleteDialinConfig(dialinConfigId) {
  if (!dialinConfigId) return true;
  try {
    await dailyApiCall({ method: 'DELETE', path: `/domain-dialin-config/${encodeURIComponent(dialinConfigId)}` });
    return true;
  } catch (e) {
    if (e && e.status === 404) return true;
    throw e;
  }
}

async function disableAiDialinRoutingForUser({ localUserId }) {
  if (!pool || !localUserId) return { disabledCount: 0 };
  let disabledCount = 0;

  const [rows] = await pool.execute(
    'SELECT id, dialin_config_id FROM ai_numbers WHERE user_id = ? AND dialin_config_id IS NOT NULL',
    [localUserId]
  );

  for (const r of rows || []) {
    const numId = r?.id;
    const dialinConfigId = r?.dialin_config_id;
    if (!numId || !dialinConfigId) continue;

    try {
      await dailyDeleteDialinConfig(dialinConfigId);
    } catch (e) {
      if (DEBUG) console.warn('[ai.dialin.disable] Failed to delete dial-in config:', {
        localUserId,
        numId,
        dialinConfigId,
        message: e?.message || e
      });
    }

    try {
      await pool.execute(
        'UPDATE ai_numbers SET dialin_config_id = NULL WHERE id = ? AND user_id = ?',
        [numId, localUserId]
      );
    } catch (e) {
      if (DEBUG) console.warn('[ai.dialin.disable] Failed to clear dialin_config_id:', e?.message || e);
    }

    disabledCount += 1;
  }

  return { disabledCount };
}

// Balance enforcement: when a user's balance goes NEGATIVE, unassign all AI numbers.
// This removes inbound routing (Daily dial-in config) and clears the UI assignment.
async function unassignAiNumbersForUser({ localUserId }) {
  if (!pool || !localUserId) return { unassignedCount: 0 };
  let unassignedCount = 0;

  const [rows] = await pool.execute(
    'SELECT id, dialin_config_id, agent_id FROM ai_numbers WHERE user_id = ? AND (agent_id IS NOT NULL OR dialin_config_id IS NOT NULL)',
    [localUserId]
  );

  for (const r of rows || []) {
    const numId = r?.id;
    const dialinConfigId = r?.dialin_config_id;
    if (!numId) continue;

    if (dialinConfigId) {
      try { await dailyDeleteDialinConfig(dialinConfigId); } catch (e) {
        if (DEBUG) console.warn('[ai.numbers.unassignAll] dialin delete failed', {
          localUserId,
          numId,
          dialinConfigId,
          message: e?.message || e
        });
      }
    }

    try {
      await pool.execute(
        'UPDATE ai_numbers SET agent_id = NULL, dialin_config_id = NULL WHERE id = ? AND user_id = ? LIMIT 1',
        [numId, localUserId]
      );
    } catch (e) {
      if (DEBUG) console.warn('[ai.numbers.unassignAll] DB update failed', e?.message || e);
      continue;
    }

    unassignedCount += 1;
  }

  return { unassignedCount };
}

async function enableAiDialinRoutingForUser({ localUserId }) {
  if (!pool || !localUserId) return { enabledCount: 0 };
  let enabledCount = 0;

  const [rows] = await pool.execute(
    `SELECT n.id, n.phone_number, n.agent_id, n.dialin_config_id, a.pipecat_agent_name
     FROM ai_numbers n
     LEFT JOIN ai_agents a ON a.id = n.agent_id
     WHERE n.user_id = ? AND n.agent_id IS NOT NULL AND n.cancel_pending = 0`,
    [localUserId]
  );

  for (const r of rows || []) {
    try {
      const numId = r?.id;
      const phoneNumber = r?.phone_number;
      const dialinConfigIdExisting = r?.dialin_config_id;
      const agentName = r?.pipecat_agent_name;

      if (!numId || !phoneNumber || dialinConfigIdExisting) continue;
      if (!agentName) continue;

      const roomCreationApi = buildDailyRoomCreationApiUrl({ agentName });
      const { dialinConfigId } = await dailyUpsertDialinConfig({ phoneNumber, roomCreationApi });

      await pool.execute(
        'UPDATE ai_numbers SET dialin_config_id = ? WHERE id = ? AND user_id = ?',
        [dialinConfigId, numId, localUserId]
      );

      enabledCount += 1;
    } catch (e) {
      if (DEBUG) console.warn('[ai.dialin.enable] Failed to enable dial-in routing:', e?.message || e);
    }
  }

  return { enabledCount };
}

async function syncAiDialinRoutingForUser({ localUserId }) {
  if (!pool || !localUserId) return { ok: false, reason: 'missing_params' };

  let credit = null;
  try {
    credit = await getUserBalance(localUserId);
    credit = Number.isFinite(credit) ? Number(credit) : null;
  } catch (e) {
    if (DEBUG) console.warn('[ai.dialin.sync] Failed to fetch balance:', e?.message || e);
    return { ok: false, reason: 'credit_fetch_failed' };
  }

  if (credit == null) return { ok: false, reason: 'credit_missing' };

  const shouldBlock = credit < AI_INBOUND_MIN_CREDIT;

  if (!AI_INBOUND_DISABLE_NUMBERS_WHEN_BALANCE_LOW) {
    return { ok: true, skipped: true, credit, shouldBlock };
  }

  if (shouldBlock) {
    const { disabledCount } = await disableAiDialinRoutingForUser({ localUserId });
    return { ok: true, credit, shouldBlock, disabledCount, enabledCount: 0 };
  }

  const { enabledCount } = await enableAiDialinRoutingForUser({ localUserId });
  return { ok: true, credit, shouldBlock, enabledCount, disabledCount: 0 };
}

function toMysqlDatetime(d) {
  try {
    if (!d) return null;
    const dt = d instanceof Date ? d : new Date(d);
    if (!Number.isFinite(dt.getTime())) return null;
    const pad2 = (n) => String(n).padStart(2, '0');
    return `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())} ${pad2(dt.getHours())}:${pad2(dt.getMinutes())}:${pad2(dt.getSeconds())}`;
  } catch {
    return null;
  }
}

function addDays(date, days) {
  try {
    const dt = date instanceof Date ? new Date(date.getTime()) : new Date(date);
    if (!Number.isFinite(dt.getTime())) return null;
    dt.setDate(dt.getDate() + (Number(days) || 0));
    return dt;
  } catch {
    return null;
  }
}

async function markAiNumberCancelPending({ localUserId, aiNumberId, billedTo = null }) {
  if (!pool || !localUserId || !aiNumberId) return false;

  const now = new Date();
  const cancelAfter = addDays(now, AI_MONTHLY_GRACE_DAYS) || now;
  const cancelAfterDb = toMysqlDatetime(cancelAfter);
  const billedToDb = toMysqlDatetime(billedTo);

  try {
    const [res] = await pool.execute(
      `UPDATE ai_numbers
       SET cancel_pending = 1,
           cancel_pending_since = COALESCE(cancel_pending_since, NOW()),
           cancel_after = COALESCE(cancel_after, ?),
           cancel_billed_to = COALESCE(cancel_billed_to, ?),
           cancel_notice_initial_sent_at = NULL,
           cancel_notice_reminder_sent_at = NULL
       WHERE id = ? AND user_id = ? AND cancel_pending = 0
       LIMIT 1`,
      [cancelAfterDb, billedToDb, aiNumberId, localUserId]
    );
    return !!(res && res.affectedRows > 0);
  } catch {
    return false;
  }
}

async function disableAiDialinRoutingForNumber({ localUserId, aiNumberId, dialinConfigId }) {
  if (!pool || !localUserId || !aiNumberId) return { ok: false };

  if (dialinConfigId) {
    try { await dailyDeleteDialinConfig(dialinConfigId); } catch {}
  }

  try {
    await pool.execute(
      'UPDATE ai_numbers SET dialin_config_id = NULL WHERE id = ? AND user_id = ? LIMIT 1',
      [aiNumberId, localUserId]
    );
  } catch {}

  return { ok: true };
}

async function cancelAiNumberForNonpayment({ localUserId, aiNumberId, dailyNumberId, dialinConfigId }) {
  if (!pool || !localUserId || !aiNumberId) return { ok: false, reason: 'missing_params' };

  const dailyId = String(dailyNumberId || '').trim();
  if (!dailyId) {
    // Without the Daily phone-number id we cannot safely release the number.
    return { ok: false, reason: 'missing_daily_number_id' };
  }

  // Disable inbound routing immediately (remove Daily dial-in config).
  try {
    await disableAiDialinRoutingForNumber({ localUserId, aiNumberId, dialinConfigId });
  } catch {}

  // Release the purchased phone number in Daily.
  try {
    await dailyReleasePhoneNumber(dailyId);
  } catch (e) {
    return { ok: false, reason: 'daily_release_failed', error: e?.data || e?.message || e };
  }

  // Remove from local DB.
  try {
    await pool.execute(
      'DELETE FROM ai_numbers WHERE id = ? AND user_id = ? LIMIT 1',
      [aiNumberId, localUserId]
    );
  } catch {}

  return { ok: true };
}

// Charge a single AI number's monthly fee via balance deduction.
// Returns { ok: true } on success.
async function chargeAiNumberMonthlyFee({ userId, aiNumberId, phoneNumber, monthlyAmount, isTollfree, billedTo }) {
  if (!pool || !userId || !aiNumberId || !monthlyAmount) return { ok: false, reason: 'missing_params' };
  const typeLabel = isTollfree ? 'toll-free' : 'local';
  const description = `AI ${typeLabel} DID monthly fee – ${phoneNumber || aiNumberId}`;
  const result = await deductBalance(userId, monthlyAmount, description);
  if (!result || !result.success) {
    return { ok: false, reason: result?.error || 'deduction_failed' };
  }
  // Update the billing marker
  try {
    const billedToDb = toMysqlDatetime(billedTo);
    if (billedToDb) {
      await pool.execute(
        'INSERT INTO ai_number_markups (user_id, ai_number_id, last_billed_to) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE last_billed_to = GREATEST(COALESCE(last_billed_to, VALUES(last_billed_to)), VALUES(last_billed_to))',
        [userId, aiNumberId, billedToDb]
      );
    }
  } catch {}
  return { ok: true };
}

// Iterate a user's AI numbers and charge monthly fees with cycle-table idempotency.
async function billAiNumberMonthlyFeesForUser({ localUserId, aiNumbers }) {
  if (!pool || !localUserId) return;
  const localFee = parseFloat(process.env.AI_DID_LOCAL_MONTHLY_FEE || process.env.DID_LOCAL_MONTHLY_MARKUP || '0') || 0;
  const tollfreeFee = parseFloat(process.env.AI_DID_TOLLFREE_MONTHLY_FEE || process.env.DID_TOLLFREE_MONTHLY_MARKUP || '0') || 0;
  if (!localFee && !tollfreeFee) return;

  for (const num of (aiNumbers || [])) {
    const aiNumberId = num.id != null ? String(num.id) : '';
    const phoneNumber = String(num.phone_number || '').trim();
    if (!aiNumberId) continue;

    const isTollfree = detectTollfreeUsCaByNpa(phoneNumber);
    const monthlyAmount = isTollfree ? tollfreeFee : localFee;
    if (!monthlyAmount || monthlyAmount <= 0) continue;

    // Compute the next billing period
    const createdAt = num.created_at instanceof Date ? num.created_at : new Date(num.created_at || Date.now());
    let billedTo = null;
    try {
      const [mrows] = await pool.execute(
        'SELECT last_billed_to FROM ai_number_markups WHERE user_id = ? AND ai_number_id = ? LIMIT 1',
        [localUserId, aiNumberId]
      );
      const lastBilled = mrows && mrows[0] && mrows[0].last_billed_to;
      if (lastBilled) {
        billedTo = addMonthsClamped(new Date(lastBilled), 1);
      } else {
        billedTo = addMonthsClamped(createdAt, 1);
      }
    } catch {
      billedTo = addMonthsClamped(createdAt, 1);
    }

    if (!billedTo || !Number.isFinite(billedTo.getTime())) continue;
    // Only bill if the billing period has arrived
    if (billedTo.getTime() > Date.now()) continue;

    const billedToDb = toMysqlDatetime(billedTo);
    // Cycle-table idempotency: INSERT IGNORE prevents double-billing the same period
    try {
      const [insRes] = await pool.execute(
        'INSERT IGNORE INTO ai_number_markup_cycles (user_id, ai_number_id, billed_to) VALUES (?, ?, ?)',
        [localUserId, aiNumberId, billedToDb]
      );
      if (!insRes || insRes.affectedRows === 0) continue; // Already billed
    } catch {
      continue;
    }

    const chargeRes = await chargeAiNumberMonthlyFee({
      userId: localUserId,
      aiNumberId,
      phoneNumber,
      monthlyAmount,
      isTollfree,
      billedTo
    });

    if (!chargeRes || !chargeRes.ok) {
      // Roll back cycle marker for retry
      try {
        await pool.execute(
          'DELETE FROM ai_number_markup_cycles WHERE user_id = ? AND ai_number_id = ? AND billed_to = ?',
          [localUserId, aiNumberId, billedToDb]
        );
      } catch {}
      // If enabled, mark for cancellation
      if (AI_MONTHLY_CANCEL_ON_INSUFFICIENT_BALANCE) {
        try {
          await markAiNumberCancelPending({ localUserId, aiNumberId, billedTo });
        } catch {}
      }
    }
  }
}

async function processAiCancelPendingForUser({ localUserId, limit = 100 }) {
  if (!pool || !localUserId) return { processed: 0, cancelled: 0, charged: 0, failed: 0, deferred: 0 };
  const safeLimit = Math.max(1, Math.min(500, parseInt(String(limit || '100'), 10) || 100));

  const now = new Date();

  // Load user contact info (best-effort)
  let toEmail = null;
  let displayName = null;
  try {
    const [[u]] = await pool.execute(
      'SELECT email, username, firstname, lastname FROM signup_users WHERE id = ? LIMIT 1',
      [localUserId]
    );
    if (u) {
      toEmail = u.email || null;
      const name = `${u.firstname || ''} ${u.lastname || ''}`.trim();
      displayName = name || u.username || null;
    }
  } catch {}

  const [rows] = await pool.query(
    `SELECT n.id, n.daily_number_id, n.dialin_config_id, n.phone_number, n.created_at,
            n.cancel_pending_since, n.cancel_after, n.cancel_billed_to,
            n.cancel_notice_initial_sent_at, n.cancel_notice_reminder_sent_at,
            m.last_billed_to
     FROM ai_numbers n
     LEFT JOIN ai_number_markups m ON m.user_id = n.user_id AND m.ai_number_id = n.id
     WHERE n.user_id = ? AND n.cancel_pending = 1
     ORDER BY n.id ASC
     LIMIT ${safeLimit}`,
    [localUserId]
  );

  if (!rows || rows.length === 0) {
    return { processed: 0, cancelled: 0, charged: 0, failed: 0, deferred: 0 };
  }

  // Ensure cancel_pending_since and cancel_after are set for older rows.
  for (const r of rows) {
    const aiNumberId = r?.id != null ? String(r.id) : '';
    if (!aiNumberId) continue;

    const hasPendingSince = !!r.cancel_pending_since;
    const ca = r.cancel_after instanceof Date ? r.cancel_after : (r.cancel_after ? new Date(r.cancel_after) : null);
    const hasCancelAfter = ca && Number.isFinite(ca.getTime());

    if (!hasPendingSince || !hasCancelAfter) {
      const cancelAfter = addDays(now, AI_MONTHLY_GRACE_DAYS) || now;
      try {
        await pool.execute(
          'UPDATE ai_numbers SET cancel_pending_since = COALESCE(cancel_pending_since, NOW()), cancel_after = COALESCE(cancel_after, ?) WHERE id = ? AND user_id = ? AND cancel_pending = 1 LIMIT 1',
          [toMysqlDatetime(cancelAfter), aiNumberId, localUserId]
        );
      } catch {}
    }
  }

  // Send initial notice (once)
  try {
    const initialRows = rows.filter(r => !r.cancel_notice_initial_sent_at);
    if (toEmail && initialRows.length) {
      let earliestCancelAfter = null;
      const nums = [];
      const ids = [];
      for (const r of initialRows) {
        const aiNumberId = r?.id != null ? String(r.id) : '';
        if (!aiNumberId) continue;
        ids.push(aiNumberId);
        nums.push(String(r?.phone_number || aiNumberId));
        const ca = r.cancel_after instanceof Date ? r.cancel_after : (r.cancel_after ? new Date(r.cancel_after) : null);
        if (ca && Number.isFinite(ca.getTime())) {
          if (!earliestCancelAfter || ca.getTime() < earliestCancelAfter.getTime()) earliestCancelAfter = ca;
        }
      }

      const sendRes = await sendNonpaymentGraceInitialEmail({
        toEmail,
        displayName,
        numbers: nums,
        graceDays: AI_MONTHLY_GRACE_DAYS,
        cancelAfter: earliestCancelAfter,
        portalUrl: process.env.PUBLIC_BASE_URL,
        productLabel: 'AI phone number'
      });

      if (sendRes && sendRes.ok && ids.length) {
        const ph = ids.map(() => '?').join(',');
        await pool.execute(
          `UPDATE ai_numbers SET cancel_notice_initial_sent_at = NOW()
           WHERE user_id = ? AND cancel_pending = 1 AND id IN (${ph})`,
          [localUserId, ...ids]
        );
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[ai.nonpayment] Failed to send initial notice:', e?.message || e);
  }

  // Send reminder when <24h remaining (once)
  try {
    const remindRows = rows.filter(r => {
      if (r.cancel_notice_reminder_sent_at) return false;
      const ca = r.cancel_after instanceof Date ? r.cancel_after : (r.cancel_after ? new Date(r.cancel_after) : null);
      if (!ca || !Number.isFinite(ca.getTime())) return false;
      const msLeft = ca.getTime() - now.getTime();
      return msLeft > 0 && msLeft <= (24 * 60 * 60 * 1000);
    });

    if (toEmail && remindRows.length) {
      let earliestCancelAfter = null;
      const nums = [];
      const ids = [];
      for (const r of remindRows) {
        const aiNumberId = r?.id != null ? String(r.id) : '';
        if (!aiNumberId) continue;
        ids.push(aiNumberId);
        nums.push(String(r?.phone_number || aiNumberId));
        const ca = r.cancel_after instanceof Date ? r.cancel_after : (r.cancel_after ? new Date(r.cancel_after) : null);
        if (ca && Number.isFinite(ca.getTime())) {
          if (!earliestCancelAfter || ca.getTime() < earliestCancelAfter.getTime()) earliestCancelAfter = ca;
        }
      }

      const sendRes = await sendNonpaymentGraceReminderEmail({
        toEmail,
        displayName,
        numbers: nums,
        graceDays: AI_MONTHLY_GRACE_DAYS,
        cancelAfter: earliestCancelAfter,
        portalUrl: process.env.PUBLIC_BASE_URL,
        productLabel: 'AI phone number'
      });

      if (sendRes && sendRes.ok && ids.length) {
        const ph = ids.map(() => '?').join(',');
        await pool.execute(
          `UPDATE ai_numbers SET cancel_notice_reminder_sent_at = NOW()
           WHERE user_id = ? AND cancel_pending = 1 AND id IN (${ph})`,
          [localUserId, ...ids]
        );
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[ai.nonpayment] Failed to send reminder notice:', e?.message || e);
  }

  // Fetch current credit once and track it locally to avoid spamming failed billing rows.
  let creditRemaining = null;
  try {
    const bal = await getUserBalance(localUserId);
    if (Number.isFinite(bal)) creditRemaining = Number(bal);
  } catch (e) {
    if (DEBUG) console.warn('[ai.nonpayment] Credit fetch failed during cancel_pending processing:', e?.message || e);
    creditRemaining = null;
  }

  const localFee = parseFloat(process.env.AI_DID_LOCAL_MONTHLY_FEE || process.env.DID_LOCAL_MONTHLY_MARKUP || '0') || 0;
  const tollfreeFee = parseFloat(process.env.AI_DID_TOLLFREE_MONTHLY_FEE || process.env.DID_TOLLFREE_MONTHLY_MARKUP || '0') || 0;

  let charged = 0;
  let cancelled = 0;
  let failed = 0;
  let deferred = 0;

  for (const r of rows || []) {
    const aiNumberId = r?.id != null ? String(r.id) : '';
    const dailyNumberId = r?.daily_number_id != null ? String(r.daily_number_id) : '';
    const dialinConfigId = r?.dialin_config_id != null ? String(r.dialin_config_id) : null;
    const phoneNumber = String(r?.phone_number || '').trim();
    if (!aiNumberId) continue;

    const ca = r.cancel_after instanceof Date ? r.cancel_after : (r.cancel_after ? new Date(r.cancel_after) : null);
    const cancelAfter = (ca && Number.isFinite(ca.getTime())) ? ca : null;
    const expired = cancelAfter && now.getTime() >= cancelAfter.getTime();

    let billedTo = r.cancel_billed_to instanceof Date ? r.cancel_billed_to : (r.cancel_billed_to ? new Date(r.cancel_billed_to) : null);
    if (!billedTo || !Number.isFinite(billedTo.getTime())) {
      // Derive a best-effort billed_to for older pending rows.
      const last = r.last_billed_to instanceof Date ? r.last_billed_to : (r.last_billed_to ? new Date(r.last_billed_to) : null);
      const base = (last && Number.isFinite(last.getTime()))
        ? last
        : (r.created_at instanceof Date ? r.created_at : (r.created_at ? new Date(r.created_at) : null));
      if (base && Number.isFinite(base.getTime())) {
        billedTo = addMonthsClamped(base, 1);
      }
      if (billedTo && Number.isFinite(billedTo.getTime())) {
        try {
          await pool.execute(
            'UPDATE ai_numbers SET cancel_billed_to = COALESCE(cancel_billed_to, ?) WHERE id = ? AND user_id = ? AND cancel_pending = 1 LIMIT 1',
            [toMysqlDatetime(billedTo), aiNumberId, localUserId]
          );
        } catch {}
      }
    }

    const isTollfree = detectTollfreeUsCaByNpa(phoneNumber);
    let monthlyAmount = isTollfree ? tollfreeFee : localFee;
    if (!monthlyAmount || monthlyAmount <= 0) {
      monthlyAmount = isTollfree ? localFee : tollfreeFee;
    }
    if (!monthlyAmount || monthlyAmount <= 0) continue;

    // Attempt to recover (charge) if we can safely determine billed_to and the credit is sufficient.
    if (creditRemaining != null && billedTo && Number.isFinite(billedTo.getTime()) && creditRemaining >= monthlyAmount) {
      const billedToDb = toMysqlDatetime(billedTo);

      try {
        const [insRes] = await pool.execute(
          'INSERT IGNORE INTO ai_number_markup_cycles (user_id, ai_number_id, billed_to) VALUES (?, ?, ?)',
          [localUserId, aiNumberId, billedToDb]
        );

        if (insRes && insRes.affectedRows === 0) {
          // Already billed elsewhere; align reporting and clear pending.
          try {
            await pool.execute(
              'INSERT INTO ai_number_markups (user_id, ai_number_id, last_billed_to) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE last_billed_to = GREATEST(COALESCE(last_billed_to, VALUES(last_billed_to)), VALUES(last_billed_to))',
              [localUserId, aiNumberId, billedToDb]
            );
          } catch {}
          try {
            await pool.execute(
              'UPDATE ai_numbers SET cancel_pending = 0, cancel_pending_since = NULL, cancel_after = NULL, cancel_billed_to = NULL, cancel_notice_initial_sent_at = NULL, cancel_notice_reminder_sent_at = NULL WHERE id = ? AND user_id = ? LIMIT 1',
              [aiNumberId, localUserId]
            );
          } catch {}
          continue;
        }

        const chargeRes = await chargeAiNumberMonthlyFee({
          userId: localUserId,
          aiNumberId,
          phoneNumber,
          monthlyAmount,
          isTollfree,
          billedTo
        });

        if (chargeRes && chargeRes.ok) {
          charged += 1;
          // Update reporting marker
          try {
            await pool.execute(
              'INSERT INTO ai_number_markups (user_id, ai_number_id, last_billed_to) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE last_billed_to = VALUES(last_billed_to)',
              [localUserId, aiNumberId, billedToDb]
            );
          } catch {}
          // Clear pending
          try {
            await pool.execute(
              'UPDATE ai_numbers SET cancel_pending = 0, cancel_pending_since = NULL, cancel_after = NULL, cancel_billed_to = NULL, cancel_notice_initial_sent_at = NULL, cancel_notice_reminder_sent_at = NULL WHERE id = ? AND user_id = ? LIMIT 1',
              [aiNumberId, localUserId]
            );
          } catch {}

          creditRemaining = Number(creditRemaining) - monthlyAmount;
          continue;
        }

        // Roll back the cycle marker so we can retry later.
        try {
          await pool.execute(
            'DELETE FROM ai_number_markup_cycles WHERE user_id = ? AND ai_number_id = ? AND billed_to = ?',
            [localUserId, aiNumberId, billedToDb]
          );
        } catch {}
      } catch (e) {
        if (DEBUG) console.warn('[ai.nonpayment] Retry charge failed:', e?.message || e);
      }
    }

    // Only cancel after the grace window expires.
    if (expired) {
      if (creditRemaining == null) {
        deferred += 1;
        continue;
      }
      if (!aiNumberId || !dailyNumberId) {
        failed += 1;
        continue;
      }
      try {
        const res = await cancelAiNumberForNonpayment({ localUserId, aiNumberId, dailyNumberId, dialinConfigId });
        if (res && res.ok) cancelled += 1;
        else failed += 1;
      } catch {
        failed += 1;
      }
    }
  }

  return { processed: (rows || []).length, cancelled, charged, failed, deferred };
}

// List Cartesia voices (used by the dashboard voice picker)
const CARTESIA_VOICES_CACHE_MS = 10 * 60 * 1000;
let cartesiaVoicesCache = { at: 0, data: null };
app.get('/api/me/ai/cartesia/voices', requireAuth, async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    const limitRaw = parseInt(String(req.query.limit || '100'), 10);
    const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? limitRaw : 100));

    if (!q && cartesiaVoicesCache.data && (Date.now() - cartesiaVoicesCache.at) < CARTESIA_VOICES_CACHE_MS) {
      return res.json({ success: true, data: cartesiaVoicesCache.data });
    }

    const params = { limit };
    if (q) params.q = q;

    const raw = await cartesiaApiCall({ method: 'GET', path: '/voices', params });
    const arr = Array.isArray(raw?.data) ? raw.data : (Array.isArray(raw) ? raw : []);

    const data = (arr || [])
      .map(v => ({
        id: v?.id,
        name: v?.name || v?.display_name || v?.displayName || '',
        language: v?.language || v?.lang || '',
        gender: v?.gender || ''
      }))
      .filter(v => v && v.id);

    if (!q) {
      cartesiaVoicesCache = { at: Date.now(), data };
    }

    return res.json({ success: true, data });
  } catch (e) {
    const msg = e?.message || 'Failed to load Cartesia voices';
    if (DEBUG) console.warn('[ai.cartesia.voices] error:', msg);
    return res.status(502).json({ success: false, message: msg });
  }
});

// List agents
app.get('/api/me/ai/agents', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const [rows] = await pool.execute(
      `SELECT a.id, a.display_name, a.greeting, a.prompt, a.cartesia_voice_id,
              a.background_audio_url, a.background_audio_gain, a.transfer_to_number,
              a.inbound_transfer_enabled, a.inbound_transfer_number,
              CASE WHEN ba.agent_id IS NULL THEN 0 ELSE 1 END AS background_audio_uploaded,
              ba.original_filename AS background_audio_upload_filename,
              ba.size_bytes AS background_audio_upload_size_bytes,
              ba.updated_at AS background_audio_upload_updated_at,
              a.pipecat_agent_name, a.pipecat_region,
              a.default_doc_template_id,
              a.created_at, a.updated_at,
              n.id AS number_id, n.phone_number AS phone_number
       FROM ai_agents a
       LEFT JOIN ai_numbers n ON n.agent_id = a.id
       LEFT JOIN ai_agent_background_audio ba ON ba.agent_id = a.id
       WHERE a.user_id = ?
       ORDER BY a.created_at DESC`,
      [userId]
    );
    return res.json({ success: true, data: rows || [] });
  } catch (e) {
    if (DEBUG) console.error('[ai.agents.list] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load agents' });
  }
});

// List inbound AI call sessions (Daily dial-in) for the logged-in user.
// These are the "sessions" used for conversation transcript viewing.
app.get('/api/me/ai/conversations/sessions', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const page = Math.max(0, parseInt(String(req.query.page || '0'), 10) || 0);
    const pageSizeRaw = parseInt(String(req.query.pageSize || '25'), 10) || 25;
    const pageSize = Math.max(1, Math.min(100, pageSizeRaw));
    const offset = page * pageSize;

    const agentIdRaw = String(req.query.agent_id || req.query.agentId || '').trim();
    const agentId = agentIdRaw ? agentIdRaw : '';

    const where = [`c.user_id = ?`, `c.direction = 'inbound'`];
    const params = [userId];

    if (agentId) {
      where.push('c.agent_id = ?');
      params.push(agentId);
    }

    const whereSql = where.length ? ('WHERE ' + where.join(' AND ')) : '';

    const [cntRows] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM ai_call_logs c
       ${whereSql}`,
      params
    );
    const total = Number(cntRows && cntRows[0] ? (cntRows[0].total || 0) : 0);

    // Use query() instead of execute() because some MySQL setups error when binding LIMIT/OFFSET
    // placeholders in prepared statements ("Incorrect arguments to mysqld_stmt_execute").
    const [rows] = await pool.query(
      `SELECT c.call_id, c.call_domain, c.agent_id,
              a.display_name AS agent_name,
              c.from_number, c.to_number,
              c.time_start, c.time_connect, c.time_end,
              c.duration, c.billsec,
              c.status,
              COALESCE(m.msg_count, 0) AS message_count
       FROM ai_call_logs c
       LEFT JOIN ai_agents a ON a.id = c.agent_id
       LEFT JOIN (
         SELECT call_domain, call_id, COUNT(*) AS msg_count
         FROM ai_call_messages
         WHERE user_id = ?
         GROUP BY call_domain, call_id
       ) m ON m.call_domain = c.call_domain AND m.call_id = c.call_id
       ${whereSql}
       ORDER BY c.time_start DESC, c.id DESC
       LIMIT ${parseInt(pageSize, 10)} OFFSET ${parseInt(offset, 10)}`,
      [userId, ...params]
    );

    return res.json({ success: true, data: rows || [], total });
  } catch (e) {
    if (DEBUG) console.warn('[ai.conversations.sessions] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load sessions' });
  }
});

// List outbound AI dialer call sessions (Pipecat dial-out) for the logged-in user.
// These use dialer_call_logs for call metadata and ai_call_messages for transcript turns.
app.get('/api/me/ai/outbound-conversations/sessions', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    if (!userId) return res.status(401).json({ success: false, message: 'Not authenticated' });

    const page = Math.max(0, parseInt(String(req.query.page || '0'), 10) || 0);
    const pageSizeRaw = parseInt(String(req.query.pageSize || '25'), 10) || 25;
    const pageSize = Math.max(1, Math.min(100, pageSizeRaw));
    const offset = page * pageSize;

    const agentIdRaw = String(req.query.agent_id || req.query.agentId || '').trim();
    const agentId = agentIdRaw ? agentIdRaw : '';

    // Exclude queued rows so the list reflects actual call attempts.
    const where = [`l.user_id = ?`, `l.call_id IS NOT NULL`, `l.call_id <> ''`, `(l.status IS NULL OR l.status <> 'queued')`];
    const params = [userId];

    if (agentId) {
      where.push('l.ai_agent_id = ?');
      params.push(agentId);
    }

    const whereSql = where.length ? ('WHERE ' + where.join(' AND ')) : '';

    const [cntRows] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM dialer_call_logs l
       ${whereSql}`,
      params
    );
    const total = Number(cntRows && cntRows[0] ? (cntRows[0].total || 0) : 0);

    const [rows] = await pool.query(
      `SELECT l.call_id,
              CONCAT('dialer-', l.campaign_id) AS call_domain,
              l.ai_agent_id AS agent_id,
              a.display_name AS agent_name,
              n.phone_number AS from_number,
              d.phone_number AS to_number,
              l.created_at AS time_start,
              l.duration_sec AS duration,
              l.status,
              l.result,
              COALESCE(m.msg_count, 0) AS message_count
       FROM dialer_call_logs l
       LEFT JOIN dialer_leads d ON d.id = l.lead_id
       LEFT JOIN ai_agents a ON a.id = l.ai_agent_id
       LEFT JOIN (
         SELECT user_id, agent_id, MIN(phone_number) AS phone_number
         FROM ai_numbers
         GROUP BY user_id, agent_id
       ) n ON n.user_id = l.user_id AND n.agent_id = l.ai_agent_id
       LEFT JOIN (
         SELECT call_domain, call_id, COUNT(*) AS msg_count
         FROM ai_call_messages
         WHERE user_id = ?
         GROUP BY call_domain, call_id
       ) m ON m.call_domain = CONCAT('dialer-', l.campaign_id) AND m.call_id = l.call_id
       ${whereSql}
       ORDER BY l.created_at DESC, l.id DESC
       LIMIT ${parseInt(pageSize, 10)} OFFSET ${parseInt(offset, 10)}`,
      [userId, ...params]
    );

    return res.json({ success: true, data: rows || [], total });
  } catch (e) {
    if (DEBUG) console.warn('[ai.outboundConversations.sessions] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load outbound sessions' });
  }
});

// List conversation messages for a single inbound session.
app.get('/api/me/ai/conversations/messages', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const callId = String(req.query.call_id || req.query.callId || '').trim();
    const callDomain = String(req.query.call_domain || req.query.callDomain || '').trim();
    if (!callId || !callDomain) {
      return res.status(400).json({ success: false, message: 'call_id and call_domain are required' });
    }

    const limitRaw = parseInt(String(req.query.limit || '1000'), 10) || 1000;
    const limit = Math.max(1, Math.min(5000, limitRaw));

    // Use query() instead of execute() for LIMIT to avoid MySQL prepared-statement edge cases.
    const [rows] = await pool.query(
      `SELECT id, role, content, created_at
       FROM ai_call_messages
       WHERE user_id = ? AND call_id = ? AND call_domain = ?
       ORDER BY id ASC
       LIMIT ${parseInt(limit, 10)}`,
      [userId, callId, callDomain]
    );

    return res.json({ success: true, data: rows || [] });
  } catch (e) {
    if (DEBUG) console.warn('[ai.conversations.messages] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load messages' });
  }
});

function parseDailyRoomPresenceCount(presence) {
  try {
    if (!presence) return null;

    const directTotal = presence.total_count ?? presence.totalCount ?? presence.count ?? presence.total;
    const n = Number(directTotal);
    if (Number.isFinite(n)) return n;

    const arr = Array.isArray(presence.data)
      ? presence.data
      : (Array.isArray(presence.participants)
        ? presence.participants
        : (Array.isArray(presence) ? presence : null));
    if (arr) return arr.length;

    const obj = (presence.data && typeof presence.data === 'object' && !Array.isArray(presence.data))
      ? presence.data
      : ((presence.participants && typeof presence.participants === 'object' && !Array.isArray(presence.participants))
        ? presence.participants
        : null);

    if (obj && typeof obj === 'object') return Object.keys(obj).length;
  } catch {}

  return null;
}

// Manually send a new AI video meeting link to an email address from the dashboard UI.
app.post('/api/me/ai/meetings/send', requireAuth, async (req, res) => {
  let sendRowId = null;
  const userId = req.session.userId;
  const meetingCost = AI_VIDEO_MEETING_LINK_COST;
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const agentIdRaw = (req.body?.agent_id !== undefined) ? req.body.agent_id : req.body?.agentId;
    const agentId = parseInt(String(agentIdRaw || '').trim(), 10);
    if (!Number.isFinite(agentId) || agentId <= 0) {
      return res.status(400).json({ success: false, message: 'agent_id is required' });
    }

    const [arows] = await pool.execute(
      'SELECT id, user_id, pipecat_agent_name FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1',
      [agentId, userId]
    );
    const agent = arows && arows[0];
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    const agentName = String(agent.pipecat_agent_name || '').trim();
    if (!agentName) return res.status(500).json({ success: false, message: 'Agent not configured' });

    const toEmail = String(req.body?.to_email || req.body?.toEmail || '').trim();
    if (!toEmail || !isValidEmail(toEmail)) {
      return res.status(400).json({ success: false, message: 'Valid to_email is required' });
    }

    const subjectRaw = String(req.body?.subject || '').trim();
    const subject = (subjectRaw ? subjectRaw.slice(0, 255) : 'Your video meeting link');

    const callId = String(req.body?.call_id || req.body?.callId || '').trim() || null;
    const callDomain = String(req.body?.call_domain || req.body?.callDomain || '').trim() || null;
    if (callId && callId.length > 64) {
      return res.status(400).json({ success: false, message: 'call_id is too long' });
    }
    if (callDomain && callDomain.length > 64) {
      return res.status(400).json({ success: false, message: 'call_domain is too long' });
    }

    let clientRequestId = String(req.body?.client_request_id || req.body?.clientRequestId || '').trim();
    if (!clientRequestId) {
      clientRequestId = crypto.randomBytes(16).toString('hex');
    }
    if (clientRequestId.length > 128) {
      return res.status(400).json({ success: false, message: 'client_request_id is too long' });
    }

    const dedupeKey = sha256(
      `manual-video-meeting-link|user:${userId}|agent:${agentId}|req:${clientRequestId}`
    );

    // Idempotency + audit row (reuses ai_email_sends with template_id NULL)
    try {
      const [ins] = await pool.execute(
        `INSERT INTO ai_email_sends (user_id, agent_id, template_id, dedupe_key, call_id, call_domain, to_email, subject, status, attempt_count)
         VALUES (?,?,?,?,?,?,?,?, 'pending', 1)`,
        [userId, agentId, null, dedupeKey, callId, callDomain, toEmail, subject]
      );
      sendRowId = ins.insertId;
    } catch (e) {
      if (e?.code !== 'ER_DUP_ENTRY') throw e;
      const [erows] = await pool.execute(
        'SELECT id, status, meeting_room_url, meeting_join_url FROM ai_email_sends WHERE dedupe_key = ? LIMIT 1',
        [dedupeKey]
      );
      const existing = erows && erows[0];
      if (existing && existing.status === 'completed') {
        return res.json({
          success: true,
          already_sent: true,
          dedupe_key: dedupeKey,
          room_url: existing.meeting_room_url || null,
          join_url: existing.meeting_join_url || null
        });
      }
      if (existing && existing.status === 'pending') {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
      if (existing && existing.status === 'failed') {
        sendRowId = existing.id;
        try {
          await pool.execute(
            'UPDATE ai_email_sends SET status = \'pending\', attempt_count = attempt_count + 1, error = NULL, to_email = ?, subject = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
            [toEmail, subject, sendRowId]
          );
        } catch {}
      } else {
        return res.status(202).json({ success: true, in_progress: true, dedupe_key: dedupeKey });
      }
    }

    if (meetingCost > 0) {
      const chargeDesc = `AI video meeting link (manual) - ${toEmail}`.substring(0, 255);
      const charge = await chargeAiEmailSendsRow({
        sendRowId,
        userId,
        amount: meetingCost,
        description: chargeDesc,
        label: 'ai.meeting'
      });

      if (!charge || charge.ok !== true) {
        const reason = charge?.reason || 'charge_failed';
        const short = (reason === 'insufficient_funds') ? 'Insufficient balance' : reason;
        const errMsg = `Meeting link charge failed: ${short}`;

        if (sendRowId) {
          try {
            await pool.execute(
              'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
              [errMsg, sendRowId]
            );
          } catch {}
        }

        return res.status(reason === 'insufficient_funds' ? 402 : 500).json({ success: false, message: errMsg });
      }
    }

    // Validate video avatar provider configuration for video meetings.
    const videoProvider = String(process.env.VIDEO_AVATAR_PROVIDER || 'heygen').trim().toLowerCase() || 'heygen';
    if (videoProvider === 'akool') {
      if (!process.env.AKOOL_API_KEY || !process.env.AKOOL_AVATAR_ID) {
        return res.status(500).json({ success: false, message: 'Akool is selected but AKOOL_API_KEY/AKOOL_AVATAR_ID are not configured' });
      }
    } else if (videoProvider === 'heygen') {
      if (!process.env.HEYGEN_API_KEY) {
        return res.status(500).json({ success: false, message: 'HeyGen is selected but HEYGEN_API_KEY is not configured' });
      }
    }

    // Start a new Pipecat session (web video meeting). Pipecat creates a Daily room.
    const startBody = {
      createDailyRoom: true,
      body: { mode: 'video_meeting' }
    };

    const startResp = await pipecatApiCall({
      method: 'POST',
      path: `/public/${encodeURIComponent(agentName)}/start`,
      body: startBody
    });

    const roomUrl = extractDailyRoomUrlFromPipecatStartResponse(startResp);
    if (!roomUrl) {
      throw new Error('Pipecat start response did not include a Daily room URL');
    }

    // Rooms created by Pipecat/Daily are typically private; Daily Prebuilt requires a meeting
    // token to join private rooms. Provide a join URL with ?t=<token> when available.
    const roomToken = extractDailyRoomTokenFromPipecatStartResponse(startResp);

    let joinUrl = roomUrl;
    if (roomToken) {
      try {
        const u = new URL(roomUrl);
        u.searchParams.set('t', roomToken);
        joinUrl = u.toString();
      } catch {}
    }

    // Store meeting link metadata for the dashboard UI
    const roomName = extractDailyRoomNameFromRoomUrl(roomUrl);
    if (sendRowId) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET meeting_provider = ?, meeting_room_name = ?, meeting_room_url = ?, meeting_join_url = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          ['daily', roomName || null, roomUrl, joinUrl, sendRowId]
        );
      } catch {}
    }

    const text = `Here is your video meeting link:\n\n${joinUrl}\n\nOpen it in your browser to join.`;
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
        <p>Here is your video meeting link:</p>
        <p><a href="${joinUrl}">${joinUrl}</a></p>
        <p>Open it in your browser to join.</p>
      </div>`;

    // Persist preview content (best-effort)
    if (sendRowId) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET email_text = ?, email_html = ?, attachments_json = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [text || null, html || null, sendRowId]
        );
      } catch {}
    }

    const info = await sendEmailViaUserSmtp({
      userId,
      toEmail,
      subject,
      text,
      html
    });

    const messageId = info && info.messageId ? String(info.messageId) : null;

    if (sendRowId) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET status = \'completed\', smtp_message_id = COALESCE(?, smtp_message_id), error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [messageId, sendRowId]
        );
      } catch {}
    }

    return res.json({
      success: true,
      already_sent: false,
      dedupe_key: dedupeKey,
      room_url: roomUrl,
      join_url: joinUrl
    });
  } catch (e) {
    const msg = e?.message || 'Failed to send video meeting link';
    if (DEBUG) console.warn('[ai.meetings.send] error:', e?.data || msg);

    // Best-effort refund if we already charged.
    if (sendRowId && pool && userId && meetingCost > 0) {
      try {
        await refundAiEmailSendsRow({
          sendRowId,
          userId,
          amount: meetingCost,
          description: `Refund: AI video meeting link failed (email_id ${sendRowId})`.substring(0, 255),
          label: 'ai.meeting'
        });
      } catch {}
    }

    if (sendRowId && pool) {
      try {
        await pool.execute(
          'UPDATE ai_email_sends SET status = \'failed\', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
          [msg, sendRowId]
        );
      } catch {}
    }
    return res.status(502).json({ success: false, message: msg });
  }
});

// List AI meeting-link sends (video meeting links) for the logged-in user.
app.get('/api/me/ai/meetings', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const page = Math.max(0, parseInt(String(req.query.page || '0'), 10) || 0);
    const pageSizeRaw = parseInt(String(req.query.pageSize || '25'), 10) || 25;
    const pageSize = Math.max(1, Math.min(100, pageSizeRaw));
    const offset = page * pageSize;

    const agentIdRaw = String(req.query.agent_id || req.query.agentId || '').trim();
    const agentId = agentIdRaw ? agentIdRaw : '';

    const where = [`e.user_id = ?`, `e.meeting_provider IS NOT NULL`];
    const params = [userId];

    if (agentId) {
      where.push('e.agent_id = ?');
      params.push(agentId);
    }

    const whereSql = where.length ? ('WHERE ' + where.join(' AND ')) : '';

    const [cntRows] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM ai_email_sends e
       ${whereSql}`,
      params
    );
    const total = Number(cntRows && cntRows[0] ? (cntRows[0].total || 0) : 0);

    const [rows] = await pool.query(
      `SELECT e.id, e.created_at, e.updated_at,
              e.agent_id,
              a.display_name AS agent_name,
              e.to_email, e.subject,
              e.status AS email_status,
              e.meeting_provider, e.meeting_room_name, e.meeting_room_url, e.meeting_join_url
       FROM ai_email_sends e
       LEFT JOIN ai_agents a ON a.id = e.agent_id
       ${whereSql}
       ORDER BY e.created_at DESC, e.id DESC
       LIMIT ${parseInt(pageSize, 10)} OFFSET ${parseInt(offset, 10)}`,
      params
    );

    const out = [];

    for (const r of rows || []) {
      const row = { ...r };
      row.meeting_live = null;
      row.meeting_status = 'unknown';
      row.participant_count = null;

      const provider = String(r?.meeting_provider || '').toLowerCase();
      if (provider === 'daily') {
        const roomName = String(r?.meeting_room_name || '').trim() || extractDailyRoomNameFromRoomUrl(r?.meeting_room_url);
        if (roomName) {
          try {
            const presence = await dailyApiCall({
              method: 'GET',
              path: `/rooms/${encodeURIComponent(roomName)}/presence`
            });
            const count = parseDailyRoomPresenceCount(presence);
            if (count != null) {
              row.participant_count = count;
              row.meeting_live = count > 0;
              row.meeting_status = row.meeting_live ? 'live' : 'ended';
            }
          } catch (e) {
            // If the room doesn't exist anymore, treat as ended.
            if (e && (e.status === 404 || e.status === 410)) {
              row.meeting_live = false;
              row.meeting_status = 'ended';
              row.participant_count = 0;
            }
          }
        }
      }

      out.push(row);
    }

    return res.json({ success: true, data: out, total });
  } catch (e) {
    if (DEBUG) console.warn('[ai.meetings.list] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load meetings' });
  }
});

// List AI emails sent by the agent for the logged-in user.
app.get('/api/me/ai/emails', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const page = Math.max(0, parseInt(String(req.query.page || '0'), 10) || 0);
    const pageSizeRaw = parseInt(String(req.query.pageSize || '25'), 10) || 25;
    const pageSize = Math.max(1, Math.min(100, pageSizeRaw));
    const offset = page * pageSize;

    const agentIdRaw = String(req.query.agent_id || req.query.agentId || '').trim();
    const agentId = agentIdRaw ? agentIdRaw : '';

    const statusRaw = String(req.query.status || '').trim().toLowerCase();
    let status = '';
    if (statusRaw) {
      if (!['pending', 'completed', 'failed'].includes(statusRaw)) {
        return res.status(400).json({ success: false, message: 'status must be one of: pending, completed, failed' });
      }
      status = statusRaw;
    }

    const where = [`e.user_id = ?`];
    const params = [userId];

    if (agentId) {
      where.push('e.agent_id = ?');
      params.push(agentId);
    }

    if (status) {
      where.push('e.status = ?');
      params.push(status);
    }

    const whereSql = where.length ? ('WHERE ' + where.join(' AND ')) : '';

    const [cntRows] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM ai_email_sends e
       ${whereSql}`,
      params
    );
    const total = Number(cntRows && cntRows[0] ? (cntRows[0].total || 0) : 0);

    const [rows] = await pool.query(
      `SELECT e.id, e.created_at, e.updated_at,
              e.agent_id,
              a.display_name AS agent_name,
              e.to_email, e.subject,
              e.status,
              e.template_id,
              e.meeting_provider, e.meeting_room_name, e.meeting_room_url, e.meeting_join_url,
              e.error
       FROM ai_email_sends e
       LEFT JOIN ai_agents a ON a.id = e.agent_id
       ${whereSql}
       ORDER BY e.created_at DESC, e.id DESC
       LIMIT ${parseInt(pageSize, 10)} OFFSET ${parseInt(offset, 10)}`,
      params
    );

    return res.json({ success: true, data: rows || [], total });
  } catch (e) {
    if (DEBUG) console.warn('[ai.emails.list] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load emails' });
  }
});

// Get one AI email send row (for preview) for the logged-in user.
app.get('/api/me/ai/emails/:id', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const id = parseInt(String(req.params.id || '').trim(), 10);
    if (!Number.isFinite(id) || id <= 0) {
      return res.status(400).json({ success: false, message: 'Invalid email id' });
    }

    const [rows] = await pool.execute(
      `SELECT e.id, e.created_at, e.updated_at,
              e.agent_id,
              a.display_name AS agent_name,
              e.to_email, e.subject,
              e.status, e.attempt_count, e.error,
              e.template_id,
              e.meeting_provider, e.meeting_room_name, e.meeting_room_url, e.meeting_join_url,
              e.email_text, e.email_html, e.attachments_json, e.smtp_message_id
       FROM ai_email_sends e
       LEFT JOIN ai_agents a ON a.id = e.agent_id
       WHERE e.id = ? AND e.user_id = ?
       LIMIT 1`,
      [id, userId]
    );

    const row = rows && rows[0];
    if (!row) {
      return res.status(404).json({ success: false, message: 'Email not found' });
    }

    return res.json({ success: true, data: row });
  } catch (e) {
    if (DEBUG) console.warn('[ai.emails.get] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load email' });
  }
});

// List AI physical mail sends (Click2Mail) for the logged-in user.
app.get('/api/me/ai/mail', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const page = Math.max(0, parseInt(String(req.query.page || '0'), 10) || 0);
    const pageSizeRaw = parseInt(String(req.query.pageSize || '25'), 10) || 25;
    const pageSize = Math.max(1, Math.min(100, pageSizeRaw));
    const offset = page * pageSize;

    const agentIdRaw = String(req.query.agent_id || req.query.agentId || '').trim();
    const agentId = agentIdRaw ? agentIdRaw : '';

    const refresh = String(req.query.refresh || '0') !== '0';

    const where = [`m.user_id = ?`];
    const params = [userId];

    if (agentId) {
      where.push('m.agent_id = ?');
      params.push(agentId);
    }

    const whereSql = where.length ? ('WHERE ' + where.join(' AND ')) : '';

    const [cntRows] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM ai_mail_sends m
       ${whereSql}`,
      params
    );
    const total = Number(cntRows && cntRows[0] ? (cntRows[0].total || 0) : 0);

    const [rows] = await pool.query(
      `SELECT m.id, m.created_at, m.updated_at,
              m.agent_id,
              a.display_name AS agent_name,
              m.to_name, m.to_organization, m.to_address1, m.to_address2, m.to_address3,
              m.to_city, m.to_state, m.to_postal_code, m.to_country,
              m.corrected_to_name, m.corrected_to_organization, m.corrected_to_address1, m.corrected_to_address2, m.corrected_to_address3,
              m.corrected_to_city, m.corrected_to_state, m.corrected_to_postal_code, m.corrected_to_country,
              m.product, m.mail_class,
              m.batch_id, m.tracking_type, m.tracking_number,
              m.tracking_status, m.tracking_status_date, m.tracking_status_location,
              m.status, m.cost_estimate, m.markup_amount, m.total_cost,
              m.billing_history_id,
              m.refund_status, m.refund_amount, m.refund_billing_history_id, m.refund_error, m.refunded_at,
              m.error
       FROM ai_mail_sends m
       LEFT JOIN ai_agents a ON a.id = m.agent_id
       ${whereSql}
       ORDER BY m.created_at DESC, m.id DESC
       LIMIT ${parseInt(pageSize, 10)} OFFSET ${parseInt(offset, 10)}`,
      params
    );

    const out = [];

    // Optional refresh: poll Click2Mail for the rows on this page (best-effort).
    if (refresh) {
      for (const r of rows || []) {
        const row = { ...r };

        const batchId = String(r?.batch_id || '').trim();
        const provider = String(r?.provider || 'click2mail').toLowerCase();

        if (provider === 'click2mail' && batchId) {
          try {
            // Batch status
            const stResp = await click2mailBatchStatus(batchId);
            const st = extractClick2MailBatchStatus(stResp.parsed);
            const hasErrors = parseBool01(st?.hasErrors);
            const completed = parseBool01(st?.completed);

            if (completed === true) {
              row.status = hasErrors ? 'failed' : 'completed';
            } else {
              // Keep existing status unless it is still pending
              if (row.status === 'pending') row.status = 'submitted';
            }

            // Batch tracking
            const tt = String(r?.tracking_type || CLICK2MAIL_DEFAULT_TRACKING_TYPE || 'impb').trim();
            const tr = await click2mailBatchTracking(batchId, tt);
            const latest = pickLatestTrackingItem(tr?.extracted?.items);

            if (latest && latest.barCode) {
              row.tracking_number = String(latest.barCode).trim();
              row.tracking_status = String(latest.status || '').trim() || null;
              row.tracking_status_location = String(latest.statusLocation || '').trim() || null;
              row.tracking_status_date = parseClick2MailDateTimeToMysql(latest.dateTime);
            }

            // Persist updated fields (best-effort)
            try {
              await pool.execute(
                `UPDATE ai_mail_sends
                 SET status = ?, tracking_number = ?, tracking_status = ?, tracking_status_date = ?, tracking_status_location = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND user_id = ? LIMIT 1`,
                [
                  row.status,
                  row.tracking_number || null,
                  row.tracking_status || null,
                  row.tracking_status_date || null,
                  row.tracking_status_location || null,
                  row.id,
                  userId
                ]
              );
            } catch {}
          } catch (e) {
            // Ignore refresh failures; still return existing row.
          }
        }

        out.push(row);
      }

      return res.json({ success: true, data: out, total });
    }

    return res.json({ success: true, data: rows || [], total });
  } catch (e) {
    if (DEBUG) console.warn('[ai.mail.list] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load mail' });
  }
});

// Create agent
app.post('/api/me/ai/agents', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const {
      display_name,
      greeting,
      prompt,
      cartesia_voice_id,
      cartesiaVoiceId,
      default_doc_template_id,
      defaultDocTemplateId,
      background_audio_url,
      backgroundAudioUrl,
      background_audio_gain,
      backgroundAudioGain,
      transfer_to_number,
      transferToNumber
    } = req.body || {};

    const name = String(display_name || '').trim();
    const voiceIdRaw = (cartesia_voice_id != null ? cartesia_voice_id : cartesiaVoiceId);
    const cartesiaVoiceIdClean = (voiceIdRaw != null ? String(voiceIdRaw) : '').trim();

    // Optional: per-agent transfer destination (override). If empty, falls back to user default.
    const transferRaw = (transfer_to_number !== undefined) ? transfer_to_number : transferToNumber;
    const transferNorm = normalizeAiTransferDestination(transferRaw);
    if (transferNorm && !isValidAiTransferDestination(transferNorm)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid transfer destination. Use E.164 phone format like +18005551234 or a SIP URI like sip:agent@domain.'
      });
    }
    if (transferNorm && transferNorm.length > 64) {
      return res.status(400).json({ success: false, message: 'Transfer destination is too long' });
    }

    // Optional: per-agent background ambience
    const bgUrlRaw = (background_audio_url != null ? background_audio_url : backgroundAudioUrl);
    let bgUrl = (bgUrlRaw != null ? String(bgUrlRaw) : '').trim();
    if (bgUrl) {
      try {
        const u = new URL(bgUrl);
        if (u.protocol !== 'https:') {
          return res.status(400).json({ success: false, message: 'Background audio URL must start with https://' });
        }
        bgUrl = u.toString();
      } catch {
        return res.status(400).json({ success: false, message: 'Background audio URL is invalid' });
      }
      if (bgUrl.length > 512) {
        return res.status(400).json({ success: false, message: 'Background audio URL is too long' });
      }
    }

    const bgGainRaw = (background_audio_gain != null ? background_audio_gain : backgroundAudioGain);
    let bgGain = null;
    if (bgGainRaw != null && String(bgGainRaw).trim()) {
      const g = parseFloat(String(bgGainRaw));
      if (!Number.isFinite(g)) {
        return res.status(400).json({ success: false, message: 'Background audio gain must be a number' });
      }
      if (g < 0 || g > 0.2) {
        return res.status(400).json({ success: false, message: 'Background audio gain must be between 0.0 and 0.2' });
      }
      bgGain = g;
    } else if (bgUrl) {
      // Default gain when a URL is provided.
      bgGain = 0.06;
    }

    if (!name) return res.status(400).json({ success: false, message: 'display_name is required' });

    // Optional: default document template
    const defaultTplRaw = (default_doc_template_id !== undefined) ? default_doc_template_id : defaultDocTemplateId;
    let defaultTplId = null;
    if (defaultTplRaw != null && String(defaultTplRaw).trim()) {
      const tid = parseInt(String(defaultTplRaw).trim(), 10);
      if (!Number.isFinite(tid) || tid <= 0) {
        return res.status(400).json({ success: false, message: 'default_doc_template_id is invalid' });
      }
      const [trows] = await pool.execute('SELECT id FROM ai_doc_templates WHERE id = ? AND user_id = ? LIMIT 1', [tid, userId]);
      if (!trows || !trows.length) {
        return res.status(404).json({ success: false, message: 'Template not found' });
      }
      defaultTplId = tid;
    }

    const [[cnt]] = await pool.execute('SELECT COUNT(*) AS total FROM ai_agents WHERE user_id = ?', [userId]);
    const total = Number(cnt?.total || 0);
    if (total >= AI_MAX_AGENTS) {
      return res.status(400).json({ success: false, message: `You can create up to ${AI_MAX_AGENTS} agents.` });
    }

    const slug = slugifyName(name);
    const rand = crypto.randomBytes(3).toString('hex');
    const agentName = `u${userId}-${slug}-${rand}`;
    const secretSetName = buildPipecatSecretSetName(agentName);

    // Optional: per-agent action token used by the agent runtime to call back into this portal
    // (e.g., sending templated documents via the customer's SMTP settings).
    let actionTokenPlain = '';
    let actionTokenHash = null;
    let actionTokenEnc = null;
    let actionTokenIv = null;
    let actionTokenTag = null;

    try {
      const key = getUserSmtpEncryptionKey();
      if (key) {
        actionTokenPlain = crypto.randomBytes(32).toString('base64url');
        actionTokenHash = sha256(actionTokenPlain);
        const enc = encryptAes256Gcm(actionTokenPlain, key);
        actionTokenEnc = enc.enc;
        actionTokenIv = enc.iv;
        actionTokenTag = enc.tag;
      } else if (DEBUG) {
        console.warn('[ai.agents.create] USER_SMTP_ENCRYPTION_KEY not configured; agent action token will not be set');
      }
    } catch (e) {
      if (DEBUG) console.warn('[ai.agents.create] Failed to generate/encrypt agent action token:', e.message || e);
    }

    // Provision Pipecat secret set + agent
    try {
      const userTransfer = await getUserAiTransferDestination(userId);
      const operatorNumber = transferNorm || normalizeAiTransferDestination(userTransfer);
      const secrets = buildPipecatSecrets({
        greeting,
        prompt,
        cartesiaVoiceId: cartesiaVoiceIdClean,
        portalAgentActionToken: actionTokenPlain,
        operatorNumber,
        backgroundAudioUrl: bgUrl,
        backgroundAudioGain: bgGain
      });
      await pipecatUpsertSecretSet(secretSetName, secrets, PIPECAT_REGION);
      await pipecatCreateAgent({ agentName, secretSetName });
    } catch (provErr) {
      // Best-effort cleanup
      try { await pipecatDeleteSecretSet(secretSetName); } catch {}
      const msg = provErr?.message || 'Failed to provision agent';
      return res.status(502).json({ success: false, message: msg, error: provErr?.data });
    }

    // Persist
    const [ins] = await pool.execute(
      'INSERT INTO ai_agents (user_id, name, display_name, greeting, prompt, cartesia_voice_id, background_audio_url, background_audio_gain, pipecat_agent_name, pipecat_secret_set, pipecat_region, agent_action_token_hash, agent_action_token_enc, agent_action_token_iv, agent_action_token_tag, transfer_to_number, default_doc_template_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [
        userId,
        name,
        name,
        String(greeting || ''),
        String(prompt || ''),
        cartesiaVoiceIdClean || null,
        bgUrl || null,
        bgGain,
        agentName,
        secretSetName,
        PIPECAT_REGION,
        actionTokenHash,
        actionTokenEnc,
        actionTokenIv,
        actionTokenTag,
        transferNorm ? String(transferNorm) : null,
        defaultTplId
      ]
    );

    return res.json({
      success: true,
      data: {
        id: ins.insertId,
        display_name: name,
        greeting: String(greeting || ''),
        prompt: String(prompt || ''),
        cartesia_voice_id: cartesiaVoiceIdClean || null,
        pipecat_agent_name: agentName,
        pipecat_secret_set: secretSetName,
        pipecat_region: PIPECAT_REGION
      }
    });
  } catch (e) {
    if (DEBUG) console.error('[ai.agents.create] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to create agent' });
  }
});

// Update agent
app.patch('/api/me/ai/agents/:id', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const agentId = req.params.id;
    const {
      display_name,
      greeting,
      prompt,
      cartesia_voice_id,
      cartesiaVoiceId,
      default_doc_template_id,
      defaultDocTemplateId,
      background_audio_url,
      backgroundAudioUrl,
      background_audio_gain,
      backgroundAudioGain,
      transfer_to_number,
      transferToNumber,
      inbound_transfer_enabled,
      inboundTransferEnabled,
      inbound_transfer_number,
      inboundTransferNumber
    } = req.body || {};

    const [rows] = await pool.execute('SELECT * FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1', [agentId, userId]);
    const agent = rows && rows[0];
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    const nextName = display_name != null ? String(display_name || '').trim() : agent.display_name;
    const nextGreeting = greeting != null ? String(greeting || '') : (agent.greeting || '');
    const nextPrompt = prompt != null ? String(prompt || '') : (agent.prompt || '');

    // Optional: per-agent transfer destination (override)
    const transferRaw = (transfer_to_number !== undefined) ? transfer_to_number : transferToNumber;
    const hasTransferUpdate = transferRaw !== undefined;
    let nextTransfer = agent.transfer_to_number != null ? String(agent.transfer_to_number) : '';
    if (hasTransferUpdate) {
      const norm = normalizeAiTransferDestination(transferRaw);
      if (norm && !isValidAiTransferDestination(norm)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid transfer destination. Use E.164 phone format like +18005551234 or a SIP URI like sip:agent@domain.'
        });
      }
      if (norm && norm.length > 64) {
        return res.status(400).json({ success: false, message: 'Transfer destination is too long' });
      }
      nextTransfer = norm;
    }
    const voiceIdRaw = (cartesia_voice_id != null ? cartesia_voice_id : cartesiaVoiceId);
    const nextVoiceId = voiceIdRaw != null ? String(voiceIdRaw).trim() : (agent.cartesia_voice_id || '');
    const nextVoiceIdClean = nextVoiceId ? String(nextVoiceId).trim() : '';

    // Optional: per-agent background ambience
    const bgUrlRaw = (background_audio_url !== undefined) ? background_audio_url : backgroundAudioUrl;
    const hasBgUrlUpdate = bgUrlRaw !== undefined;
    let nextBgUrl = agent.background_audio_url ? String(agent.background_audio_url) : '';
    if (hasBgUrlUpdate) {
      let v = (bgUrlRaw != null ? String(bgUrlRaw) : '').trim();
      if (v) {
        try {
          const u = new URL(v);
          if (u.protocol !== 'https:') {
            return res.status(400).json({ success: false, message: 'Background audio URL must start with https://' });
          }
          v = u.toString();
        } catch {
          return res.status(400).json({ success: false, message: 'Background audio URL is invalid' });
        }
        if (v.length > 512) {
          return res.status(400).json({ success: false, message: 'Background audio URL is too long' });
        }
      }
      nextBgUrl = v;
    }

    const bgGainRaw = (background_audio_gain !== undefined) ? background_audio_gain : backgroundAudioGain;
    const hasBgGainUpdate = bgGainRaw !== undefined;
    let nextBgGain = (agent.background_audio_gain != null) ? Number(agent.background_audio_gain) : null;
    if (hasBgGainUpdate) {
      const s = String(bgGainRaw == null ? '' : bgGainRaw).trim();
      if (!s) {
        nextBgGain = null;
      } else {
        const g = parseFloat(s);
        if (!Number.isFinite(g)) {
          return res.status(400).json({ success: false, message: 'Background audio gain must be a number' });
        }
        if (g < 0 || g > 0.2) {
          return res.status(400).json({ success: false, message: 'Background audio gain must be between 0.0 and 0.2' });
        }
        nextBgGain = g;
      }
    }

    if (nextBgUrl && nextBgGain == null) {
      nextBgGain = 0.06;
    }

    if (!nextName) return res.status(400).json({ success: false, message: 'display_name cannot be empty' });

    // Optional: update default template
    const defaultTplRaw = (default_doc_template_id !== undefined) ? default_doc_template_id : defaultDocTemplateId;
    let nextDefaultTpl = agent.default_doc_template_id;
    if (defaultTplRaw !== undefined) {
      const s = String(defaultTplRaw == null ? '' : defaultTplRaw).trim();
      if (!s) {
        nextDefaultTpl = null;
      } else {
        const tid = parseInt(s, 10);
        if (!Number.isFinite(tid) || tid <= 0) {
          return res.status(400).json({ success: false, message: 'default_doc_template_id is invalid' });
        }
        const [trows] = await pool.execute(
          'SELECT id FROM ai_doc_templates WHERE id = ? AND user_id = ? LIMIT 1',
          [tid, userId]
        );
        if (!trows || !trows.length) {
          return res.status(404).json({ success: false, message: 'Template not found' });
        }
        nextDefaultTpl = tid;
      }
    }

    // Optional: inbound direct transfer settings
    const inboundTransferEnabledRaw = (inbound_transfer_enabled !== undefined) ? inbound_transfer_enabled : inboundTransferEnabled;
    let nextInboundTransferEnabled = agent.inbound_transfer_enabled != null ? Number(agent.inbound_transfer_enabled) : 0;
    if (inboundTransferEnabledRaw !== undefined) {
      nextInboundTransferEnabled = inboundTransferEnabledRaw ? 1 : 0;
    }

    const inboundTransferNumberRaw = (inbound_transfer_number !== undefined) ? inbound_transfer_number : inboundTransferNumber;
    let nextInboundTransferNumber = agent.inbound_transfer_number || '';
    if (inboundTransferNumberRaw !== undefined) {
      const norm = normalizeAiTransferDestination(inboundTransferNumberRaw);
      if (norm && !isValidAiTransferDestination(norm)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid inbound transfer number. Use E.164 phone format like +18005551234 or a SIP URI like sip:agent@domain.'
        });
      }
      if (norm && norm.length > 32) {
        return res.status(400).json({ success: false, message: 'Inbound transfer number is too long (max 32 chars)' });
      }
      nextInboundTransferNumber = norm;
    }

    // If inbound transfer is enabled, require a number
    if (nextInboundTransferEnabled && !nextInboundTransferNumber) {
      return res.status(400).json({ success: false, message: 'inbound_transfer_number is required when inbound transfer is enabled' });
    }

    await pool.execute(
      `UPDATE ai_agents SET
         display_name = ?,
         greeting = ?,
         prompt = ?,
         cartesia_voice_id = ?,
         background_audio_url = ?,
         background_audio_gain = ?,
         transfer_to_number = ?,
         default_doc_template_id = ?,
         inbound_transfer_enabled = ?,
         inbound_transfer_number = ?
       WHERE id = ? AND user_id = ?`,
      [
        nextName,
        nextGreeting,
        nextPrompt,
        nextVoiceIdClean || null,
        nextBgUrl || null,
        nextBgGain,
        nextTransfer ? String(nextTransfer) : null,
        nextDefaultTpl,
        nextInboundTransferEnabled,
        nextInboundTransferNumber || null,
        agentId,
        userId
      ]
    );

    // Update secret set + redeploy
    try {
      const portalToken = await getOrCreateAgentActionTokenPlain({
        agentRow: agent,
        userId,
        label: 'ai.agents.update'
      });

      const userTransfer = await getUserAiTransferDestination(userId);
      const operatorNumber = normalizeAiTransferDestination(nextTransfer) || normalizeAiTransferDestination(userTransfer);
      const publicBaseUrl = getPublicBaseUrlFromReq(req);
      const bgUrlResolved = await resolveAgentBackgroundAudioUrl({
        userId,
        agentId,
        fallbackUrl: nextBgUrl,
        publicBaseUrl
      });

      const secrets = buildPipecatSecrets({
        greeting: nextGreeting,
        prompt: nextPrompt,
        cartesiaVoiceId: nextVoiceIdClean,
        portalAgentActionToken: portalToken,
        operatorNumber,
        backgroundAudioUrl: bgUrlResolved,
        backgroundAudioGain: nextBgGain
      });
      await pipecatUpsertSecretSet(agent.pipecat_secret_set, secrets, agent.pipecat_region || PIPECAT_REGION);
      await pipecatUpdateAgent({
        agentName: agent.pipecat_agent_name,
        secretSetName: agent.pipecat_secret_set,
        regionOverride: agent.pipecat_region || PIPECAT_REGION
      });
    } catch (provErr) {
      const msg = provErr?.message || 'Failed to update agent in Pipecat';
      return res.status(502).json({ success: false, message: msg, error: provErr?.data });
    }

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.agents.update] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to update agent' });
  }
});

// Upload/replace per-agent background audio (WAV)
app.post('/api/me/ai/agents/:id/background-audio', requireAuth, aiAgentBackgroundAudioUpload.single('file'), async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const agentId = String(req.params.id || '').trim();
    if (!agentId) return res.status(400).json({ success: false, message: 'Missing agent id' });

    const [rows] = await pool.execute('SELECT * FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1', [agentId, userId]);
    const agent = rows && rows[0];
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    const file = req.file;
    if (!file || !file.buffer) {
      return res.status(400).json({ success: false, message: 'Missing file upload (field name: file)' });
    }

    if (!looksLikeWav(file.buffer)) {
      return res.status(400).json({ success: false, message: 'Uploaded file must be a WAV file (RIFF/WAVE)' });
    }

    const token = crypto.randomBytes(32).toString('hex');
    const original = String(file.originalname || '').trim() || null;
    const mime = String(file.mimetype || '').trim() || 'audio/wav';
    const sizeBytes = file.size != null ? Number(file.size) : (file.buffer ? file.buffer.length : null);

    await pool.execute(
      `INSERT INTO ai_agent_background_audio (user_id, agent_id, original_filename, mime_type, size_bytes, access_token, wav_blob)
       VALUES (?,?,?,?,?,?,?)
       ON DUPLICATE KEY UPDATE
         original_filename=VALUES(original_filename),
         mime_type=VALUES(mime_type),
         size_bytes=VALUES(size_bytes),
         access_token=VALUES(access_token),
         wav_blob=VALUES(wav_blob),
         updated_at=CURRENT_TIMESTAMP`,
      [userId, agentId, original, mime, sizeBytes, token, file.buffer]
    );

    // Update secret set + redeploy so the agent starts using the uploaded WAV
    try {
      const portalToken = await getOrCreateAgentActionTokenPlain({
        agentRow: agent,
        userId,
        label: 'ai.agents.background-audio.upload'
      });

      const userTransfer = await getUserAiTransferDestination(userId);
      const operatorNumber = normalizeAiTransferDestination(agent.transfer_to_number) || normalizeAiTransferDestination(userTransfer);
      const publicBaseUrl = getPublicBaseUrlFromReq(req);
      const audioUrl = buildAgentBackgroundAudioPublicUrl({ baseUrl: publicBaseUrl, agentId, token });

      const secrets = buildPipecatSecrets({
        greeting: agent.greeting,
        prompt: agent.prompt,
        cartesiaVoiceId: agent.cartesia_voice_id,
        portalAgentActionToken: portalToken,
        operatorNumber,
        backgroundAudioUrl: audioUrl,
        backgroundAudioGain: agent.background_audio_gain
      });

      await pipecatUpsertSecretSet(agent.pipecat_secret_set, secrets, agent.pipecat_region || PIPECAT_REGION);
      await pipecatUpdateAgent({
        agentName: agent.pipecat_agent_name,
        secretSetName: agent.pipecat_secret_set,
        regionOverride: agent.pipecat_region || PIPECAT_REGION
      });
    } catch (provErr) {
      const msg = provErr?.message || 'Failed to update agent in Pipecat';
      return res.status(502).json({ success: false, message: msg, error: provErr?.data });
    }

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.agents.background-audio.upload] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to upload background audio' });
  }
});

// Delete uploaded background audio for an agent
app.delete('/api/me/ai/agents/:id/background-audio', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const agentId = String(req.params.id || '').trim();
    if (!agentId) return res.status(400).json({ success: false, message: 'Missing agent id' });

    const [rows] = await pool.execute('SELECT * FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1', [agentId, userId]);
    const agent = rows && rows[0];
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    await pool.execute('DELETE FROM ai_agent_background_audio WHERE user_id = ? AND agent_id = ?', [userId, agentId]);

    // Update secret set + redeploy so the agent stops using the uploaded WAV
    try {
      const portalToken = await getOrCreateAgentActionTokenPlain({
        agentRow: agent,
        userId,
        label: 'ai.agents.background-audio.delete'
      });

      const userTransfer = await getUserAiTransferDestination(userId);
      const operatorNumber = normalizeAiTransferDestination(agent.transfer_to_number) || normalizeAiTransferDestination(userTransfer);
      const publicBaseUrl = getPublicBaseUrlFromReq(req);
      const bgUrlResolved = await resolveAgentBackgroundAudioUrl({
        userId,
        agentId,
        fallbackUrl: agent.background_audio_url,
        publicBaseUrl
      });

      const secrets = buildPipecatSecrets({
        greeting: agent.greeting,
        prompt: agent.prompt,
        cartesiaVoiceId: agent.cartesia_voice_id,
        portalAgentActionToken: portalToken,
        operatorNumber,
        backgroundAudioUrl: bgUrlResolved,
        backgroundAudioGain: agent.background_audio_gain
      });

      await pipecatUpsertSecretSet(agent.pipecat_secret_set, secrets, agent.pipecat_region || PIPECAT_REGION);
      await pipecatUpdateAgent({
        agentName: agent.pipecat_agent_name,
        secretSetName: agent.pipecat_secret_set,
        regionOverride: agent.pipecat_region || PIPECAT_REGION
      });
    } catch (provErr) {
      const msg = provErr?.message || 'Failed to update agent in Pipecat';
      return res.status(502).json({ success: false, message: msg, error: provErr?.data });
    }

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.agents.background-audio.delete] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to delete background audio' });
  }
});

// Public: serve uploaded background audio via token (used by the Pipecat agent runtime)
app.get('/api/public/ai/agents/:id/background-audio.wav', async (req, res) => {
  try {
    if (!pool) return res.status(404).send('Not found');
    const agentId = String(req.params.id || '').trim();
    const token = String(req.query.token || '').trim();
    if (!agentId || !token) return res.status(404).send('Not found');

    const [rows] = await pool.execute(
      'SELECT original_filename, size_bytes, wav_blob FROM ai_agent_background_audio WHERE agent_id = ? AND access_token = ? LIMIT 1',
      [agentId, token]
    );
    const row = rows && rows[0] ? rows[0] : null;
    if (!row || !row.wav_blob) return res.status(404).send('Not found');

    const buf = row.wav_blob;
    const name = row.original_filename ? String(row.original_filename) : '';
    const safeName = (name ? name.replace(/[^a-z0-9_.-]+/gi, '_').slice(0, 120) : 'background.wav') || 'background.wav';

    res.set('Content-Type', 'audio/wav');
    res.set('Cache-Control', 'no-store');
    if (row.size_bytes != null) res.set('Content-Length', String(row.size_bytes));
    res.set('Content-Disposition', `inline; filename="${safeName}"`);

    return res.status(200).send(buf);
  } catch (e) {
    if (DEBUG) console.warn('[ai.public.background-audio] error:', e.message || e);
    return res.status(404).send('Not found');
  }
});

// Redeploy agent (force restart by deleting + recreating the Pipecat agent with the same name + secrets)
app.post('/api/me/ai/agents/:id/redeploy', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const agentId = String(req.params.id || '').trim();
    if (!agentId) return res.status(400).json({ success: false, message: 'Missing agent id' });

    const [rows] = await pool.execute('SELECT * FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1', [agentId, userId]);
    const agent = rows && rows[0];
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    const agentName = String(agent.pipecat_agent_name || '').trim();
    const secretSetName = String(agent.pipecat_secret_set || '').trim();
    const region = String(agent.pipecat_region || PIPECAT_REGION || 'us-west');

    if (!agentName || !secretSetName) {
      return res.status(500).json({ success: false, message: 'Agent not configured' });
    }

    try {
      const portalToken = await getOrCreateAgentActionTokenPlain({
        agentRow: agent,
        userId,
        label: 'ai.agents.redeploy'
      });

      const userTransfer = await getUserAiTransferDestination(userId);
      const operatorNumber = normalizeAiTransferDestination(agent.transfer_to_number) || normalizeAiTransferDestination(userTransfer);
      const publicBaseUrl = getPublicBaseUrlFromReq(req);
      const bgUrlResolved = await resolveAgentBackgroundAudioUrl({
        userId,
        agentId,
        fallbackUrl: agent.background_audio_url,
        publicBaseUrl
      });

      const secrets = buildPipecatSecrets({
        greeting: agent.greeting,
        prompt: agent.prompt,
        cartesiaVoiceId: agent.cartesia_voice_id,
        portalAgentActionToken: portalToken,
        operatorNumber,
        backgroundAudioUrl: bgUrlResolved,
        backgroundAudioGain: agent.background_audio_gain
      });

      // Ensure the secret set exists/updated
      await pipecatUpsertSecretSet(secretSetName, secrets, region);

      // Force a restart by deleting + recreating the Pipecat agent service.
      await pipecatDeleteAgent(agentName);
      await pipecatCreateAgent({ agentName, secretSetName, regionOverride: region });
    } catch (provErr) {
      const msg = provErr?.message || 'Failed to redeploy agent in Pipecat';
      return res.status(502).json({ success: false, message: msg, error: provErr?.data });
    }

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.agents.redeploy] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to redeploy agent' });
  }
});

// Delete agent
app.delete('/api/me/ai/agents/:id', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const agentId = req.params.id;

    const [rows] = await pool.execute('SELECT * FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1', [agentId, userId]);
    const agent = rows && rows[0];
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    // Unassign any number first (and remove Daily dial-in config)
    const [nums] = await pool.execute('SELECT id, dialin_config_id FROM ai_numbers WHERE user_id = ? AND agent_id = ?', [userId, agentId]);
    for (const n of nums || []) {
      try { await dailyDeleteDialinConfig(n.dialin_config_id); } catch (e) { if (DEBUG) console.warn('[ai.agent.delete] dialin delete failed', e.message || e); }
      try { await pool.execute('UPDATE ai_numbers SET agent_id = NULL, dialin_config_id = NULL WHERE id = ? AND user_id = ?', [n.id, userId]); } catch {}
    }

    // Delete Pipecat agent + secret set
    try {
      await pipecatDeleteAgent(agent.pipecat_agent_name);
      await pipecatDeleteSecretSet(agent.pipecat_secret_set);
    } catch (provErr) {
      const msg = provErr?.message || 'Failed to delete agent in Pipecat';
      return res.status(502).json({ success: false, message: msg, error: provErr?.data });
    }

    await pool.execute('DELETE FROM ai_agents WHERE id = ? AND user_id = ?', [agentId, userId]);
    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.agents.delete] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to delete agent' });
  }
});

// List numbers
app.get('/api/me/ai/numbers', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    // If the user's balance is NEGATIVE, auto-unassign AI numbers from agents.
    // (Keeps numbers owned by the user, but removes routing/assignment until balance is positive.)
    try {
      const balance = await getUserBalance(userId);
      if (balance != null && balance < 0) {
        await unassignAiNumbersForUser({ localUserId: userId });
      }
    } catch (e) {
      if (DEBUG) console.warn('[ai.numbers.list] balance enforcement failed:', e?.message || e);
    }

    const [rows] = await pool.execute(
      `SELECT n.id, n.daily_number_id, n.phone_number, n.agent_id, n.dialin_config_id, n.created_at, n.updated_at,
              a.display_name AS agent_name
       FROM ai_numbers n
       LEFT JOIN ai_agents a ON a.id = n.agent_id
       WHERE n.user_id = ?
       ORDER BY n.created_at DESC`,
      [userId]
    );
    return res.json({ success: true, data: rows || [] });
  } catch (e) {
    if (DEBUG) console.error('[ai.numbers.list] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load numbers' });
  }
});

// Search available numbers by region/city (Daily)
app.get('/api/me/ai/numbers/available', requireAuth, async (req, res) => {
  try {
    const region = String(req.query.region || '').trim();
    const city = String(req.query.city || '').trim();

    const limitRaw = req.query.limit;
    let limit = 20;
    if (limitRaw != null && String(limitRaw).trim() !== '') {
      const n = parseInt(String(limitRaw), 10);
      if (Number.isFinite(n) && n > 0) limit = Math.min(n, 50);
    }

    if (!region && !city) {
      return res.status(400).json({ success: false, message: 'Provide at least one filter (region or city)' });
    }

    const US_STATE_NAME_TO_CODE = {
      'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
      'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
      'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
      'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
      'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
      'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
      'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
      'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
      'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
      'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
      'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
      'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
      'wisconsin': 'WI', 'wyoming': 'WY'
    };
    const US_STATE_CODE_SET = new Set(Object.values(US_STATE_NAME_TO_CODE));

    function normalizeUsStateCode(s) {
      const raw = String(s || '').trim();
      if (!raw) return '';
      const lower = raw.toLowerCase();
      if (US_STATE_NAME_TO_CODE[lower]) return US_STATE_NAME_TO_CODE[lower];
      const upper = raw.toUpperCase();
      if (/^[A-Z]{2}$/.test(upper) && US_STATE_CODE_SET.has(upper)) return upper;
      const tokens = upper.split(/[^A-Z]+/).filter(Boolean);
      for (let i = tokens.length - 1; i >= 0; i--) {
        const tok = tokens[i];
        if (tok === 'US' || tok === 'USA') continue;
        if (tok.length === 2 && US_STATE_CODE_SET.has(tok)) return tok;
      }
      return '';
    }

    // Build query params for Daily API. Supports: region (ISO 3166-2 alpha-2), city, limit.
    const dailyParams = {};
    if (region) {
      const regionCode = normalizeUsStateCode(region);
      if (regionCode) dailyParams.region = regionCode;
    }
    if (city) {
      dailyParams.city = city;
    }
    // Note: Daily API does not support a 'limit' parameter on /list-available-numbers

    if (DEBUG) console.log('[ai.numbers.available] Daily API params:', dailyParams);

    const avail = await dailyApiCall({ method: 'GET', path: '/list-available-numbers', params: dailyParams });
    const items = Array.isArray(avail?.data)
      ? avail.data
      : (Array.isArray(avail?.numbers) ? avail.numbers : (Array.isArray(avail) ? avail : []));

    const qRegionRaw = String(region || '').trim();
    const qRegionLower = qRegionRaw.toLowerCase();
    const qCity = String(city || '').toLowerCase();

    const results = [];

    for (const it of (items || [])) {
      const num = it?.number || it?.phone_number || it?.phoneNumber;
      if (!num) continue;

      const regionVal = it?.region || it?.state || it?.region_name || it?.state_name || it?.province || '';
      const cityVal = it?.city || it?.locality || it?.city_name || '';

      if (qRegionRaw) {
        const rRaw = String(regionVal || '').trim();
        const qCode = normalizeUsStateCode(qRegionRaw);
        if (qCode) {
          const rCode = normalizeUsStateCode(rRaw);
          if (!rCode || rCode !== qCode) continue;
        } else {
          const r = rRaw.toLowerCase();
          if (!r.includes(qRegionLower)) continue;
        }
      }

      if (qCity) {
        const c = String(cityVal || '').toLowerCase();
        if (!c.includes(qCity)) continue;
      }

      results.push({
        number: String(num),
        region: regionVal ? String(regionVal) : '',
        city: cityVal ? String(cityVal) : ''
      });

      if (results.length >= limit) break;
    }

    return res.json({ success: true, data: results });
  } catch (e) {
    if (DEBUG) console.warn('[ai.numbers.available] error:', e?.data || e?.message || e);
    return res.status(500).json({ success: false, message: e?.message || 'Failed to search available numbers' });
  }
});

// Buy any available number
app.post('/api/me/ai/numbers/buy', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;

    const [[cnt]] = await pool.execute('SELECT COUNT(*) AS total FROM ai_numbers WHERE user_id = ?', [userId]);
    const total = Number(cnt?.total || 0);
    if (total >= AI_MAX_NUMBERS) {
      return res.status(400).json({ success: false, message: `You can have up to ${AI_MAX_NUMBERS} phone numbers.` });
    }

    // Balance must be positive to buy/assign AI phone numbers.
    const buyBalance = await getUserBalance(userId);
    if (buyBalance != null && buyBalance <= 0) {
      return res.status(402).json({
        success: false,
        message: `Insufficient balance. Your current balance is $${buyBalance.toFixed(2)}. Please add funds and try again.`
      });
    }

    // Optional: balance check before buying a number (same pattern as DID purchase).
    // Uses AI_DID_* monthly fee config (fallback: DID_* markup) as the required minimum balance.
    const localFee = parseFloat(process.env.AI_DID_LOCAL_MONTHLY_FEE || process.env.DID_LOCAL_MONTHLY_MARKUP || '0') || 0;
    const tollfreeFee = parseFloat(process.env.AI_DID_TOLLFREE_MONTHLY_FEE || process.env.DID_TOLLFREE_MONTHLY_MARKUP || '0') || 0;
    const requiredMonthly = Math.max(localFee, tollfreeFee, 0);
    if (requiredMonthly > 0) {
      try {
        const currentCredit = await getUserBalance(userId);
        if (DEBUG) console.log('[ai.number.buy] Balance check:', { currentCredit, requiredMonthly });
        if (currentCredit != null && currentCredit < requiredMonthly) {
          return res.status(400).json({
            success: false,
            message: `Insufficient balance in your Phone.System account. This AI number costs at least $${requiredMonthly.toFixed(2)} per month and your current balance is $${Number(currentCredit).toFixed(2)}. Please add funds on the dashboard and try again.`
          });
        }
      } catch (balanceErr) {
        if (DEBUG) console.error('[ai.number.buy] Failed to check balance:', balanceErr.message || balanceErr);
        return res.status(500).json({ success: false, message: 'Failed to verify account balance. Please try again.' });
      }
    }

    const requestedNumber = String((req.body && (req.body.number || req.body.phone_number || req.body.phoneNumber)) || '').trim();
    if (requestedNumber && requestedNumber.length > 64) {
      return res.status(400).json({ success: false, message: 'number is too long' });
    }

    const { dailyNumberId, phoneNumber } = requestedNumber
      ? await dailyBuySpecificPhoneNumber(requestedNumber)
      : await dailyBuyAnyAvailableNumber();

    const [ins] = await pool.execute(
      'INSERT INTO ai_numbers (user_id, daily_number_id, phone_number) VALUES (?,?,?)',
      [userId, dailyNumberId, phoneNumber]
    );

    // Best-effort: bill the first monthly AI DID fee immediately.
    // The scheduler will also handle this later; cycle-table idempotency avoids double-charges.
    try {
      if (requiredMonthly > 0) {
        const [nrows] = await pool.execute(
          'SELECT id, phone_number, created_at FROM ai_numbers WHERE id = ? AND user_id = ? LIMIT 1',
          [ins.insertId, userId]
        );
        const numRow = nrows && nrows[0];
        if (numRow) {
          await chargeAiNumberMonthlyFee({
            userId,
            aiNumberId: numRow.id,
            phoneNumber: numRow.phone_number,
            monthlyAmount: requiredMonthly,
            isTollfree: detectTollfreeUsCaByNpa(numRow.phone_number),
            billedTo: numRow.created_at
          });
        }
      }
    } catch (billErr) {
      if (DEBUG) console.warn('[ai.number.buy] Initial monthly fee billing failed (will retry via scheduler):', billErr.message || billErr);
    }

    return res.json({ success: true, data: { id: ins.insertId, daily_number_id: dailyNumberId, phone_number: phoneNumber } });
  } catch (e) {
    if (DEBUG) console.error('[ai.numbers.buy] error:', e.data || e.message || e);
    return res.status(500).json({ success: false, message: e.message || 'Failed to buy number' });
  }
});

// Assign number to agent (one number per agent)
app.post('/api/me/ai/numbers/:id/assign', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const numberId = req.params.id;
    const agentId = req.body?.agent_id;
    if (!agentId) return res.status(400).json({ success: false, message: 'agent_id is required' });

    // Balance must be positive to change AI number assignments.
    const assignBalance = await getUserBalance(userId);
    if (assignBalance != null && assignBalance <= 0) {
      return res.status(402).json({
        success: false,
        message: `Insufficient balance. Your current balance is $${assignBalance.toFixed(2)}. Please add funds and try again.`
      });
    }

    const [nrows] = await pool.execute('SELECT * FROM ai_numbers WHERE id = ? AND user_id = ? LIMIT 1', [numberId, userId]);
    const num = nrows && nrows[0];
    if (!num) return res.status(404).json({ success: false, message: 'Number not found' });
    if (num.cancel_pending) {
      return res.status(409).json({ success: false, message: 'This AI number is being cancelled due to nonpayment and cannot be assigned.' });
    }

    const [arows] = await pool.execute('SELECT * FROM ai_agents WHERE id = ? AND user_id = ? LIMIT 1', [agentId, userId]);
    const agent = arows && arows[0];
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    // Enforce one number per agent
    const [existing] = await pool.execute(
      'SELECT id FROM ai_numbers WHERE user_id = ? AND agent_id = ? AND id <> ? LIMIT 1',
      [userId, agentId, numberId]
    );
    if (existing && existing.length) {
      return res.status(409).json({ success: false, message: 'That agent already has a number assigned. Unassign it first.' });
    }

    // If the number was previously assigned, remove its existing dial-in config
    if (num.dialin_config_id) {
      try { await dailyDeleteDialinConfig(num.dialin_config_id); } catch (e) { if (DEBUG) console.warn('[ai.assign] Failed to delete old dialin config', e.message || e); }
    }

    // Ensure the Pipecat agent has the latest platform keys (including DAILY_API_KEY)
    // before routing real PSTN traffic to it.
    try {
      const portalToken = await getOrCreateAgentActionTokenPlain({
        agentRow: agent,
        userId,
        label: 'ai.numbers.assign'
      });

      const userTransfer = await getUserAiTransferDestination(userId);
      const operatorNumber = normalizeAiTransferDestination(agent.transfer_to_number) || normalizeAiTransferDestination(userTransfer);
      const publicBaseUrl = getPublicBaseUrlFromReq(req);
      const bgUrlResolved = await resolveAgentBackgroundAudioUrl({
        userId,
        agentId: agent.id,
        fallbackUrl: agent.background_audio_url,
        publicBaseUrl
      });

      const secrets = buildPipecatSecrets({
        greeting: agent.greeting,
        prompt: agent.prompt,
        cartesiaVoiceId: agent.cartesia_voice_id,
        portalAgentActionToken: portalToken,
        operatorNumber,
        backgroundAudioUrl: bgUrlResolved,
        backgroundAudioGain: agent.background_audio_gain
      });
      await pipecatUpsertSecretSet(agent.pipecat_secret_set, secrets, agent.pipecat_region || PIPECAT_REGION);
      await pipecatUpdateAgent({
        agentName: agent.pipecat_agent_name,
        secretSetName: agent.pipecat_secret_set,
        regionOverride: agent.pipecat_region || PIPECAT_REGION
      });
    } catch (provErr) {
      const msg = provErr?.message || 'Failed to update agent in Pipecat';
      return res.status(502).json({ success: false, message: msg, error: provErr?.data });
    }

    const roomCreationApi = buildDailyRoomCreationApiUrl({ agentName: agent.pipecat_agent_name });
    const { dialinConfigId } = await dailyUpsertDialinConfig({ phoneNumber: num.phone_number, roomCreationApi });

    await pool.execute(
      'UPDATE ai_numbers SET agent_id = ?, dialin_config_id = ? WHERE id = ? AND user_id = ?',
      [agentId, dialinConfigId, numberId, userId]
    );

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.numbers.assign] error:', e.data || e.message || e);
    return res.status(500).json({ success: false, message: e.message || 'Failed to assign number' });
  }
});

// Unassign number
app.post('/api/me/ai/numbers/:id/unassign', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const numberId = req.params.id;

    // Balance must be positive to change AI number assignments.
    const unassignBalance = await getUserBalance(userId);
    if (unassignBalance != null && unassignBalance <= 0) {
      return res.status(402).json({
        success: false,
        message: `Insufficient balance. Your current balance is $${unassignBalance.toFixed(2)}. Please add funds and try again.`
      });
    }

    const [rows] = await pool.execute('SELECT * FROM ai_numbers WHERE id = ? AND user_id = ? LIMIT 1', [numberId, userId]);
    const num = rows && rows[0];
    if (!num) return res.status(404).json({ success: false, message: 'Number not found' });

    if (num.dialin_config_id) {
      try { await dailyDeleteDialinConfig(num.dialin_config_id); } catch (e) { if (DEBUG) console.warn('[ai.unassign] dialin delete failed', e.message || e); }
    }

    await pool.execute(
      'UPDATE ai_numbers SET agent_id = NULL, dialin_config_id = NULL WHERE id = ? AND user_id = ?',
      [numberId, userId]
    );

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.numbers.unassign] error:', e.data || e.message || e);
    return res.status(500).json({ success: false, message: e.message || 'Failed to unassign number' });
  }
});

// Delete AI number (release from Daily)
// Daily phone numbers can only be released after 28 days from purchase.
app.delete('/api/me/ai/numbers/:id', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const numberId = req.params.id;

    const [rows] = await pool.execute(
      'SELECT * FROM ai_numbers WHERE id = ? AND user_id = ? LIMIT 1',
      [numberId, userId]
    );
    const num = rows && rows[0];
    if (!num) return res.status(404).json({ success: false, message: 'Number not found' });

    // Check if number is at least 28 days old (Daily restriction)
    const createdAt = num.created_at ? new Date(num.created_at) : null;
    const now = new Date();
    const ageInDays = createdAt ? Math.floor((now - createdAt) / (1000 * 60 * 60 * 24)) : 0;
    const MIN_AGE_DAYS = 28;

    if (ageInDays < MIN_AGE_DAYS) {
      const daysRemaining = MIN_AGE_DAYS - ageInDays;
      return res.status(400).json({
        success: false,
        message: `This number cannot be deleted yet. Daily phone numbers can only be released after ${MIN_AGE_DAYS} days. Please wait ${daysRemaining} more day${daysRemaining === 1 ? '' : 's'}.`
      });
    }

    // Remove dial-in config if assigned
    if (num.dialin_config_id) {
      try {
        await dailyDeleteDialinConfig(num.dialin_config_id);
      } catch (e) {
        if (DEBUG) console.warn('[ai.numbers.delete] dialin delete failed', e.message || e);
      }
    }

    // Release the phone number from Daily
    try {
      await dailyReleasePhoneNumber(num.daily_number_id);
    } catch (e) {
      const msg = e?.message || 'Failed to release phone number from Daily';
      if (DEBUG) console.error('[ai.numbers.delete] Daily release failed:', e?.data || msg);
      return res.status(502).json({ success: false, message: msg });
    }

    // Delete from local database
    await pool.execute('DELETE FROM ai_numbers WHERE id = ? AND user_id = ?', [numberId, userId]);

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[ai.numbers.delete] error:', e.data || e.message || e);
    return res.status(500).json({ success: false, message: e.message || 'Failed to delete number' });
  }
});

// ========== Billing History ==========
// Get billing history for current user
// NOTE: We intentionally hide raw DID purchase rows ("DID Purchase (Order: ...)")
// from the customer-facing history so users only see Phone.System-level charges
// such as refills and DID markup fees
app.get('/api/me/billing-history', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const pageSize = Math.max(1, Math.min(100, parseInt(req.query.pageSize || '20', 10)));
    const offset = page * pageSize;

    // Get total count of *visible* billing rows (exclude internal DID purchase lines)
    const [[countRow]] = await pool.execute(
      "SELECT COUNT(*) as total FROM billing_history WHERE user_id = ? AND (description IS NULL OR description NOT LIKE 'DID Purchase (Order:%')",
      [userId]
    );
    const total = countRow?.total || 0;

    // Get visible billing records (exclude internal DID purchases)
    // Use query instead of execute for LIMIT/OFFSET as MySQL prepared statements require string conversion
    const [rows] = await pool.query(
      `SELECT id, amount, description, status, created_at
       FROM billing_history
       WHERE user_id = ? AND (description IS NULL OR description NOT LIKE 'DID Purchase (Order:%')
       ORDER BY created_at DESC
       LIMIT ${parseInt(pageSize, 10)} OFFSET ${parseInt(offset, 10)}`,
      [userId]
    );

    // Normalize legacy refill descriptions (remove older parentheses/+ formatting) for display.
    const normalizedRows = Array.isArray(rows) ? rows.map((row) => {
      const desc = row && row.description != null ? String(row.description) : '';
      const lower = desc.toLowerCase();
      const isLegacyRefill = !!desc && (desc.includes('+') || desc.startsWith('(') || desc.endsWith(')')) &&
        lower.includes('a.i service credit') && lower.includes('Phone.System') && lower.includes('invoice number');
      if (isLegacyRefill) {
        return { ...row, description: buildRefillDescription(row.id) };
      }
      return row;
    }) : rows;

    if (DEBUG) console.log('[billing.history.rows]', { userId, count: normalizedRows.length, sample: normalizedRows[0] || null });

    // Fetch current balance from local database
    const balance = await getUserBalance(userId);
    
    // If the balance is NEGATIVE, immediately unassign AI numbers (DIDs) from agents.
    if (Number.isFinite(balance) && balance < 0) {
      try { 
        await unassignAiNumbersForUser({ localUserId: userId }); 
      } catch (e) {
        if (DEBUG) console.warn('[ai.numbers.unassignAll] failed during billing-history fetch:', e?.message || e);
      }
    }

    return res.json({ success: true, data: normalizedRows, total, page, pageSize, balance });
  } catch (e) {
    if (DEBUG) console.error('[billing.history] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch billing history' });
  }
});

// ========== Per-user CDR history (inbound Voice CDRs) ==========
// Returns CDRs for the logged-in user based on user_did_cdrs table.
// Supports pagination (?page, ?pageSize) and optional filters: ?from, ?to, ?did.

// Shared helper to build a unified CDR timeline (inbound from user_did_cdrs + outbound
// from user_mb_cdrs) with sorting and pagination. Used by both /api/me/cdrs and
// /api/me/cdrs/local so they stay in sync.
async function loadUserCdrTimeline({ userId, page, pageSize, fromRaw, toRaw, didFilter }) {
  if (!pool) throw new Error('Database not configured');
  if (!userId) throw new Error('Missing userId');

  const filters = ['user_id = ?'];
  const params = [userId];

  if (fromRaw) {
    filters.push('DATE(time_start) >= ?');
    params.push(fromRaw);
  }
  if (toRaw) {
    filters.push('DATE(time_start) <= ?');
    params.push(toRaw);
  }
  if (didFilter) {
    filters.push('did_number = ?');
    params.push(didFilter);
  }
  const whereSql = filters.length ? 'WHERE ' + filters.join(' AND ') : '';

  // Inbound from user_did_cdrs (local CDR mirror)
  const [inboundRows] = await pool.query(
    `SELECT id, cdr_id, user_id, did_number, direction, src_number, dst_number,
            time_start, time_connect, time_end, duration, billsec, price, created_at
     FROM user_did_cdrs
     ${whereSql}
     ORDER BY time_start DESC, id DESC`,
    params
  );

  const inbound = inboundRows.map(r => ({
    id: r.id,
    cdrId: r.cdr_id,
    didNumber: r.did_number,
    direction: r.direction,
    srcNumber: r.src_number,
    dstNumber: r.dst_number,
    timeStart: r.time_start ? r.time_start.toISOString() : null,
    timeConnect: r.time_connect ? r.time_connect.toISOString() : null,
    timeEnd: r.time_end ? r.time_end.toISOString() : null,
    duration: r.duration != null ? Number(r.duration) : null,
    billsec: r.billsec != null ? Number(r.billsec) : null,
    price: r.price != null ? Number(r.price) : null,
    status: 'completed',
    createdAt: r.created_at ? r.created_at.toISOString() : null
  }));

  // Inbound AI calls from ai_call_logs (Daily dial-in -> Pipecat)
  let aiInbound = [];
  try {
    const aiFilters = ['user_id = ?'];
    const aiParams = [userId];
    if (fromRaw) {
      aiFilters.push('DATE(time_start) >= ?');
      aiParams.push(fromRaw);
    }
    if (toRaw) {
      aiFilters.push('DATE(time_start) <= ?');
      aiParams.push(toRaw);
    }
    if (didFilter) {
      aiFilters.push('(to_number = ? OR from_number = ?)');
      aiParams.push(didFilter, didFilter);
    }
    const whereAiSql = aiFilters.length ? 'WHERE ' + aiFilters.join(' AND ') : '';

    const [aiRows] = await pool.query(
      `SELECT id, call_id, call_domain, direction, from_number, to_number,
              time_start, time_connect, time_end, duration, billsec, price, status, created_at
       FROM ai_call_logs
       ${whereAiSql}
       ORDER BY time_start DESC, id DESC`,
      aiParams
    );

    aiInbound = (aiRows || []).map(r => ({
      id: r.call_id ? `ai-${r.call_id}` : `ai-db-${r.id}`,
      cdrId: r.call_id ? `daily-${r.call_id}` : null,
      didNumber: r.to_number || null,
      direction: r.direction || 'inbound',
      srcNumber: r.from_number || '',
      dstNumber: r.to_number || '',
      timeStart: r.time_start ? r.time_start.toISOString() : null,
      timeConnect: r.time_connect ? r.time_connect.toISOString() : null,
      timeEnd: r.time_end ? r.time_end.toISOString() : null,
      duration: r.duration != null ? Number(r.duration) : (r.billsec != null ? Number(r.billsec) : null),
      billsec: r.billsec != null ? Number(r.billsec) : (r.duration != null ? Number(r.duration) : null),
      price: r.price != null ? Number(r.price) : null,
      status: r.status || null,
      createdAt: r.created_at ? r.created_at.toISOString() : null
    }));
  } catch (e) {
    if (DEBUG) console.warn('[cdr.timeline] Failed to load AI inbound from ai_call_logs:', e.message || e);
    aiInbound = [];
  }

  // Local CDRs from new cdrs table
  let localCdrs = [];
  try {
    const localFilters = ['user_id = ?'];
    const localParams = [userId];
    if (fromRaw) {
      localFilters.push('DATE(time_start) >= ?');
      localParams.push(fromRaw);
    }
    if (toRaw) {
      localFilters.push('DATE(time_start) <= ?');
      localParams.push(toRaw);
    }
    if (didFilter) {
      localFilters.push('did_number = ?');
      localParams.push(didFilter);
    }
    const whereLocalSql = localFilters.length ? 'WHERE ' + localFilters.join(' AND ') : '';

    const [localRows] = await pool.query(
      `SELECT id, direction, src_number, dst_number, did_number, time_start, time_end,
              duration, billsec, price, status, session_id, created_at
       FROM cdrs
       ${whereLocalSql}
       ORDER BY time_start DESC, id DESC`,
      localParams
    );

    localCdrs = (localRows || []).map(r => ({
      id: `local-${r.id}`,
      cdrId: r.session_id || null,
      didNumber: r.did_number || null,
      direction: r.direction || 'inbound',
      srcNumber: r.src_number || '',
      dstNumber: r.dst_number || '',
      timeStart: r.time_start ? r.time_start.toISOString() : null,
      timeConnect: r.time_start ? r.time_start.toISOString() : null,
      timeEnd: r.time_end ? r.time_end.toISOString() : null,
      duration: r.duration != null ? Number(r.duration) : null,
      billsec: r.billsec != null ? Number(r.billsec) : null,
      price: r.price != null ? Number(r.price) : null,
      status: r.status || 'completed',
      createdAt: r.created_at ? r.created_at.toISOString() : null
    }));
  } catch (e) {
    if (DEBUG) console.warn('[cdr.timeline] Failed to load from local cdrs table:', e.message || e);
    localCdrs = [];
  }

  // Outbound from user_mb_cdrs (local billing system mirror)
  let outbound = [];
  try {
    outbound = await loadLocalOutboundCdrs({ userId, fromRaw, toRaw, didFilter });
  } catch (e) {
    if (DEBUG) console.warn('[cdr.timeline] Failed to load outbound from user_mb_cdrs:', e.message || e);
    outbound = [];
  }
  // Outbound dialer calls from dialer_call_logs (Pipecat dial-out)
  let dialerOutbound = [];
  try {
    const dialerFilters = ['l.user_id = ?'];
    const dialerParams = [userId];
    if (fromRaw) {
      dialerFilters.push('DATE(l.created_at) >= ?');
      dialerParams.push(fromRaw);
    }
    if (toRaw) {
      dialerFilters.push('DATE(l.created_at) <= ?');
      dialerParams.push(toRaw);
    }
    if (didFilter) {
      dialerFilters.push('(d.phone_number = ? OR n.phone_number = ?)');
      dialerParams.push(didFilter, didFilter);
    }
    const whereDialerSql = dialerFilters.length ? 'WHERE ' + dialerFilters.join(' AND ') : '';

    const [dialerRows] = await pool.query(
      `SELECT l.id, l.call_id, l.duration_sec, l.price, l.status, l.result, l.created_at,
              d.phone_number AS to_number,
              n.phone_number AS from_number
       FROM dialer_call_logs l
       LEFT JOIN dialer_leads d ON d.id = l.lead_id
       LEFT JOIN ai_numbers n ON n.agent_id = l.ai_agent_id AND n.user_id = l.user_id
       ${whereDialerSql}
       ORDER BY l.created_at DESC, l.id DESC`,
      dialerParams
    );

    dialerOutbound = (dialerRows || []).map(r => {
      let timeStart = null;
      let timeEnd = null;
      try {
        if (r.created_at) {
          const start = (r.created_at instanceof Date) ? r.created_at : new Date(r.created_at);
          if (!Number.isNaN(start.getTime())) {
            timeStart = start.toISOString();
            const dur = Number(r.duration_sec || 0);
            if (Number.isFinite(dur) && dur > 0) {
              timeEnd = new Date(start.getTime() + (dur * 1000)).toISOString();
            }
          }
        }
      } catch {}
      const durVal = (r.duration_sec != null ? Number(r.duration_sec) : null);
      return {
        id: `dialer-${r.id}`,
        cdrId: r.call_id ? `dialer-${r.call_id}` : null,
        didNumber: r.from_number || null,
        direction: 'outbound',
        srcNumber: r.from_number || '',
        dstNumber: r.to_number || '',
        timeStart,
        timeConnect: timeStart,
        timeEnd,
        duration: Number.isFinite(durVal) ? durVal : null,
        billsec: Number.isFinite(durVal) ? durVal : null,
        price: r.price != null ? Number(r.price) : null,
        status: r.status || r.result || null,
        createdAt: r.created_at ? (r.created_at instanceof Date ? r.created_at.toISOString() : new Date(r.created_at).toISOString()) : null
      };
    });
  } catch (e) {
    if (DEBUG) console.warn('[cdr.timeline] Failed to load dialer outbound from dialer_call_logs:', e.message || e);
    dialerOutbound = [];
  }

  let all = inbound.concat(aiInbound).concat(localCdrs).concat(dialerOutbound).concat(outbound);

  // Sort newest first by timeStart (fallback to createdAt)
  all.sort((a, b) => {
    const aTs = a.timeStart ? Date.parse(a.timeStart) : (a.createdAt ? Date.parse(a.createdAt) : 0);
    const bTs = b.timeStart ? Date.parse(b.timeStart) : (b.createdAt ? Date.parse(b.createdAt) : 0);
    if (bTs !== aTs) return bTs - aTs;
    const aId = String(a.id || '');
    const bId = String(b.id || '');
    return bId.localeCompare(aId);
  });

  const total = all.length;
  const startIndex = page * pageSize;
  const endIndex = startIndex + pageSize;
  const pageRows = all.slice(startIndex, endIndex);

  if (DEBUG) console.log('[cdr.timeline]', { userId, count: pageRows.length, total });

  return { rows: pageRows, total };
}
app.get('/api/me/cdrs', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    if (!userId) return res.status(401).json({ success: false, message: 'Not authenticated' });

    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const pageSize = Math.max(1, Math.min(200, parseInt(req.query.pageSize || '50', 10)));

    const fromRaw = (req.query.from || '').toString().trim();
    const toRaw = (req.query.to || '').toString().trim();
    const didFilter = (req.query.did || '').toString().trim();

    const { rows, total } = await loadUserCdrTimeline({
      userId,
      page,
      pageSize,
      fromRaw,
      toRaw,
      didFilter
    });

    return res.json({ success: true, data: rows, total, page, pageSize });
  } catch (e) {
    if (DEBUG) console.error('[me.cdrs] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch CDRs' });
  }
});

// Debug: CDR health snapshot for the logged-in user (helps diagnose "history not updating")
app.get('/api/me/cdrs/health', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    if (!userId) return res.status(401).json({ success: false, message: 'Not authenticated' });

    const [[u]] = await pool.execute(
      'SELECT id, username, email FROM signup_users WHERE id=? LIMIT 1',
      [userId]
    );

    const username = u?.username || req.session.username || '';
    const email = u?.email || req.session.email || '';

    const [[outAgg]] = await pool.execute(
      'SELECT COUNT(*) AS total, SUM(CASE WHEN time_start IS NULL THEN 1 ELSE 0 END) AS null_time_start, MAX(time_start) AS last_time_start, MAX(created_at) AS last_created_at FROM user_mb_cdrs WHERE user_id=?',
      [userId]
    );

    const [recent] = await pool.query(
      `SELECT cdr_id, time_start, src_number, dst_number, did_number, price
       FROM user_mb_cdrs
       WHERE user_id = ?
       ORDER BY time_start DESC, id DESC
       LIMIT 5`,
      [userId]
    );

    const outbound = {
      total: Number(outAgg?.total || 0),
      nullTimeStart: Number(outAgg?.null_time_start || 0),
      lastTimeStart: outAgg?.last_time_start ? outAgg.last_time_start.toISOString() : null,
      lastCreatedAt: outAgg?.last_created_at ? outAgg.last_created_at.toISOString() : null,
      recent: (recent || []).map(r => ({
        cdrId: r.cdr_id != null ? String(r.cdr_id) : null,
        timeStart: r.time_start ? r.time_start.toISOString() : null,
        srcNumber: r.src_number || '',
        dstNumber: r.dst_number || '',
        didNumber: r.did_number || null,
        price: r.price != null ? Number(r.price) : null
      }))
    };

    return res.json({
      success: true,
      data: {
        localUserId: String(userId),
        username,
        email,
        outbound
      }
    });
  } catch (e) {
    if (DEBUG) console.error('[me.cdrs.health] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to compute CDR health' });
  }
});

// CDR import from external billing system is no longer available.
app.post('/api/me/cdrs/import-now', requireAuth, (req, res) => {
  return res.status(410).json({ success: false, message: 'CDR import from external billing system has been removed. CDRs are now managed locally.' });
});


// Simple list of the logged-in user's DIDs from local DB (used for Call History DID filter)
app.get('/api/me/dids', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    if (!userId) return res.status(401).json({ success: false, message: 'Not authenticated' });

    // Include DIDs + AI (Daily) phone numbers so Call History filtering works for both.
    const [rows] = await pool.query(
      `SELECT did_number FROM user_dids WHERE user_id = ?
       UNION
       SELECT phone_number AS did_number FROM ai_numbers WHERE user_id = ?
       ORDER BY did_number`,
      [userId, userId]
    );

    return res.json({ success: true, data: rows });
  } catch (e) {
    if (DEBUG) console.error('[me.dids] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch DIDs' });
  }
});

// Lightweight per-user stats endpoint for dashboard (totals + daily counts)
// Query params: optional from, to (YYYY-MM-DD). Defaults to PREFETCH_DAYS window.
app.get('/api/me/stats', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    if (!userId) return res.status(401).json({ success: false, message: 'Not authenticated' });

    const fromRaw = (req.query.from || '').toString().trim();
    const toRaw = (req.query.to || '').toString().trim();
    let from = fromRaw;
    let to = toRaw;
    if (!from || !to) {
      const rng = defaultRange();
      if (!from) from = rng.from;
      if (!to) to = rng.to;
    }

    // Total DIDs for this user (local user_dids + AI numbers)
    let userDidCount = 0;
    try {
      const [[row]] = await pool.execute(
        'SELECT COUNT(*) AS total FROM user_dids WHERE user_id = ? AND cancel_pending = 0',
        [userId]
      );
      userDidCount = Number(row?.total || 0);
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.dids] failed:', e.message || e);
    }

    let aiDidCount = 0;
    try {
      const [[row]] = await pool.execute(
        'SELECT COUNT(*) AS total FROM ai_numbers WHERE user_id = ?',
        [userId]
      );
      aiDidCount = Number(row?.total || 0);
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.dids] failed:', e.message || e);
    }

    const totalDids = userDidCount + aiDidCount;

    // AI stats: agent count + emails sent
    let aiAgentCount = 0;
    try {
      const [[row]] = await pool.execute(
        'SELECT COUNT(*) AS total FROM ai_agents WHERE user_id = ?',
        [userId]
      );
      aiAgentCount = Number(row?.total || 0);
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.agents] failed:', e.message || e);
    }

    let aiEmailSentCount = 0;
    let aiMeetingLinkSentCount = 0;
    try {
      const [[row]] = await pool.execute(
        `SELECT
           SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS total_completed,
           SUM(CASE WHEN status = 'completed'
                     AND (meeting_provider IS NOT NULL OR meeting_room_url IS NOT NULL OR meeting_join_url IS NOT NULL)
                    THEN 1 ELSE 0 END) AS meeting_completed
         FROM ai_email_sends
         WHERE user_id = ?`,
        [userId]
      );
      aiEmailSentCount = Number(row?.total_completed || 0);
      aiMeetingLinkSentCount = Number(row?.meeting_completed || 0);
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.emails] failed:', e.message || e);
    }

    let aiPhysicalMailSentCount = 0;
    try {
      const [[row]] = await pool.execute(
        "SELECT COUNT(*) AS total FROM ai_mail_sends WHERE user_id = ? AND status IN ('submitted','completed')",
        [userId]
      );
      aiPhysicalMailSentCount = Number(row?.total || 0);
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.mail] failed:', e.message || e);
    }

    // Status breakdowns for pie charts
    let emailStatusBreakdown = [];
    try {
      const [rows] = await pool.execute(
        `SELECT status, COUNT(*) AS count FROM ai_email_sends WHERE user_id = ? GROUP BY status`,
        [userId]
      );
      emailStatusBreakdown = rows.map(r => ({ status: r.status || 'unknown', count: Number(r.count || 0) }));
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.emailBreakdown] failed:', e.message || e);
    }

    let smsStatusBreakdown = [];
    try {
      const [rows] = await pool.execute(
        `SELECT status, dlr_status, COUNT(*) AS count FROM ai_sms_sends WHERE user_id = ? GROUP BY status, dlr_status`,
        [userId]
      );
      smsStatusBreakdown = rows.map(r => ({
        status: r.status || 'unknown',
        dlr_status: r.dlr_status || null,
        count: Number(r.count || 0)
      }));
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.smsBreakdown] failed:', e.message || e);
    }

    let mailStatusBreakdown = [];
    try {
      const [rows] = await pool.execute(
        `SELECT status, COUNT(*) AS count FROM ai_mail_sends WHERE user_id = ? GROUP BY status`,
        [userId]
      );
      mailStatusBreakdown = rows.map(r => ({ status: r.status || 'unknown', count: Number(r.count || 0) }));
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.mailBreakdown] failed:', e.message || e);
    }

    let meetingStatusBreakdown = [];
    try {
      const [rows] = await pool.execute(
        `SELECT status, COUNT(*) AS count FROM ai_email_sends 
         WHERE user_id = ? AND (meeting_provider IS NOT NULL OR meeting_room_url IS NOT NULL OR meeting_join_url IS NOT NULL)
         GROUP BY status`,
        [userId]
      );
      meetingStatusBreakdown = rows.map(r => ({ status: r.status || 'unknown', count: Number(r.count || 0) }));
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.meetingBreakdown] failed:', e.message || e);
    }

    let paymentStatusBreakdown = [];
    try {
      const [rows] = await pool.execute(
        `SELECT status, COUNT(*) AS count FROM user_payment_requests WHERE user_id = ? GROUP BY status`,
        [userId]
      );
      paymentStatusBreakdown = rows.map(r => ({ status: r.status || 'unknown', count: Number(r.count || 0) }));
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.ai.paymentBreakdown] failed:', e.message || e);
    }

    // Inbound calls grouped by day
    let inboundCount = 0;
    const inboundByDayMap = new Map();
    try {
      const [rows] = await pool.query(
        `SELECT DATE(time_start) AS d, COUNT(*) AS calls
         FROM user_did_cdrs
         WHERE user_id = ? AND DATE(time_start) BETWEEN ? AND ?
         GROUP BY DATE(time_start)
         ORDER BY DATE(time_start)`,
        [userId, from, to]
      );
      for (const r of rows) {
        const dObj = r.d instanceof Date ? r.d : null;
        const day = dObj ? dObj.toISOString().slice(0, 10) : String(r.d || '');
        const c = Number(r.calls || 0);
        inboundCount += c;
        inboundByDayMap.set(day, (inboundByDayMap.get(day) || 0) + c);
      }
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.inbound.dids] failed:', e.message || e);
    }

    try {
      const [rows] = await pool.query(
        `SELECT DATE(time_start) AS d, COUNT(*) AS calls
         FROM ai_call_logs
         WHERE user_id = ? AND DATE(time_start) BETWEEN ? AND ?
         GROUP BY DATE(time_start)
         ORDER BY DATE(time_start)`,
        [userId, from, to]
      );
      for (const r of rows) {
        const dObj = r.d instanceof Date ? r.d : null;
        const day = dObj ? dObj.toISOString().slice(0, 10) : String(r.d || '');
        const c = Number(r.calls || 0);
        inboundCount += c;
        inboundByDayMap.set(day, (inboundByDayMap.get(day) || 0) + c);
      }
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.inbound.ai] failed:', e.message || e);
    }

    // Local CDRs from cdrs table (inbound)
    try {
      const [rows] = await pool.query(
        `SELECT DATE(time_start) AS d, COUNT(*) AS calls
         FROM cdrs
         WHERE user_id = ? AND direction = 'inbound' AND DATE(time_start) BETWEEN ? AND ?
         GROUP BY DATE(time_start)
         ORDER BY DATE(time_start)`,
        [userId, from, to]
      );
      for (const r of rows) {
        const dObj = r.d instanceof Date ? r.d : null;
        const day = dObj ? dObj.toISOString().slice(0, 10) : String(r.d || '');
        const c = Number(r.calls || 0);
        inboundCount += c;
        inboundByDayMap.set(day, (inboundByDayMap.get(day) || 0) + c);
      }
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.inbound.local] failed:', e.message || e);
    }

    // Outbound calls grouped by day from user_mb_cdrs (billing system) + dialer_call_logs (AI outbound dialer)
    let outboundCount = 0;
    const outboundByDayMap = new Map();

    // Standard SIP/telephony outbound (billing system mirror)
    try {
      const [rows] = await pool.query(
        `SELECT DATE(time_start) AS d, COUNT(*) AS calls
         FROM user_mb_cdrs
         WHERE user_id = ? AND DATE(time_start) BETWEEN ? AND ?
         GROUP BY DATE(time_start)
         ORDER BY DATE(time_start)`,
        [userId, from, to]
      );
      for (const r of rows) {
        const dObj = r.d instanceof Date ? r.d : null;
        const day = dObj ? dObj.toISOString().slice(0, 10) : String(r.d || '');
        const c = Number(r.calls || 0);
        outboundCount += c;
        outboundByDayMap.set(day, (outboundByDayMap.get(day) || 0) + c);
      }
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.outbound.mb] failed:', e.message || e);
    }

    // AI outbound dialer (Pipecat dial-out) from dialer_call_logs
    // NOTE: dialer_call_logs does not have time_start; use created_at as the call attempt timestamp.
    // Exclude "queued" so we count actual dial attempts, not just queued rows.
    try {
      const [rows] = await pool.query(
        `SELECT DATE(created_at) AS d, COUNT(*) AS calls
         FROM dialer_call_logs
         WHERE user_id = ?
           AND DATE(created_at) BETWEEN ? AND ?
           AND (status IS NULL OR status <> 'queued')
         GROUP BY DATE(created_at)
         ORDER BY DATE(created_at)`,
        [userId, from, to]
      );
      for (const r of rows) {
        const dObj = r.d instanceof Date ? r.d : null;
        const day = dObj ? dObj.toISOString().slice(0, 10) : String(r.d || '');
        const c = Number(r.calls || 0);
        outboundCount += c;
        outboundByDayMap.set(day, (outboundByDayMap.get(day) || 0) + c);
      }
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.outbound.dialer] failed:', e.message || e);
    }

    // Local CDRs from cdrs table (outbound)
    try {
      const [rows] = await pool.query(
        `SELECT DATE(time_start) AS d, COUNT(*) AS calls
         FROM cdrs
         WHERE user_id = ? AND direction = 'outbound' AND DATE(time_start) BETWEEN ? AND ?
         GROUP BY DATE(time_start)
         ORDER BY DATE(time_start)`,
        [userId, from, to]
      );
      for (const r of rows) {
        const dObj = r.d instanceof Date ? r.d : null;
        const day = dObj ? dObj.toISOString().slice(0, 10) : String(r.d || '');
        const c = Number(r.calls || 0);
        outboundCount += c;
        outboundByDayMap.set(day, (outboundByDayMap.get(day) || 0) + c);
      }
    } catch (e) {
      if (DEBUG) console.warn('[me.stats.outbound.local] failed:', e.message || e);
    }

// SIP users: no longer tracked via external billing system
    const totalSip = 0;
    const onlineSip = 0;

    // Merge daily inbound/outbound into a single series
    const allDays = new Set([...inboundByDayMap.keys(), ...outboundByDayMap.keys()]);
    const callsByDay = Array.from(allDays)
      .sort()
      .map(d => ({
        date: d,
        inbound: inboundByDayMap.get(d) || 0,
        outbound: outboundByDayMap.get(d) || 0
      }));

    // "Today" is interpreted as the end of the selected range ("to" date)
    const todayKey = to;
    const inboundToday = inboundByDayMap.get(todayKey) || 0;
    const outboundToday = outboundByDayMap.get(todayKey) || 0;

    return res.json({
      success: true,
      range: { from, to },
      kpis: {
        totalDids,
        userDidCount,
        aiDidCount,
        inboundCount,
        outboundCount,
        inboundToday,
        outboundToday,
        totalSip,
        onlineSip,
        aiAgentCount,
        aiEmailSentCount,
        aiMeetingLinkSentCount,
        aiPhysicalMailSentCount
      },
      series: {
        callsByDay
      },
      statusBreakdowns: {
        email: emailStatusBreakdown,
        sms: smsStatusBreakdown,
        mail: mailStatusBreakdown,
        meeting: meetingStatusBreakdown,
        payment: paymentStatusBreakdown
      }
    });
  } catch (e) {
    if (DEBUG) console.error('[me.stats] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch stats' });
  }
});

// Local CDR history endpoint: reads outbound from user_mb_cdrs instead of billing system
// and inbound from user_did_cdrs. Same query params as /api/me/cdrs: page, pageSize,
// optional from, to, did. Useful for long history and when billing system is offline.
app.get('/api/me/cdrs/local', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    if (!userId) return res.status(401).json({ success: false, message: 'Not authenticated' });

    const page = Math.max(0, parseInt(req.query.page || '0', 10));
    const pageSize = Math.max(1, Math.min(200, parseInt(req.query.pageSize || '50', 10)));

    const fromRaw = (req.query.from || '').toString().trim();
    const toRaw = (req.query.to || '').toString().trim();
    const didFilter = (req.query.did || '').toString().trim();

    const { rows, total } = await loadUserCdrTimeline({
      userId,
      page,
      pageSize,
      fromRaw,
      toRaw,
      didFilter
    });

    return res.json({ success: true, data: rows, total, page, pageSize });
  } catch (e) {
    if (DEBUG) console.error('[me.cdrs.local] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to fetch local CDRs' });
  }
});

// Add funds to user account (manual/direct refill)
app.post('/api/me/add-funds', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    const userId = req.session.userId;
    const { amount } = req.body || {};

    // Validate amount
    const amountNum = parseFloat(amount);
    if (!amountNum || amountNum <= 0) {
      return res.status(400).json({ success: false, message: 'Amount must be greater than 0' });
    }
    if (amountNum > 10000) {
      return res.status(400).json({ success: false, message: 'Amount cannot exceed $10,000' });
    }

    // Fetch user info for receipt email (display name + recipient)
    let username = '';
    let firstName = '';
    let lastName = '';
    let userEmail = '';
    try {
      if (pool) {
        const [rows] = await pool.execute('SELECT username, firstname, lastname, email FROM signup_users WHERE id=? LIMIT 1', [userId]);
        if (rows && rows[0]) {
          if (rows[0].username) username = String(rows[0].username);
          if (rows[0].firstname) firstName = String(rows[0].firstname);
          if (rows[0].lastname) lastName = String(rows[0].lastname);
          if (rows[0].email) userEmail = String(rows[0].email);
        }
      }
    } catch (e) {
      if (DEBUG) console.warn('[add-funds] Failed to fetch user info for description:', e.message || e);
    }
    const baseUser = username || (req.session.username || 'unknown');
    const fullName = `${firstName || ''} ${lastName || ''}`.trim();
    const displayName = fullName || baseUser;

    const paymentMethod = 'Manual';

    // Insert pending billing record. billing_history.id is our invoice number.
    const [insertResult] = await pool.execute(
      'INSERT INTO billing_history (user_id, amount, description, status) VALUES (?, ?, ?, ?)',
      [userId, amountNum, null, 'pending']
    );
    const billingId = insertResult.insertId;

    const desc = buildRefillDescription(billingId);
    try {
      await pool.execute('UPDATE billing_history SET description = ? WHERE id = ?', [desc, billingId]);
    } catch {}

    const result = await creditBalance(userId, amountNum, desc);

    if (result.success) {
      return res.json({ success: true, message: 'Funds added successfully' });
    }
    return res.status(500).json({ success: false, message: 'Failed to add credit: ' + (result.error || 'Unknown error') });
  } catch (e) {
    if (DEBUG) console.error('[add-funds] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to add funds' });
  }
});

// Create a NOWPayments checkout (Standard e-commerce flow)
// Returns a hosted payment URL where the user can complete a crypto payment.
app.post('/api/me/nowpayments/checkout', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    if (!NOWPAYMENTS_API_KEY) {
      return res.status(500).json({ success: false, message: 'Crypto payments are not configured' });
    }
    const userId = req.session.userId;
    const { amount } = req.body || {};

    const amountNum = parseFloat(amount);
    if (!Number.isFinite(amountNum)) {
      return res.status(400).json({ success: false, message: 'Invalid amount' });
    }
    if (amountNum < CHECKOUT_MIN_AMOUNT || amountNum > CHECKOUT_MAX_AMOUNT) {
      return res.status(400).json({ success: false, message: `Amount must be between $${CHECKOUT_MIN_AMOUNT} and $${CHECKOUT_MAX_AMOUNT}` });
    }

    // Fetch user info for description + email
    let username = '';
    let firstName = '';
    let lastName = '';
    let userEmail = '';
    try {
      const [rows] = await pool.execute('SELECT username, firstname, lastname, email FROM signup_users WHERE id=? LIMIT 1', [userId]);
      if (rows && rows[0]) {
        if (rows[0].username) username = String(rows[0].username);
        if (rows[0].firstname) firstName = String(rows[0].firstname);
        if (rows[0].lastname) lastName = String(rows[0].lastname);
        if (rows[0].email) userEmail = String(rows[0].email);
      }
    } catch (e) {
      if (DEBUG) console.warn('[nowpayments.checkout] Failed to fetch user info:', e.message || e);
    }
    const baseUser = username || (req.session.username || 'unknown');
    const fullName = `${firstName || ''} ${lastName || ''}`.trim();
    const displayName = fullName || baseUser;

    // Insert pending billing record (we will mark completed after successful IPN).
    // billing_history.id is our invoice number.
    const [insertResult] = await pool.execute(
      'INSERT INTO billing_history (user_id, amount, description, status) VALUES (?, ?, ?, ?)',
      [userId, amountNum, null, 'pending']
    );
    const billingId = insertResult.insertId;

    const desc = buildRefillDescription(billingId);
    try {
      await pool.execute('UPDATE billing_history SET description = ? WHERE id = ?', [desc, billingId]);
    } catch {}

    // Build deterministic order_id encoding userId + billingId for later lookup from IPN
    const orderId = `np-${userId}-${billingId}`;

    // Build IPN & redirect URLs
    const baseUrl = PUBLIC_BASE_URL || `${req.protocol}://${req.get('host')}`;
    const ipnUrl = joinUrl(baseUrl, '/nowpayments/ipn');
    const successUrl = joinUrl(baseUrl, '/dashboard?payment=success&method=crypto');
    const cancelUrl = joinUrl(baseUrl, '/dashboard?payment=cancel&method=crypto');

    const client = nowpaymentsAxios();
    const payload = {
      price_amount: amountNum,
      price_currency: 'usd',
      order_id: orderId,
      order_description: desc,
      ipn_callback_url: ipnUrl,
      success_url: successUrl,
      cancel_url: cancelUrl
    };

    if (DEBUG) console.log('[nowpayments.checkout] Creating invoice:', payload);
    const resp = await client.post('/invoice', payload);
    const data = resp?.data || {};
    const paymentUrl = data.invoice_url || data.payment_url || data.checkout_url || null;
    if (!paymentUrl) {
      if (DEBUG) console.error('[nowpayments.checkout] Missing invoice/payment URL in response:', data);
      await pool.execute(
        'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
        ['failed', JSON.stringify(data), billingId]
      );
      return res.status(502).json({ success: false, message: 'Payment provider did not return a checkout URL' });
    }

    // Record NOWPayments invoice/payment row for tracking/idempotency
    try {
      const remoteId = data.id || data.payment_id || orderId;
      const paymentStatus = data.payment_status || data.invoice_status || 'waiting';
      await pool.execute(
        'INSERT INTO nowpayments_payments (user_id, payment_id, order_id, price_amount, price_currency, payment_status, credited, raw_payload) VALUES (?, ?, ?, ?, ?, ?, 0, ?)',
        [userId, String(remoteId), String(orderId), amountNum, 'usd', String(paymentStatus), JSON.stringify(data)]
      );
    } catch (e) {
      if (DEBUG) console.warn('[nowpayments.checkout] Failed to insert nowpayments_payments row:', e.message || e);
      // Do not fail the checkout creation if this insert fails; IPN can still be processed via order_id encoding
    }

    return res.json({ success: true, payment_url: paymentUrl });
  } catch (e) {
    if (DEBUG) console.error('[nowpayments.checkout] error:', e.response?.data || e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to create crypto payment' });
  }
});

// Create a Bill.com / BILL invoice and return a hosted payment link.
// Users can pay via BILL and their Phone.System balance will be credited on webhook confirmation.
async function handleBillcomCheckout(req, res) {
  let billingId = null;
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    if (!isBillcomConfigured()) {
      return res.status(500).json({ success: false, message: 'ACH payments are not configured' });
    }

    const userId = req.session.userId;
    const { amount } = req.body || {};

    const amountNum = parseFloat(amount);
    if (!Number.isFinite(amountNum)) {
      return res.status(400).json({ success: false, message: 'Invalid amount' });
    }
    if (amountNum < CHECKOUT_MIN_AMOUNT || amountNum > CHECKOUT_MAX_AMOUNT) {
      return res.status(400).json({ success: false, message: `Amount must be between $${CHECKOUT_MIN_AMOUNT} and $${CHECKOUT_MAX_AMOUNT}` });
    }

    // Fetch user info for description + invoice customer fields
    let username = '';
    let firstName = '';
    let lastName = '';
    let userEmail = '';
    try {
      const [rows] = await pool.execute('SELECT username, firstname, lastname, email FROM signup_users WHERE id=? LIMIT 1', [userId]);
      if (rows && rows[0]) {
        if (rows[0].username) username = String(rows[0].username);
        if (rows[0].firstname) firstName = String(rows[0].firstname);
        if (rows[0].lastname) lastName = String(rows[0].lastname);
        if (rows[0].email) userEmail = String(rows[0].email);
      }
    } catch (e) {
      if (DEBUG) console.warn('[billcom.checkout] Failed to fetch user info:', e.message || e);
    }

    const baseUser = username || (req.session.username || 'unknown');
    const fullName = `${firstName || ''} ${lastName || ''}`.trim();
    const displayName = fullName || baseUser;

    if (!userEmail) {
      return res.status(400).json({ success: false, message: 'An email address is required to use ACH payments' });
    }

    // Insert pending billing record (we will mark completed after successful webhook).
    // billing_history.id is our invoice number.
    const [insertResult] = await pool.execute(
      'INSERT INTO billing_history (user_id, amount, description, status) VALUES (?, ?, ?, ?)',
      [userId, amountNum, null, 'pending']
    );
    billingId = insertResult.insertId;

    const desc = buildRefillDescription(billingId);
    try {
      await pool.execute('UPDATE billing_history SET description = ? WHERE id = ?', [desc, billingId]);
    } catch {}

    const orderId = `bc-${userId}-${billingId}`;

    const dueDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

    // Create invoice
    const invoicePayload = {
      invoiceNumber: String(billingId),
      customer: {
        // Create/identify customer by name/email (BILL will create if not found)
        name: String(displayName || baseUser || 'Customer').substring(0, 90),
        email: String(userEmail).trim()
      },
      invoiceDate: dueDate,
      dueDate,
      invoiceLineItems: [
        {
          quantity: 1,
          description: desc.substring(0, 200),
          price: Number(amountNum.toFixed(2))
        }
      ],
      processingOptions: {
        // Send invoice email from BILL (best-effort).
        sendEmail: true
      }
    };

    if (DEBUG) console.log('[billcom.checkout] Creating invoice:', { orderId, amountNum });
    const invoice = await billcomApiCall({ method: 'POST', path: '/v3/invoices', body: invoicePayload });

    const invoiceId = invoice && invoice.id ? String(invoice.id) : null;
    const customerId = invoice && invoice.customerId ? String(invoice.customerId) : null;

    if (!invoiceId || !customerId) {
      await pool.execute(
        'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
        ['failed', JSON.stringify(invoice || {}), billingId]
      );
      return res.status(502).json({ success: false, message: 'Payment provider did not return invoice identifiers' });
    }

    // Create payment link
    const linkPayload = { customerId, email: String(userEmail).trim() };
    const link = await billcomApiCall({
      method: 'POST',
      path: `/v3/invoices/${encodeURIComponent(invoiceId)}/payment-link`,
      body: linkPayload
    });

    const paymentUrl = link && (link.paymentLink || link.payment_link || link.url) ? String(link.paymentLink || link.payment_link || link.url) : null;
    if (!paymentUrl) {
      await pool.execute(
        'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
        ['failed', JSON.stringify(link || {}), billingId]
      );
      return res.status(502).json({ success: false, message: 'Payment provider did not return a payment link' });
    }

    // Record BILL invoice/payment link for tracking/idempotency
    try {
      const rawPayload = JSON.stringify({ invoice, payment_link: link });
      await pool.execute(
        'INSERT INTO billcom_payments (user_id, invoice_id, customer_id, order_id, payment_link, status, credited, raw_payload) VALUES (?, ?, ?, ?, ?, ?, 0, ?)',
        [userId, invoiceId, customerId, orderId, paymentUrl, 'pending', rawPayload]
      );
    } catch (e) {
      if (DEBUG) console.warn('[billcom.checkout] Failed to insert billcom_payments row:', e.message || e);
      // Do not fail the checkout creation if this insert fails; webhook can still be processed via billing_history.
    }

    return res.json({ success: true, payment_url: paymentUrl });
  } catch (e) {
    if (billingId && pool) {
      try {
        await pool.execute(
          'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
          ['failed', String(e?.message || e), billingId]
        );
      } catch {}
    }
    if (DEBUG) console.error('[billcom.checkout] error:', e?.data || e?.message || e);
    return res.status(500).json({ success: false, message: 'Failed to create ACH payment' });
  }
}

app.post('/api/me/billcom/checkout', requireAuth, handleBillcomCheckout);

// ACH checkout (Bill.com / BILL hosted payment link)
app.post('/api/me/ach/checkout', requireAuth, handleBillcomCheckout);

// Create a Square Payment Link checkout for card payments
// Returns a hosted Square URL where the user can pay with card.
async function handleSquareCheckout(req, res) {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    if (!SQUARE_ACCESS_TOKEN || !SQUARE_APPLICATION_ID) {
      return res.status(500).json({ success: false, message: 'Card payments are not configured' });
    }
    const userId = req.session.userId;
    const { amount } = req.body || {};

    const amountNum = parseFloat(amount);
    if (!Number.isFinite(amountNum)) {
      return res.status(400).json({ success: false, message: 'Invalid amount' });
    }
    if (amountNum < CHECKOUT_MIN_AMOUNT || amountNum > CHECKOUT_MAX_AMOUNT) {
      return res.status(400).json({ success: false, message: `Amount must be between $${CHECKOUT_MIN_AMOUNT} and $${CHECKOUT_MAX_AMOUNT}` });
    }

    // Fetch user info for description + email
    let username = '';
    let firstName = '';
    let lastName = '';
    let userEmail = '';
    try {
      const [rows] = await pool.execute('SELECT username, firstname, lastname, email FROM signup_users WHERE id=? LIMIT 1', [userId]);
      if (rows && rows[0]) {
        if (rows[0].username) username = String(rows[0].username);
        if (rows[0].firstname) firstName = String(rows[0].firstname);
        if (rows[0].lastname) lastName = String(rows[0].lastname);
        if (rows[0].email) userEmail = String(rows[0].email);
      }
    } catch (e) {
      if (DEBUG) console.warn('[square.checkout] Failed to fetch user info:', e.message || e);
    }
    const baseUser = username || (req.session.username || 'unknown');

    // Insert pending billing record (we will mark completed after successful webhook).
    // billing_history.id is our invoice number.
    const [insertResult] = await pool.execute(
      'INSERT INTO billing_history (user_id, amount, description, status) VALUES (?, ?, ?, ?)',
      [userId, amountNum, null, 'pending']
    );
    const billingId = insertResult.insertId;

    const desc = buildRefillDescription(billingId);
    try {
      await pool.execute('UPDATE billing_history SET description = ? WHERE id = ?', [desc, billingId]);
    } catch {}

    // Build deterministic local order_id encoding userId + billingId for correlation with Square
    const orderId = `sq-${userId}-${billingId}`;

    const baseUrl = PUBLIC_BASE_URL || `${req.protocol}://${req.get('host')}`;
    const redirectUrl = joinUrl(baseUrl, '/dashboard?payment=success&method=card');

    const locationId = await getSquareLocationId();
    if (!locationId) {
      if (DEBUG) console.error('[square.checkout] Could not resolve Square location id');
      await pool.execute(
        'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
        ['failed', 'Square location_id not configured', billingId]
      );
      return res.status(500).json({ success: false, message: 'Card payments are temporarily unavailable' });
    }

    const idempotencyKey = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex');
    const body = {
      idempotency_key: idempotencyKey,
      quick_pay: {
        name: desc.substring(0, 60),
        price_money: {
          amount: Math.round(amountNum * 100),
          currency: 'USD'
        },
        location_id: locationId
      },
      payment_note: orderId,
      checkout_options: {
        redirect_url: redirectUrl
      }
    };

    if (userEmail) {
      body.pre_populated_data = { buyer_email: userEmail };
    }

    const squareBase = squareApiBaseUrl();
    if (DEBUG) console.log('[square.checkout] Creating payment link:', { orderId, amountNum, locationId });
    const resp = await axios.post(`${squareBase}/v2/online-checkout/payment-links`, body, {
      timeout: 30000,
      headers: {
        'Authorization': `Bearer ${SQUARE_ACCESS_TOKEN}`,
        'Square-Version': '2025-10-16',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });

    const data = resp?.data || {};
    const paymentLink = data.payment_link || {};
    const paymentUrl = paymentLink.url || paymentLink.long_url || null;
    const squareOrderId = paymentLink.order_id || (data.related_resources && data.related_resources.orders && data.related_resources.orders[0] && data.related_resources.orders[0].id) || null;

    if (!paymentUrl) {
      if (DEBUG) console.error('[square.checkout] Missing payment link URL in response:', data);
      await pool.execute(
        'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
        ['failed', JSON.stringify(data), billingId]
      );
      return res.status(502).json({ success: false, message: 'Card payment provider did not return a checkout URL' });
    }

    // Record Square payment link for tracking/idempotency
    try {
      await pool.execute(
        'INSERT INTO square_payments (user_id, payment_link_id, order_id, square_order_id, amount, currency, status, credited, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)',
        [userId, String(paymentLink.id || orderId), String(orderId), squareOrderId, amountNum, 'USD', 'pending', JSON.stringify(data)]
      );
    } catch (e) {
      if (DEBUG) console.warn('[square.checkout] Failed to insert square_payments row:', e.message || e);
      // Do not fail the checkout creation if this insert fails; webhook can still be correlated via payment_note/order_id
    }

    return res.json({ success: true, payment_url: paymentUrl });
  } catch (e) {
    if (DEBUG) console.error('[square.checkout] error:', e.response?.data || e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to create card payment' });
  }
}

// Create a Stripe Checkout Session (hosted) for card payments
// Returns a hosted Stripe URL where the user can pay with card.
async function handleStripeCheckout(req, res) {
  let billingId = null;
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });
    if (!STRIPE_SECRET_KEY) {
      return res.status(500).json({ success: false, message: 'Card payments are not configured' });
    }

    const userId = req.session.userId;
    const { amount } = req.body || {};

    const amountNum = parseFloat(amount);
    if (!Number.isFinite(amountNum)) {
      return res.status(400).json({ success: false, message: 'Invalid amount' });
    }
    if (amountNum < CHECKOUT_MIN_AMOUNT || amountNum > CHECKOUT_MAX_AMOUNT) {
      return res.status(400).json({ success: false, message: `Amount must be between $${CHECKOUT_MIN_AMOUNT} and $${CHECKOUT_MAX_AMOUNT}` });
    }

    // Fetch user info for description + email
    let username = '';
    let userEmail = '';
    try {
      const [rows] = await pool.execute('SELECT username, email FROM signup_users WHERE id=? LIMIT 1', [userId]);
      if (rows && rows[0]) {
        if (rows[0].username) username = String(rows[0].username);
        if (rows[0].email) userEmail = String(rows[0].email);
      }
    } catch (e) {
      if (DEBUG) console.warn('[stripe.checkout] Failed to fetch user info:', e.message || e);
    }

    const baseUser = username || (req.session.username || 'unknown');

    // Insert pending billing record (we will mark completed after successful webhook).
    // billing_history.id is our invoice number.
    const [insertResult] = await pool.execute(
      'INSERT INTO billing_history (user_id, amount, description, status) VALUES (?, ?, ?, ?)',
      [userId, amountNum, null, 'pending']
    );
    billingId = insertResult.insertId;

    const desc = buildRefillDescription(billingId);
    try {
      await pool.execute('UPDATE billing_history SET description = ? WHERE id = ?', [desc, billingId]);
    } catch {}

    // Build deterministic order_id encoding userId + billingId for correlation with Stripe
    const orderId = `st-${userId}-${billingId}`;

    const baseUrl = PUBLIC_BASE_URL || `${req.protocol}://${req.get('host')}`;
    const successUrl = joinUrl(baseUrl, '/dashboard?payment=success&method=card');
    const cancelUrl = joinUrl(baseUrl, '/dashboard?payment=cancel&method=card');

    const stripe = getStripeApiClient();
    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      client_reference_id: orderId,
      ...(userEmail ? { customer_email: userEmail } : {}),
      line_items: [
        {
          price_data: {
            currency: 'usd',
            product_data: {
              name: desc.substring(0, 60) || 'Phone.System refill'
            },
            unit_amount: Math.round(amountNum * 100)
          },
          quantity: 1
        }
      ],
      success_url: successUrl,
      cancel_url: cancelUrl,
      metadata: {
        order_id: orderId,
        user_id: String(userId),
        billing_id: String(billingId)
      }
    });

    const paymentUrl = session && session.url ? String(session.url) : null;
    if (!paymentUrl) {
      if (DEBUG) console.error('[stripe.checkout] Missing Checkout Session url in response:', session);
      await pool.execute(
        'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
        ['failed', JSON.stringify(session || {}), billingId]
      );
      return res.status(502).json({ success: false, message: 'Card payment provider did not return a checkout URL' });
    }

    // Record Stripe checkout session for tracking/idempotency
    try {
      await pool.execute(
        'INSERT INTO stripe_payments (user_id, session_id, order_id, payment_intent_id, amount, currency, status, credited, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)',
        [userId, String(session.id || ''), String(orderId), session.payment_intent ? String(session.payment_intent) : null, amountNum, 'USD', 'pending', JSON.stringify(session)]
      );
    } catch (e) {
      if (DEBUG) console.warn('[stripe.checkout] Failed to insert stripe_payments row:', e.message || e);
      // Do not fail the checkout creation if this insert fails; webhook can still be correlated via order_id.
    }

    return res.json({ success: true, payment_url: paymentUrl });
  } catch (e) {
    if (billingId) {
      try {
        await pool.execute(
          'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
          ['failed', String(e?.message || e), billingId]
        );
      } catch {}
    }
    if (DEBUG) console.error('[stripe.checkout] error:', e?.raw?.message || e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to create card payment' });
  }
}

// Provider switch: choose Square or Stripe for card checkout
app.post('/api/me/card/checkout', requireAuth, async (req, res) => {
  const provider = String(process.env.CARD_PAYMENT_PROVIDER || CARD_PAYMENT_PROVIDER || 'square').trim().toLowerCase();
  if (provider === 'stripe') return handleStripeCheckout(req, res);
  if (provider === 'square') return handleSquareCheckout(req, res);
  return res.status(500).json({ success: false, message: 'Card payment provider is not configured' });
});

// Provider-specific endpoints (kept for compatibility)
app.post('/api/me/square/checkout', requireAuth, handleSquareCheckout);
app.post('/api/me/stripe/checkout', requireAuth, handleStripeCheckout);

// NOWPayments IPN webhook
app.post('/nowpayments/ipn', async (req, res) => {
  try {
    if (!pool) {
      return res.status(500).json({ success: false, message: 'Database not configured' });
    }

    // Verify HMAC signature
    const sigOk = verifyNowpaymentsSignature(req);
    if (!sigOk) {
      if (DEBUG) console.warn('[nowpayments.ipn] Invalid signature');
      return res.status(400).json({ success: false, message: 'Invalid signature' });
    }

    const p = req.body || {};
    if (DEBUG) console.log('[nowpayments.ipn] Payload:', JSON.stringify(p));

    const orderId = String(p.order_id || '').trim();
    const paymentStatus = String(p.payment_status || '').toLowerCase();
    const priceAmount = parseFloat(p.price_amount || '0');
    const payAmount = p.pay_amount != null ? parseFloat(p.pay_amount) : null;
    const payCurrency = p.pay_currency || null;
    const remotePaymentId = p.payment_id || p.id || null;

    if (!orderId) {
      if (DEBUG) console.warn('[nowpayments.ipn] Missing order_id in IPN payload');
      return res.status(200).json({ success: true, ignored: true });
    }

    // Order id format: np-<userId>-<billingId>
    const m = /^np-(\d+)-(\d+)$/.exec(orderId);
    if (!m) {
      if (DEBUG) console.warn('[nowpayments.ipn] order_id does not match expected pattern:', orderId);
      return res.status(200).json({ success: true, ignored: true });
    }
    const userId = Number(m[1]);
    const billingId = Number(m[2]);

    // Fetch billing_history row
    const [billingRows] = await pool.execute(
      'SELECT id, user_id, amount, description, status FROM billing_history WHERE id = ? LIMIT 1',
      [billingId]
    );
    const billing = billingRows && billingRows[0];
    if (!billing || String(billing.user_id) !== String(userId)) {
      if (DEBUG) console.warn('[nowpayments.ipn] Billing row not found or user mismatch for order:', { orderId, userId, billingId });
      return res.status(200).json({ success: true, ignored: true });
    }

    // Upsert nowpayments_payments row for tracking
    let existing = null;
    try {
      const [npRows] = await pool.execute(
        'SELECT * FROM nowpayments_payments WHERE order_id = ? LIMIT 1',
        [orderId]
      );
      existing = npRows && npRows[0] ? npRows[0] : null;
    } catch (e) {
      if (DEBUG) console.warn('[nowpayments.ipn] Failed to select nowpayments_payments row:', e.message || e);
    }

    const paymentStatusLabel = paymentStatus || 'unknown';
    const payloadJson = JSON.stringify(p);
    if (existing) {
      try {
        await pool.execute(
          'UPDATE nowpayments_payments SET payment_id = ?, price_amount = ?, price_currency = ?, pay_amount = ?, pay_currency = ?, payment_status = ?, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
          [remotePaymentId || existing.payment_id || orderId, priceAmount || billing.amount, p.price_currency || 'usd', payAmount, payCurrency, paymentStatusLabel, payloadJson, existing.id]
        );
      } catch (e) {
        if (DEBUG) console.warn('[nowpayments.ipn] Failed to update nowpayments_payments row:', e.message || e);
      }
    } else {
      try {
        await pool.execute(
          'INSERT INTO nowpayments_payments (user_id, payment_id, order_id, price_amount, price_currency, pay_amount, pay_currency, payment_status, credited, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)',
          [userId, String(remotePaymentId || orderId), String(orderId), priceAmount || billing.amount, p.price_currency || 'usd', payAmount, payCurrency, paymentStatusLabel, payloadJson]
        );
      } catch (e) {
        if (DEBUG) console.warn('[nowpayments.ipn] Failed to insert nowpayments_payments row:', e.message || e);
      }
    }

    // Handle non-finished statuses: distinguish pending vs terminal failure
    if (paymentStatus !== 'finished') {
      const normalized = paymentStatusLabel.toLowerCase();
      const pendingStatuses = ['waiting', 'confirming', 'confirmed', 'sending'];
      const isPending = pendingStatuses.includes(normalized);

      if (!isPending) {
        // Terminal state: mark billing row as failed
        try {
          await pool.execute(
            'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
            ['failed', payloadJson, billingId]
          );
        } catch (e) {
          if (DEBUG) console.warn('[nowpayments.ipn] Failed to mark billing as failed for order:', orderId, e.message || e);
        }
        if (DEBUG) console.log('[nowpayments.ipn] Terminal status, marking failed:', { orderId, status: paymentStatusLabel });
        return res.status(200).json({ success: true, failed: true });
      }

      if (DEBUG) console.log('[nowpayments.ipn] Payment not finished yet, status=', paymentStatusLabel);
      return res.status(200).json({ success: true, pending: true });
    }

    // Acquire a per-order lock to prevent double-crediting the same NOWPayments order.
    let lockAcquired = false;
    try {
      const [lockResult] = await pool.execute(
        'UPDATE nowpayments_payments SET credited = 2 WHERE order_id = ? AND credited = 0',
        [orderId]
      );
      lockAcquired = !!(lockResult && lockResult.affectedRows > 0);
    } catch (e) {
      if (DEBUG) console.warn('[nowpayments.ipn] Failed to acquire processing lock:', e.message || e);
    }

    if (!lockAcquired) {
      // Someone else has already processed or is processing this order.
      try {
        const [npRows2] = await pool.execute(
          'SELECT credited FROM nowpayments_payments WHERE order_id = ? LIMIT 1',
          [orderId]
        );
        const creditedVal = npRows2 && npRows2[0] ? Number(npRows2[0].credited || 0) : 0;
        if (creditedVal === 1) {
          if (DEBUG) console.log('[nowpayments.ipn] Order already credited, skipping duplicate:', { orderId, billingId });
          return res.status(200).json({ success: true, alreadyCredited: true });
        }
        if (DEBUG) console.log('[nowpayments.ipn] Order is being processed by another handler, skipping duplicate:', { orderId, billingId, credited: creditedVal });
        return res.status(200).json({ success: true, pending: true, duplicate: true });
      } catch (e) {
        if (DEBUG) console.warn('[nowpayments.ipn] Failed to re-read nowpayments_payments for lock check:', e.message || e);
        // Do not risk double-charging if we are unsure.
        return res.status(200).json({ success: true, pending: true });
      }
    }

    // If billing row is already completed, treat this as already credited and
    // simply sync the credited flag.
    if (billing.status === 'completed') {
      if (DEBUG) console.log('[nowpayments.ipn] Billing already completed, skipping refill:', { billingId, orderId });
      try {
        await pool.execute(
          'UPDATE nowpayments_payments SET credited = 1 WHERE order_id = ? AND credited <> 1',
          [orderId]
        );
      } catch (e) {
        if (DEBUG) console.warn('[nowpayments.ipn] Failed to sync credited flag for already-completed billing:', e.message || e);
      }
      return res.status(200).json({ success: true, alreadyCredited: true });
    }

    // Use the amount from billing_history as source of truth (what we asked customer to pay)
    const amountNum = Number(billing.amount || priceAmount || 0);
    const desc = billing.description || p.order_description || `Crypto payment for order ${orderId}`;

    // Credit local balance
    const creditResult = await creditBalance(
      userId,
      amountNum,
      desc,
      'crypto',
      remotePaymentId || 'N/A'
    );

    if (!creditResult.success) {
      if (DEBUG) console.error('[nowpayments.ipn] Failed to credit local balance for order:', { orderId, error: creditResult.error });
      // Release the processing lock so a future IPN can retry.
      try {
        await pool.execute(
          'UPDATE nowpayments_payments SET credited = 0 WHERE order_id = ? AND credited = 2',
          [orderId]
        );
      } catch (e) {
        if (DEBUG) console.warn('[nowpayments.ipn] Failed to revert credited flag after error:', e.message || e);
      }
      return res.status(500).json({ success: false, message: 'Failed to credit balance' });
    }

    // Mark billing_history as completed
    try {
      await pool.execute(
        'UPDATE billing_history SET status = ? WHERE id = ?',
        ['completed', billingId]
      );
    } catch (e) {
      if (DEBUG) console.warn('[nowpayments.ipn] Failed to mark billing as completed:', e.message || e);
    }

    // Mark as credited
    try {
      await pool.execute(
        'UPDATE nowpayments_payments SET credited = 1 WHERE order_id = ?',
        [orderId]
      );
    } catch (e) {
      if (DEBUG) console.warn('[nowpayments.ipn] Failed to mark payment as credited:', e.message || e);
    }

    if (DEBUG) console.log('[nowpayments.ipn] Successfully credited order:', { orderId, billingId, userId });
    return res.status(200).json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[nowpayments.ipn] Unhandled error:', e.message || e);
    return res.status(500).json({ success: false, message: 'IPN handling failed' });
  }
});

// Square webhook for card payments (payment.updated -> COMPLETED)
app.post('/webhooks/square', async (req, res) => {
  try {
    if (!pool) {
      return res.status(500).json({ success: false, message: 'Database not configured' });
    }

    const sigOk = verifySquareSignature(req);
    if (!sigOk) {
      if (DEBUG) console.warn('[square.webhook] Invalid signature');
      return res.status(400).json({ success: false, message: 'Invalid signature' });
    }

    const evt = req.body || {};
    const type = String(evt.type || '').toLowerCase();
    if (type !== 'payment.updated' && type !== 'payment.created') {
      return res.status(200).json({ success: true, ignored: true });
    }

    const dataObj = evt.data || {};
    const payment = dataObj.object && dataObj.object.payment ? dataObj.object.payment : null;
    if (!payment) {
      if (DEBUG) console.warn('[square.webhook] Missing payment object in event');
      return res.status(200).json({ success: true, ignored: true });
    }

    const status = String(payment.status || '').toUpperCase();

    const squareOrderId = payment.order_id || null;
    if (!squareOrderId) {
      if (DEBUG) console.warn('[square.webhook] Missing order_id on payment');
      return res.status(200).json({ success: true, ignored: true });
    }

    // Look up our local square_payments record
    const [sqRows] = await pool.execute(
      'SELECT * FROM square_payments WHERE square_order_id = ? LIMIT 1',
      [squareOrderId]
    );
    const sq = sqRows && sqRows[0] ? sqRows[0] : null;
    if (!sq) {
      if (DEBUG) console.warn('[square.webhook] No local square_payments row for order:', squareOrderId);
      return res.status(200).json({ success: true, ignored: true });
    }

    // Handle non-completed statuses: mark terminal failures
    if (status !== 'COMPLETED') {
      const terminal = ['FAILED', 'CANCELED'];
      const isTerminal = terminal.includes(status);

      if (isTerminal) {
        const localOrderIdFail = String(sq.order_id || '');
        const mFail = /^sq-(\d+)-(\d+)$/.exec(localOrderIdFail);
        if (mFail) {
          const userIdFail = Number(mFail[1]);
          const billingIdFail = Number(mFail[2]);
          try {
            const [bRows] = await pool.execute(
              'SELECT id, user_id, status FROM billing_history WHERE id = ? LIMIT 1',
              [billingIdFail]
            );
            const bRow = bRows && bRows[0] ? bRows[0] : null;
            if (bRow && String(bRow.user_id) === String(userIdFail) && bRow.status !== 'completed') {
              await pool.execute(
                'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
                ['failed', `Square status: ${status}`, billingIdFail]
              );
            }
          } catch (e) {
            if (DEBUG) console.warn('[square.webhook] Failed to mark billing as failed for order:', localOrderIdFail, e.message || e);
          }

          try {
            await pool.execute(
              'UPDATE square_payments SET status = ?, credited = 0 WHERE id = ?',
              [status, sq.id]
            );
          } catch (e) {
            if (DEBUG) console.warn('[square.webhook] Failed to update square_payments for failed status:', e.message || e);
          }

          if (DEBUG) console.log('[square.webhook] Terminal status, marking failed:', { orderId: squareOrderId, status });
          return res.status(200).json({ success: true, failed: true });
        }
      }

      if (DEBUG) console.log('[square.webhook] Payment not completed yet, status=', status);
      return res.status(200).json({ success: true, pending: true });
    }

    // Acquire a per-payment lock to prevent double-crediting the same order.
    let lockAcquired = false;
    try {
      const [lockResult] = await pool.execute(
        'UPDATE square_payments SET status = ?, credited = 2 WHERE id = ? AND credited = 0',
        ['PROCESSING', sq.id]
      );
      lockAcquired = !!(lockResult && lockResult.affectedRows > 0);
    } catch (e) {
      if (DEBUG) console.warn('[square.webhook] Failed to acquire processing lock:', e.message || e);
    }

    if (!lockAcquired) {
      // Someone else has already processed or is processing this payment.
      try {
        const [freshRows] = await pool.execute(
          'SELECT credited FROM square_payments WHERE id = ? LIMIT 1',
          [sq.id]
        );
        const fresh = freshRows && freshRows[0] ? freshRows[0] : null;
        const creditedVal = fresh ? Number(fresh.credited || 0) : Number(sq.credited || 0);
        if (creditedVal === 1) {
          if (DEBUG) console.log('[square.webhook] Payment already credited, skipping duplicate:', { id: sq.id, order_id: sq.order_id });
          return res.status(200).json({ success: true, alreadyCredited: true });
        }
        if (DEBUG) console.log('[square.webhook] Payment is being processed by another handler, skipping duplicate:', { id: sq.id, order_id: sq.order_id, credited: creditedVal });
        return res.status(200).json({ success: true, pending: true, duplicate: true });
      } catch (e) {
        if (DEBUG) console.warn('[square.webhook] Failed to re-read square_payments for lock check:', e.message || e);
        // Do not risk double-charging if we are unsure.
        return res.status(200).json({ success: true, pending: true });
      }
    }

    const localOrderId = String(sq.order_id || '');
    const m = /^sq-(\d+)-(\d+)$/.exec(localOrderId);
    if (!m) {
      if (DEBUG) console.warn('[square.webhook] order_id does not match expected pattern:', localOrderId);
      return res.status(200).json({ success: true, ignored: true });
    }
    const userId = Number(m[1]);
    const billingId = Number(m[2]);

    // Fetch billing_history row
    const [billingRows] = await pool.execute(
      'SELECT id, user_id, amount, description, status FROM billing_history WHERE id = ? LIMIT 1',
      [billingId]
    );
    const billing = billingRows && billingRows[0] ? billingRows[0] : null;
    if (!billing || String(billing.user_id) !== String(userId)) {
      if (DEBUG) console.warn('[square.webhook] Billing row not found or user mismatch for order:', { localOrderId, userId, billingId });
      return res.status(200).json({ success: true, ignored: true });
    }

    // If billing row is already completed, treat this as already credited
    if (billing.status === 'completed') {
      if (DEBUG) console.log('[square.webhook] Billing already completed, skipping refill:', { billingId, localOrderId });
      try {
        await pool.execute(
          'UPDATE square_payments SET status = ?, credited = 1 WHERE id = ? AND credited <> 1',
          ['COMPLETED', sq.id]
        );
      } catch (e) {
        if (DEBUG) console.warn('[square.webhook] Failed to sync credited flag for already-completed billing:', e.message || e);
      }
      return res.status(200).json({ success: true, alreadyCredited: true });
    }

    // Use the amount from billing_history as source of truth (what we asked customer to pay)
    const amountNum = Number(billing.amount || 0);
    const desc = billing.description || `Square payment for order ${localOrderId}`;
    const paymentId = String(payment.id || sq.square_payment_id || '').trim() || 'N/A';

    // Credit local balance
    const creditResult = await creditBalance(
      userId,
      amountNum,
      desc,
      'card',
      paymentId
    );

    if (!creditResult.success) {
      if (DEBUG) console.error('[square.webhook] Failed to credit local balance for order:', { localOrderId, error: creditResult.error });
      // Release the processing lock so a future retry can attempt again.
      try {
        await pool.execute(
          'UPDATE square_payments SET status = ?, credited = 0 WHERE id = ? AND credited = 2',
          ['FAILED', sq.id]
        );
      } catch (e) {
        if (DEBUG) console.warn('[square.webhook] Failed to revert credited flag after error:', e.message || e);
      }
      return res.status(500).json({ success: false, message: 'Failed to credit balance' });
    }

    // Mark billing_history as completed
    try {
      await pool.execute(
        'UPDATE billing_history SET status = ? WHERE id = ?',
        ['completed', billingId]
      );
    } catch (e) {
      if (DEBUG) console.warn('[square.webhook] Failed to mark billing as completed:', e.message || e);
    }

    // Mark as credited and save latest payload
    try {
      await pool.execute(
        'UPDATE square_payments SET status = ?, square_payment_id = ?, credited = 1, raw_payload = ? WHERE id = ?',
        ['COMPLETED', String(payment.id || sq.square_payment_id || ''), JSON.stringify(evt), sq.id]
      );
    } catch (e) {
      if (DEBUG) console.warn('[square.webhook] Failed to mark payment as credited:', e.message || e);
    }

    if (DEBUG) console.log('[square.webhook] Successfully credited order:', { localOrderId, billingId, userId });
    return res.status(200).json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[square.webhook] Unhandled error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Webhook handling failed' });
  }
});

// Stripe webhook for card payments (Checkout Session)
// Configure in Stripe dashboard to send at least:
// - checkout.session.completed
// - checkout.session.expired (optional)
app.post('/webhooks/stripe', async (req, res) => {
  try {
    if (!pool) {
      return res.status(500).json({ success: false, message: 'Database not configured' });
    }

    const raw = req.rawBody;
    if (!raw || !Buffer.isBuffer(raw)) {
      if (DEBUG) console.warn('[stripe.webhook] Missing rawBody');
      return res.status(400).json({ success: false, message: 'Invalid payload' });
    }

    const webhookSecret = String(STRIPE_WEBHOOK_SECRET || '').trim();
    let event = null;

    if (webhookSecret) {
      const sig = req.headers['stripe-signature'];
      if (!sig) {
        if (DEBUG) console.warn('[stripe.webhook] Missing stripe-signature header');
        return res.status(400).json({ success: false, message: 'Missing signature' });
      }
      try {
        const stripe = getStripeWebhookClient();
        event = stripe.webhooks.constructEvent(raw, String(sig), webhookSecret);
      } catch (e) {
        if (DEBUG) console.warn('[stripe.webhook] Signature verification failed:', e?.message || e);
        return res.status(400).json({ success: false, message: 'Invalid signature' });
      }
    } else {
      if (DEBUG) console.warn('[stripe.webhook] STRIPE_WEBHOOK_SECRET not set; skipping signature verification');
      try {
        event = JSON.parse(raw.toString('utf8'));
      } catch (e) {
        return res.status(400).json({ success: false, message: 'Invalid JSON' });
      }
    }

    const type = String(event?.type || '').toLowerCase();
    if (!type.startsWith('checkout.session.')) {
      return res.status(200).json({ success: true, ignored: true });
    }

    const session = event?.data?.object || null;
    if (!session || typeof session !== 'object') {
      return res.status(200).json({ success: true, ignored: true });
    }

    const sessionId = String(session.id || '').trim();
    const orderId = String(session.client_reference_id || session.metadata?.order_id || '').trim();
    const paymentStatus = String(session.payment_status || '').toLowerCase();
    const paymentIntentId = session.payment_intent ? String(session.payment_intent) : null;

    const payloadJson = JSON.stringify(event);

    // Best-effort: save latest event payload for debugging
    if (sessionId) {
      try {
        await pool.execute(
          'UPDATE stripe_payments SET status = ?, payment_intent_id = COALESCE(?, payment_intent_id), raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE session_id = ? LIMIT 1',
          [type, paymentIntentId, payloadJson, sessionId]
        );
      } catch {}
    }

    if (!sessionId || !orderId) {
      if (DEBUG) console.warn('[stripe.webhook] Missing sessionId/orderId:', { sessionId, orderId });
      return res.status(200).json({ success: true, ignored: true });
    }

    const m = /^st-(\d+)-(\d+)$/.exec(orderId);
    if (!m) {
      if (DEBUG) console.warn('[stripe.webhook] order_id does not match expected pattern:', orderId);
      return res.status(200).json({ success: true, ignored: true });
    }

    const userId = Number(m[1]);
    const billingId = Number(m[2]);

    // Terminal failure: session expired
    if (type === 'checkout.session.expired') {
      try {
        const [bRows] = await pool.execute(
          'SELECT id, user_id, status FROM billing_history WHERE id = ? LIMIT 1',
          [billingId]
        );
        const bRow = bRows && bRows[0] ? bRows[0] : null;
        if (bRow && String(bRow.user_id) === String(userId) && bRow.status !== 'completed') {
          await pool.execute(
            'UPDATE billing_history SET status = ?, error = ? WHERE id = ?',
            ['failed', 'Stripe status: expired', billingId]
          );
        }
      } catch {}

      try {
        await pool.execute(
          'UPDATE stripe_payments SET status = ?, credited = 0, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE session_id = ? LIMIT 1',
          ['EXPIRED', payloadJson, sessionId]
        );
      } catch {}

      return res.status(200).json({ success: true, failed: true });
    }

    // Only credit on completed + paid
    if (type !== 'checkout.session.completed') {
      return res.status(200).json({ success: true, ignored: true });
    }

    if (paymentStatus !== 'paid') {
      if (DEBUG) console.log('[stripe.webhook] Session completed but not paid yet:', { sessionId, paymentStatus });
      return res.status(200).json({ success: true, pending: true });
    }

    // Fetch billing_history row
    const [billingRows] = await pool.execute(
      'SELECT id, user_id, amount, description, status FROM billing_history WHERE id = ? LIMIT 1',
      [billingId]
    );
    const billing = billingRows && billingRows[0] ? billingRows[0] : null;
    if (!billing || String(billing.user_id) !== String(userId)) {
      if (DEBUG) console.warn('[stripe.webhook] Billing row not found or user mismatch for order:', { orderId, userId, billingId });
      return res.status(200).json({ success: true, ignored: true });
    }

    // Acquire a per-session lock to prevent double-crediting.
    let lockAcquired = false;
    try {
      const [lockResult] = await pool.execute(
        'UPDATE stripe_payments SET status = ?, credited = 2, payment_intent_id = COALESCE(?, payment_intent_id), raw_payload = ? WHERE session_id = ? AND credited = 0',
        ['PROCESSING', paymentIntentId, payloadJson, sessionId]
      );
      lockAcquired = !!(lockResult && lockResult.affectedRows > 0);
    } catch (e) {
      if (DEBUG) console.warn('[stripe.webhook] Failed to acquire processing lock:', e.message || e);
    }

    if (!lockAcquired) {
      // If the session row was not inserted at checkout time, insert a lock row now.
      try {
        await pool.execute(
          'INSERT INTO stripe_payments (user_id, session_id, order_id, payment_intent_id, amount, currency, status, credited, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, 2, ?)',
          [userId, sessionId, orderId, paymentIntentId, Number(billing.amount || 0), 'USD', 'PROCESSING', payloadJson]
        );
        lockAcquired = true;
      } catch (e) {
        if (e?.code !== 'ER_DUP_ENTRY' && DEBUG) console.warn('[stripe.webhook] Failed to insert lock row:', e.message || e);
      }
    }

    if (!lockAcquired) {
      // Someone else has already processed or is processing.
      try {
        const [freshRows] = await pool.execute(
          'SELECT credited FROM stripe_payments WHERE session_id = ? LIMIT 1',
          [sessionId]
        );
        const creditedVal = freshRows && freshRows[0] ? Number(freshRows[0].credited || 0) : 0;
        if (creditedVal === 1) {
          return res.status(200).json({ success: true, alreadyCredited: true });
        }
        return res.status(200).json({ success: true, pending: true, duplicate: true });
      } catch {
        return res.status(200).json({ success: true, pending: true });
      }
    }

    // If billing row is already completed, treat this as already credited.
    if (billing.status === 'completed') {
      try {
        await pool.execute(
          'UPDATE stripe_payments SET status = ?, credited = 1, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE session_id = ? AND credited <> 1',
          ['COMPLETED', payloadJson, sessionId]
        );
      } catch {}
      return res.status(200).json({ success: true, alreadyCredited: true });
    }

    // Use amount from billing_history as source of truth
    const amountNum = Number(billing.amount || 0);
    const desc = billing.description || `Stripe payment for order ${orderId}`;

    // Credit local balance
    const creditResult = await creditBalance(
      userId,
      amountNum,
      desc,
      'card',
      paymentIntentId || sessionId
    );

    if (!creditResult.success) {
      if (DEBUG) console.error('[stripe.webhook] Failed to credit local balance for order:', { orderId, error: creditResult.error });
      // Release the processing lock so a future retry can attempt again.
      try {
        await pool.execute(
          'UPDATE stripe_payments SET status = ?, credited = 0, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE session_id = ? AND credited = 2',
          ['FAILED', payloadJson, sessionId]
        );
      } catch {}
      return res.status(500).json({ success: false, message: 'Failed to credit balance' });
    }

    // Mark billing_history as completed
    try {
      await pool.execute(
        'UPDATE billing_history SET status = ? WHERE id = ?',
        ['completed', billingId]
      );
    } catch (e) {
      if (DEBUG) console.warn('[stripe.webhook] Failed to mark billing as completed:', e.message || e);
    }

    // Mark as credited
    try {
      await pool.execute(
        'UPDATE stripe_payments SET status = ?, payment_intent_id = COALESCE(?, payment_intent_id), credited = 1, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE session_id = ?',
        ['COMPLETED', paymentIntentId, payloadJson, sessionId]
      );
    } catch (e) {
      if (DEBUG) console.warn('[stripe.webhook] Failed to mark payment as credited:', e.message || e);
    }

    if (DEBUG) console.log('[stripe.webhook] Successfully credited order:', { orderId, billingId, userId });
    return res.status(200).json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[stripe.webhook] Unhandled error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Webhook handling failed' });
  }
});

// Bill.com / BILL webhook (invoice updated)
// Configure BILL connect-events subscription to include invoice.updated and point it to:
//   PUBLIC_BASE_URL + /webhooks/billcom
app.post('/webhooks/billcom', async (req, res) => {
  try {
    if (!pool) {
      return res.status(500).json({ success: false, message: 'Database not configured' });
    }

    const sigOk = verifyBillcomWebhookSignature(req);
    if (!sigOk) {
      if (DEBUG) console.warn('[billcom.webhook] Invalid signature');
      return res.status(400).json({ success: false, message: 'Invalid signature' });
    }

    const evt = req.body || {};
    const meta = evt.metadata || {};
    const eventTypeRaw = meta.eventType || evt.eventType || evt.type || '';
    const eventType = String(eventTypeRaw || '').trim().toLowerCase();

    // We only care about invoice updates.
    if (!eventType.startsWith('invoice.')) {
      return res.status(200).json({ success: true, ignored: true });
    }

    const inv = evt.invoice || evt.payload?.invoice || null;
    if (!inv || typeof inv !== 'object') {
      if (DEBUG) console.warn('[billcom.webhook] Missing invoice object');
      return res.status(200).json({ success: true, ignored: true });
    }

    const invoiceId = String(inv.id || '').trim();
    const status = String(inv.status || '').toUpperCase();
    const invoiceNumber = String(inv.invoiceNumber || '').trim();
    const dueAmount = (inv.dueAmount != null && Number.isFinite(Number(inv.dueAmount))) ? Number(inv.dueAmount) : null;

    const payloadJson = JSON.stringify(evt);

    // Best-effort: sync status/payload into billcom_payments
    if (invoiceId) {
      try {
        await pool.execute(
          'UPDATE billcom_payments SET status = ?, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE invoice_id = ? LIMIT 1',
          [status || eventType || 'unknown', payloadJson, invoiceId]
        );
      } catch {}
    }

    // Only credit on paid in full (dueAmount <= 0 as extra guard)
    const isPaidInFull = (status === 'PAID_IN_FULL') || (dueAmount != null && dueAmount <= 0 && status.startsWith('PAID'));
    if (!isPaidInFull) {
      return res.status(200).json({ success: true, pending: true });
    }

    // First try: find our local billcom_payments row by invoice_id
    let bc = null;
    if (invoiceId) {
      try {
        const [rows] = await pool.execute(
          'SELECT * FROM billcom_payments WHERE invoice_id = ? LIMIT 1',
          [invoiceId]
        );
        bc = rows && rows[0] ? rows[0] : null;
      } catch {}
    }

    // Fallback: parse billing id from invoiceNumber (we set invoiceNumber = billingId)
    let billingId = null;
    let userId = null;
    let orderId = null;

    if (bc) {
      userId = Number(bc.user_id);
      orderId = String(bc.order_id || '').trim() || null;
      const m = orderId ? /^bc-(\d+)-(\d+)$/.exec(orderId) : null;
      if (m) billingId = Number(m[2]);
    }

    if (!billingId && invoiceNumber && /^\d+$/.test(invoiceNumber)) {
      billingId = Number(invoiceNumber);
    }

    if (!userId && billingId) {
      try {
        const [bRows] = await pool.execute(
          'SELECT user_id FROM billing_history WHERE id = ? LIMIT 1',
          [billingId]
        );
        const b = bRows && bRows[0] ? bRows[0] : null;
        if (b && b.user_id != null) userId = Number(b.user_id);
      } catch {}
    }

    if (!userId || !billingId) {
      if (DEBUG) console.warn('[billcom.webhook] Could not resolve userId/billingId:', { invoiceId, invoiceNumber, status });
      return res.status(200).json({ success: true, ignored: true });
    }

    // Acquire a per-invoice lock to prevent double-crediting.
    let lockAcquired = false;
    if (invoiceId) {
      try {
        const [lockResult] = await pool.execute(
          'UPDATE billcom_payments SET status = ?, credited = 2, raw_payload = ? WHERE invoice_id = ? AND credited = 0',
          ['PROCESSING', payloadJson, invoiceId]
        );
        lockAcquired = !!(lockResult && lockResult.affectedRows > 0);
      } catch {}
    }

    if (!lockAcquired) {
      // If row missing (or insert failed earlier), insert a lock row now.
      if (invoiceId) {
        try {
          const insertOrderId = orderId || `bc-${userId}-${billingId}`;
          await pool.execute(
            'INSERT INTO billcom_payments (user_id, invoice_id, customer_id, order_id, payment_link, status, credited, raw_payload) VALUES (?, ?, ?, ?, ?, ?, 2, ?)',
            [userId, invoiceId, inv.customerId ? String(inv.customerId) : null, insertOrderId, null, 'PROCESSING', payloadJson]
          );
          lockAcquired = true;
        } catch (e) {
          if (e?.code !== 'ER_DUP_ENTRY' && DEBUG) console.warn('[billcom.webhook] Failed to insert lock row:', e.message || e);
        }
      }
    }

    if (!lockAcquired) {
      // Someone else has already processed or is processing.
      try {
        if (invoiceId) {
          const [freshRows] = await pool.execute('SELECT credited FROM billcom_payments WHERE invoice_id = ? LIMIT 1', [invoiceId]);
          const creditedVal = freshRows && freshRows[0] ? Number(freshRows[0].credited || 0) : 0;
          if (creditedVal === 1) return res.status(200).json({ success: true, alreadyCredited: true });
        }
      } catch {}
      return res.status(200).json({ success: true, pending: true, duplicate: true });
    }

    // Fetch billing_history row
    const [billingRows] = await pool.execute(
      'SELECT id, user_id, amount, description, status FROM billing_history WHERE id = ? LIMIT 1',
      [billingId]
    );
    const billing = billingRows && billingRows[0] ? billingRows[0] : null;
    if (!billing || String(billing.user_id) !== String(userId)) {
      return res.status(200).json({ success: true, ignored: true });
    }

    // If already completed, just mark provider row as credited.
    if (billing.status === 'completed') {
      try {
        if (invoiceId) {
          await pool.execute(
            'UPDATE billcom_payments SET status = ?, credited = 1, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE invoice_id = ? AND credited <> 1',
            ['COMPLETED', payloadJson, invoiceId]
          );
        }
      } catch {}
      return res.status(200).json({ success: true, alreadyCredited: true });
    }

    // Fetch user row for email/name
    const [userRows] = await pool.execute(
      'SELECT id, username, firstname, lastname, email FROM signup_users WHERE id = ? LIMIT 1',
      [userId]
    );
    const user = userRows && userRows[0] ? userRows[0] : null;
    if (!user) {
      try { if (invoiceId) await pool.execute('UPDATE billcom_payments SET credited = 0, status = ? WHERE invoice_id = ? AND credited = 2', ['FAILED', invoiceId]); } catch {}
      return res.status(200).json({ success: true, ignored: true });
    }

    const nameBase = user.username || '';
    const fullName = `${user.firstname || ''} ${user.lastname || ''}`.trim();
    const displayName = fullName || nameBase || 'Customer';
    const userEmail = user.email || '';

    const amountNum = Number(billing.amount || 0);
    const desc = billing.description || buildRefillDescription(billingId);
    const result = await creditBalance(userId, amountNum, desc, paymentMethod, processorTransactionId);

    if (!result.success) {
      // Release the processing lock so a future retry can attempt again.
      try {
        if (invoiceId) {
          await pool.execute(
            'UPDATE billcom_payments SET status = ?, credited = 0, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE invoice_id = ? AND credited = 2',
            ['FAILED', payloadJson, invoiceId]
          );
        }
      } catch {}
      return res.status(500).json({ success: false, message: 'Failed to credit balance' });
    }

    // Mark as credited
    try {
      if (invoiceId) {
        await pool.execute(
          'UPDATE billcom_payments SET status = ?, credited = 1, raw_payload = ?, updated_at = CURRENT_TIMESTAMP WHERE invoice_id = ?',
          ['COMPLETED', payloadJson, invoiceId]
        );
      }
    } catch {}

    return res.status(200).json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[billcom.webhook] Unhandled error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Webhook handling failed' });
  }
});


async function buildCallerMemoryForDialin({ userId, agentId, fromDigits, excludeCallId, excludeCallDomain }) {
  try {
    if (!pool) return null;

    const u = Number(userId);
    const a = Number(agentId);
    const digits = String(fromDigits || '').replace(/[^0-9]/g, '').trim();

    if (!u || !a || !digits || digits.length < 7) return null;
    if (!AI_CALLER_MEMORY_ENABLE) return null;
    if (AI_CALLER_MEMORY_MAX_MESSAGES <= 0) return null;

    const maxCalls = Math.min(5, Math.max(1, AI_CALLER_MEMORY_MAX_CALLS));
    const maxMessages = Math.min(50, Math.max(1, AI_CALLER_MEMORY_MAX_MESSAGES));
    const maxChars = Math.min(2000, Math.max(50, AI_CALLER_MEMORY_MAX_CHARS_PER_MESSAGE));
    const maxDays = Math.min(3650, Math.max(1, AI_CALLER_MEMORY_MAX_DAYS));

    const cutoff = new Date(Date.now() - (maxDays * 24 * 60 * 60 * 1000));

    const excludeId = String(excludeCallId || '').trim();
    const excludeDomain = String(excludeCallDomain || '').trim();

    const digitsLast10 = (digits.length >= 10) ? digits.slice(-10) : '';

    // First attempt: exact digits-only match.
    let calls = [];

    const [callRowsExact] = await pool.execute(
      `SELECT call_id, call_domain, time_start, time_end
       FROM ai_call_logs
       WHERE user_id = ?
         AND agent_id = ?
         AND from_number IS NOT NULL
         AND from_number <> ''
         AND REGEXP_REPLACE(from_number, '[^0-9]', '') = ?
         AND NOT (call_id = ? AND call_domain = ?)
         AND (status IS NULL OR status NOT LIKE 'blocked%')
         AND (time_start IS NULL OR time_start >= ?)
       ORDER BY COALESCE(time_end, time_start) DESC, id DESC
       LIMIT ${maxCalls}`,
      [u, a, digits, excludeId, excludeDomain, cutoff]
    );

    calls = Array.isArray(callRowsExact) ? callRowsExact : [];

    // Fallback: match by last 10 digits (helps when the caller ID flips between +1XXXXXXXXXX and XXXXXXXXXX).
    if (!calls.length && digitsLast10 && digitsLast10.length === 10) {
      const [callRowsLast10] = await pool.execute(
        `SELECT call_id, call_domain, time_start, time_end
         FROM ai_call_logs
         WHERE user_id = ?
           AND agent_id = ?
           AND from_number IS NOT NULL
           AND from_number <> ''
           AND RIGHT(REGEXP_REPLACE(from_number, '[^0-9]', ''), 10) = ?
           AND NOT (call_id = ? AND call_domain = ?)
           AND (status IS NULL OR status NOT LIKE 'blocked%')
           AND (time_start IS NULL OR time_start >= ?)
         ORDER BY COALESCE(time_end, time_start) DESC, id DESC
         LIMIT ${maxCalls}`,
        [u, a, digitsLast10, excludeId, excludeDomain, cutoff]
      );
      calls = Array.isArray(callRowsLast10) ? callRowsLast10 : [];
    }

    if (!calls.length) return null;

    // Pull memory from the newest prior call that actually has logged transcript messages.
    for (const c of calls) {
      const callId = String(c?.call_id || '').trim();
      const callDomain = String(c?.call_domain || '').trim();
      if (!callId || !callDomain) continue;

      const [msgRows] = await pool.execute(
        `SELECT role, content
         FROM ai_call_messages
         WHERE user_id = ? AND agent_id = ? AND call_id = ? AND call_domain = ?
         ORDER BY id DESC
         LIMIT ${maxMessages}`,
        [u, a, callId, callDomain]
      );

      const rows = Array.isArray(msgRows) ? msgRows : [];
      if (!rows.length) continue;

      const messages = rows
        .slice()
        .reverse()
        .map(r => {
          const roleRaw = String(r.role || '').trim().toLowerCase();
          const role = (roleRaw === 'assistant') ? 'assistant' : (roleRaw === 'user' ? 'user' : '');
          let content = String(r.content || '').trim();
          if (!role || !content) return null;
          if (content.length > maxChars) content = content.slice(0, maxChars);
          return { role, content };
        })
        .filter(Boolean);

      if (!messages.length) continue;

      let lastCallStartedAt = null;
      let lastCallEndedAt = null;

      try {
        if (c.time_start) {
          const d = (c.time_start instanceof Date) ? c.time_start : new Date(c.time_start);
          if (!Number.isNaN(d.getTime())) lastCallStartedAt = d.toISOString();
        }
      } catch {}

      try {
        if (c.time_end) {
          const d = (c.time_end instanceof Date) ? c.time_end : new Date(c.time_end);
          if (!Number.isNaN(d.getTime())) lastCallEndedAt = d.toISOString();
        }
      } catch {}

      const metaParts = [
        'Returning caller detected. The following messages are from a previous phone call with this same caller.',
        'Use them as background context. Do not mention storing transcripts; just be helpful.'
      ];

      if (lastCallStartedAt) metaParts.push(`Previous call started: ${lastCallStartedAt}.`);
      if (lastCallEndedAt) metaParts.push(`Previous call ended: ${lastCallEndedAt}.`);

      return {
        meta: metaParts.join(' '),
        messages
      };
    }

    return null;
  } catch (e) {
    if (DEBUG) console.warn('[callerMemory] buildCallerMemoryForDialin failed:', e?.message || e);
    return null;
  }
}

// Daily pinless dial-in webhook (room_creation_api) -> start Pipecat Cloud session.
// This lets us log inbound AI calls into the user's Call History.
app.post('/webhooks/daily/dialin/:agentName', async (req, res) => {
  try {
    const agentName = String(req.params.agentName || '').trim();
    if (!agentName) return res.status(400).json({ success: false, message: 'Missing agentName' });

    // Optional shared-secret guard (recommended if you use the local webhook).
    if (DAILY_DIALIN_WEBHOOK_TOKEN) {
      const token = String(req.query.token || '').trim();
      if (!token || token !== DAILY_DIALIN_WEBHOOK_TOKEN) {
        if (DEBUG) console.warn('[daily.dialin] Invalid token');
        return res.status(403).json({ success: false, message: 'Forbidden' });
      }
    }

    const body = (req.body && typeof req.body === 'object') ? req.body : {};

    // Daily payload (per Daily/Pipecat docs) uses capitalized keys: To / From / callId / callDomain.
    const toNumber = String(body.To || body.to || body.to_number || body.toNumber || '').trim();
    const fromNumber = String(body.From || body.from || body.from_number || body.fromNumber || '').trim();
    const callId = String(body.callId || body.call_id || body.callID || '').trim();
    const callDomain = String(body.callDomain || body.call_domain || '').trim();

    if (!callId || !callDomain) {
      if (DEBUG) console.warn('[daily.dialin] Missing callId/callDomain in webhook payload');
      return res.status(400).json({ success: false, message: 'Missing callId/callDomain' });
    }

    // Best-effort lookup so we can attribute calls to the correct user.
    let agentRow = null;
    let aiNumberRow = null;
    let shouldBlock = false;
    let blockReason = null;
    let credit = null;
    if (pool) {
      try {
        const [rows] = await pool.execute(
          'SELECT id, user_id FROM ai_agents WHERE pipecat_agent_name = ? LIMIT 1',
          [agentName]
        );
        agentRow = rows && rows[0] ? rows[0] : null;
      } catch (e) {
        if (DEBUG) console.warn('[daily.dialin] Failed to lookup agent:', e.message || e);
      }

      if (agentRow) {
        try {
          const [nRows] = await pool.execute(
            'SELECT id, phone_number FROM ai_numbers WHERE agent_id = ? AND user_id = ? LIMIT 1',
            [agentRow.id, agentRow.user_id]
          );
          aiNumberRow = nRows && nRows[0] ? nRows[0] : null;
        } catch (e) {
          if (DEBUG) console.warn('[daily.dialin] Failed to lookup ai_number:', e.message || e);
        }

        // Balance gate: block inbound AI calls when user's credit is below threshold.
        try {
          credit = await getUserBalance(agentRow.user_id);
          credit = Number.isFinite(credit) ? Number(credit) : null;
        } catch (e) {
          if (DEBUG) console.warn('[daily.dialin] Failed to fetch balance:', e.message || e);
          credit = null;
        }

        if (credit != null) {
          if (credit < AI_INBOUND_MIN_CREDIT) {
            shouldBlock = true;
            blockReason = 'insufficient_funds';
          }
        } else if (AI_INBOUND_BALANCE_FAIL_CLOSED) {
          shouldBlock = true;
          blockReason = 'credit_unavailable';
        }
      }

      // Insert/update call log (idempotent via UNIQUE(call_domain, call_id)).
      if (agentRow) {
        const initialStatus = shouldBlock
          ? (blockReason === 'insufficient_funds' ? 'blocked_insufficient_funds' : 'blocked_balance_check_failed')
          : 'webhook_received';

        try {
          await pool.execute(
            `INSERT INTO ai_call_logs (call_id, call_domain, user_id, agent_id, ai_number_id, direction, from_number, to_number, time_start, status, raw_payload)
             VALUES (?, ?, ?, ?, ?, 'inbound', ?, ?, NOW(), ?, ?)
             ON DUPLICATE KEY UPDATE
               updated_at = CURRENT_TIMESTAMP,
               from_number = COALESCE(NULLIF(VALUES(from_number), ''), from_number),
               to_number = COALESCE(NULLIF(VALUES(to_number), ''), to_number)`,
            [
              callId,
              callDomain,
              agentRow.user_id,
              agentRow.id,
              aiNumberRow ? aiNumberRow.id : null,
              fromNumber || null,
              toNumber || (aiNumberRow ? aiNumberRow.phone_number : null),
              initialStatus,
              JSON.stringify(body)
            ]
          );
        } catch (e) {
          if (DEBUG) console.warn('[daily.dialin] Failed to insert ai_call_logs row:', e.message || e);
        }
      }
    }

    if (shouldBlock && agentRow) {
      if (DEBUG) {
        console.warn('[daily.dialin] Blocking inbound AI call due to balance gate:', {
          userId: agentRow.user_id,
          agentName,
          callId,
          callDomain,
          credit,
          minCredit: AI_INBOUND_MIN_CREDIT,
          blockReason
        });
      }

      // Best-effort: disable AI number routing while the user is blocked.
      // Only do this when we *confirmed* the balance is below threshold.
      if (AI_INBOUND_DISABLE_NUMBERS_WHEN_BALANCE_LOW && blockReason === 'insufficient_funds') {
        const localUserId = agentRow.user_id;

        // If the balance went NEGATIVE, also clear the UI assignment until the user adds funds.
        if (credit != null && Number.isFinite(credit) && credit < 0) {
          unassignAiNumbersForUser({ localUserId }).catch(() => {});
        } else {
          disableAiDialinRoutingForUser({ localUserId }).catch(() => {});
        }
      }

      // Return non-2xx so Daily rejects the call.
      return res.status(402).json({ success: false, message: 'Insufficient balance' });
    }

    // Trigger Pipecat Cloud to start the dial-in session for this agent.
    // Pipecat Cloud handles creating the Daily room and connecting the PSTN call.
    try {
      let callerMemory = null;
      let fromDigits = '';
      try {
        if (pool && agentRow && fromNumber) {
          fromDigits = digitsOnly(fromNumber);
          if (fromDigits) {
            callerMemory = await buildCallerMemoryForDialin({
              userId: agentRow.user_id,
              agentId: agentRow.id,
              fromDigits,
              excludeCallId: callId,
              excludeCallDomain: callDomain
            });
          }
        }
      } catch (e) {
        if (DEBUG) console.warn('[daily.dialin] Caller memory lookup failed:', e?.message || e);
        callerMemory = null;
      }

      if (DEBUG && fromDigits) {
        const last4 = fromDigits.length >= 4 ? fromDigits.slice(-4) : fromDigits;
        const msgCount = Array.isArray(callerMemory?.messages) ? callerMemory.messages.length : 0;
        console.log('[daily.dialin] callerMemory', {
          agentName,
          callId,
          callDomain,
          fromLast4: last4,
          hasMemory: !!callerMemory,
          messages: msgCount
        });
      }

      // Fetch agent's inbound transfer settings
      let agentConfig = null;
      if (pool && agentRow) {
        try {
          const [agentRows] = await pool.execute(
            'SELECT inbound_transfer_enabled, inbound_transfer_number FROM ai_agents WHERE id = ? LIMIT 1',
            [agentRow.id]
          );
          const agentData = agentRows && agentRows[0] ? agentRows[0] : null;
          if (agentData) {
            agentConfig = {
              inbound_transfer_enabled: Boolean(agentData.inbound_transfer_enabled),
              inbound_transfer_number: agentData.inbound_transfer_number || null
            };
          }
        } catch (e) {
          if (DEBUG) console.warn('[daily.dialin] Failed to fetch agent config:', e?.message || e);
        }
      }

      const startBody = {
        createDailyRoom: true,
        dailyRoomProperties: {
          sip: {
            display_name: fromNumber || 'Caller',
            sip_mode: 'dial-in',
            num_endpoints: 1
          }
        },
        body: {
          dialin_settings: {
            from: fromNumber,
            to: toNumber,
            call_id: callId,
            call_domain: callDomain
          },
          ...(callerMemory ? { caller_memory: callerMemory } : {}),
          ...(agentConfig ? { agent_config: agentConfig } : {})
        }
      };

      await pipecatApiCall({
        method: 'POST',
        path: `/public/${encodeURIComponent(agentName)}/start`,
        body: startBody
      });

      if (pool && agentRow) {
        try {
          await pool.execute(
            'UPDATE ai_call_logs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE call_id = ? AND call_domain = ? AND user_id = ? LIMIT 1',
            ['pipecat_started', callId, callDomain, agentRow.user_id]
          );
        } catch (e) {
          if (DEBUG) console.warn('[daily.dialin] Failed to update ai_call_logs status:', e.message || e);
        }
      }
    } catch (e) {
      if (DEBUG) console.error('[daily.dialin] Failed to start Pipecat dial-in session:', e?.data || e?.message || e);

      if (pool && agentRow) {
        try {
          await pool.execute(
            'UPDATE ai_call_logs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE call_id = ? AND call_domain = ? AND user_id = ? LIMIT 1',
            ['pipecat_start_failed', callId, callDomain, agentRow.user_id]
          );
        } catch {}
      }

      // Return non-2xx so Daily can retry.
      return res.status(502).json({ success: false, message: 'Failed to start Pipecat dial-in session' });
    }

    return res.status(200).json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[daily.dialin] Unhandled error:', e.message || e);
    // Return 200 to avoid webhook retry storms on unexpected errors.
    return res.status(200).json({ success: true });
  }
});

// Pipecat agent callback for dialout calls completing.
// The Pipecat bot sends this when a dialout call ends, allowing us to update call status
// without relying on Daily webhooks (which need to be configured separately).
app.post('/webhooks/pipecat/dialout-completed', async (req, res) => {
  try {
    if (!pool) return res.status(200).json({ success: true });

    const callId = String(req.body?.call_id || '').trim();
    const result = String(req.body?.result || '').trim();
    const durationSec = req.body?.duration_sec != null ? Number(req.body.duration_sec) : null;

    if (!callId) {
      if (DEBUG) console.warn('[pipecat.dialout-completed] Missing call_id');
      return res.status(400).json({ success: false, message: 'Missing call_id' });
    }

    if (DEBUG) {
      console.log('[pipecat.dialout-completed] Received:', { callId, result, durationSec });
    }

    // Find the call log by call_id
    const [logRows] = await pool.execute(
      'SELECT id, lead_id, user_id FROM dialer_call_logs WHERE call_id = ? ORDER BY id DESC LIMIT 1',
      [callId]
    );
    const logRow = logRows && logRows[0] ? logRows[0] : null;

    if (!logRow) {
      if (DEBUG) console.warn('[pipecat.dialout-completed] Call log not found:', callId);
      return res.status(404).json({ success: false, message: 'Call log not found' });
    }

    // Update call log
    const status = 'completed';
    const resultVal = result || 'answered';
    await pool.execute(
      `UPDATE dialer_call_logs
       SET status = ?, result = ?, duration_sec = COALESCE(?, duration_sec)
       WHERE id = ? LIMIT 1`,
      [status, resultVal, durationSec, logRow.id]
    );

    // Update lead if we have one
    if (logRow.lead_id) {
      const leadStatus = ['answered', 'voicemail', 'transferred'].includes(resultVal)
        ? resultVal
        : 'completed';
      await pool.execute(
        'UPDATE dialer_leads SET status = ? WHERE id = ? AND user_id = ? LIMIT 1',
        [leadStatus, logRow.lead_id, logRow.user_id]
      );
    }

    if (DEBUG) {
      console.log('[pipecat.dialout-completed] Updated:', { logId: logRow.id, leadId: logRow.lead_id });
    }

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[pipecat.dialout-completed] error:', e?.message || e);
    return res.status(500).json({ success: false, message: 'Internal error' });
  }
});

// Daily Webhooks (call state updates)
// Configure a Daily webhook for dialin.connected + dialin.stopped (+ optional dialin.warning/dialin.error)
// and dialout.* events (dialout.started/connected/answered/stopped/error/warning)
// to this endpoint, e.g.:
//   https://www.Phone.System.net/webhooks/daily/events?token=<DAILY_DIALIN_WEBHOOK_TOKEN>
// These events allow us to update AI call durations/status automatically.
app.post('/webhooks/daily/events', async (req, res) => {
  try {
    if (!pool) return res.status(200).json({ success: true });

    // Optional shared-secret guard (recommended).
    if (DAILY_DIALIN_WEBHOOK_TOKEN) {
      const token = String(req.query.token || '').trim();
      if (!token || token !== DAILY_DIALIN_WEBHOOK_TOKEN) {
        if (DEBUG) console.warn('[daily.webhook] Invalid token');
        return res.status(403).json({ success: false, message: 'Forbidden' });
      }
    }

    const root = req.body;

    // Enhanced logging
    if (DEBUG) {
      console.log('[daily.webhook] Received webhook:', JSON.stringify(root).slice(0, 500));
    }

    // Daily may send a single event object or (less commonly) a batch array.
    // Support both so we don't silently ignore valid deliveries.
    const events = Array.isArray(root)
      ? root
      : ((root && typeof root === 'object' && Array.isArray(root.events)) ? root.events : [root]);

    let processed = 0;
    let updated = 0;
    let ignored = 0;

    const digitsOnly = (s) => String(s || '').replace(/[^0-9]/g, '');

    for (const evtRaw of events) {
      processed += 1;

      try {
        const evt = (evtRaw && typeof evtRaw === 'object') ? evtRaw : {};

        // Daily webhook payloads can vary slightly depending on event source/version.
        // Support common keys for the event type and payload wrapper.
        const typeRaw = String(
          evt.type || evt.eventType || evt.event_type || evt.event || evt.name || ''
        ).trim();
        const type = typeRaw.toLowerCase();

        const payload = (evt.payload && typeof evt.payload === 'object')
          ? evt.payload
          : ((evt.data && typeof evt.data === 'object') ? evt.data : {});

        const src = (payload && Object.keys(payload).length) ? payload : evt;

        if (!type || (!type.startsWith('dialin.') && !type.startsWith('dialout.'))) {
          ignored += 1;
          continue;
        }

        const nestedBody = (src.body && typeof src.body === 'object') ? src.body : ((evt.body && typeof evt.body === 'object') ? evt.body : null);
        const callIdCandidates = [
          src.call_id,
          src.callId,
          src.callID,
          nestedBody?.call_id,
          nestedBody?.callId,
          nestedBody?.callID,
          evt.call_id,
          evt.callId,
          evt.callID,
          src.session_id,
          src.sessionId,
          evt.session_id,
          evt.sessionId
        ]
          .map(v => String(v || '').trim())
          .filter(Boolean);

        const callDomainCandidates = [
          src.call_domain,
          src.callDomain,
          nestedBody?.call_domain,
          nestedBody?.callDomain,
          evt.call_domain,
          evt.callDomain,
          src.domain_id,
          src.domainId,
          evt.domain_id,
          evt.domainId
        ]
          .map(v => String(v || '').trim())
          .filter(Boolean);

        // Dialout settings can be nested; attempt to resolve them for fallback matching.
        let dialoutSettings = null;
        try {
          dialoutSettings = src.dialout_settings
            || src.dialoutSettings
            || src.dialout
            || nestedBody?.dialout_settings
            || nestedBody?.dialoutSettings
            || nestedBody?.dialout
            || evt.dialout_settings
            || evt.dialoutSettings
            || evt.dialout
            || null;
          if (Array.isArray(dialoutSettings)) dialoutSettings = dialoutSettings[0] || null;
          if (dialoutSettings && typeof dialoutSettings !== 'object') dialoutSettings = null;
        } catch {
          dialoutSettings = null;
        }

        // Phone numbers (optional fallback matching)
        let toNumber = String(
          src.To
            || src.to
            || src.to_number
            || src.toNumber
            || src.phone_number
            || src.phoneNumber
            || src.destination
            || src.dest
            || evt.To
            || evt.to
            || evt.to_number
            || evt.toNumber
            || evt.phone_number
            || evt.phoneNumber
            || evt.destination
            || evt.dest
            || ''
        ).trim();
        let fromNumber = String(
          src.From
            || src.from
            || src.from_number
            || src.fromNumber
            || src.caller_id
            || src.callerId
            || evt.From
            || evt.from
            || evt.from_number
            || evt.fromNumber
            || evt.caller_id
            || evt.callerId
            || ''
        ).trim();

        if ((!toNumber || toNumber === '') && dialoutSettings) {
          const dn = dialoutSettings.phoneNumber || dialoutSettings.phone_number || dialoutSettings.to || dialoutSettings.to_number || dialoutSettings.toNumber;
          if (dn) toNumber = String(dn).trim();
        }
        if ((!fromNumber || fromNumber === '') && dialoutSettings) {
          const cn = dialoutSettings.callerId || dialoutSettings.caller_id || dialoutSettings.from || dialoutSettings.from_number || dialoutSettings.fromNumber;
          if (cn) fromNumber = String(cn).trim();
        }

        const toDigits = digitsOnly(toNumber);
        const fromDigits = digitsOnly(fromNumber);

        if (!callIdCandidates.length && !toDigits) {
          if (DEBUG) {
            console.warn('[daily.webhook] Missing call identifiers (and no to_number) for Daily event:', {
              type: typeRaw,
              keys: Object.keys(src || {}).slice(0, 25)
            });
          }
          ignored += 1;
          continue;
        }

        // Prefer call_id/callId for display/logging; fall back to session_id.
        const callId = callIdCandidates[0] || '';
        const callDomain = callDomainCandidates[0] || '';

        async function tryUpdateAiCallLog({ sqlByDomain, buildParamsByDomain, sqlById, buildParamsById }) {
          for (const cid of callIdCandidates) {
            if (sqlByDomain && callDomainCandidates.length) {
              for (const dom of callDomainCandidates) {
                const [r] = await pool.execute(sqlByDomain, buildParamsByDomain(cid, dom));
                if (r && r.affectedRows) return { updated: true, via: 'call_id+call_domain', callId: cid, callDomain: dom };
              }
            }

            const [r2] = await pool.execute(sqlById, buildParamsById(cid));
            if (r2 && r2.affectedRows) return { updated: true, via: 'call_id', callId: cid, callDomain: null };
          }

          return { updated: false };
        }

        async function tryUpdateAiCallLogByNumbers({ sqlToFrom, paramsToFrom, sqlToOnly, paramsToOnly }) {
          // MySQL 8 has REGEXP_REPLACE so we can match numbers even if formats differ (+1, spaces, etc.)
          if (toDigits && fromDigits && sqlToFrom) {
            const [r] = await pool.execute(sqlToFrom, paramsToFrom);
            if (r && r.affectedRows) return { updated: true, via: 'to+from' };
          }
          if (toDigits && sqlToOnly) {
            const [r2] = await pool.execute(sqlToOnly, paramsToOnly);
            if (r2 && r2.affectedRows) return { updated: true, via: 'to_only' };
          }
          return { updated: false };
        }

        async function tryUpdateAiCallLogByTimeWindow({ sqlById, buildParamsById }) {
          // Fallback for when Daily event identifiers don't match the ids we stored
          // from room_creation_api. Use a tight time window and only rows that
          // have not been ended yet.
          const windowMinutes = 30;

          const where = [
            'time_end IS NULL',
            'time_start IS NOT NULL',
            '(status IS NULL OR status IN (\'webhook_received\',\'pipecat_started\',\'connected\',\'warning\'))',
            '(event_call_id IS NULL OR event_call_id = \'\')',
            'time_start >= DATE_SUB(?, INTERVAL ' + windowMinutes + ' MINUTE)',
            'time_start <= DATE_ADD(?, INTERVAL ' + windowMinutes + ' MINUTE)'
          ];

          const params = [ts, ts];

          // If we have numbers in the event payload, constrain by them as well.
          if (toDigits) {
            where.push("REGEXP_REPLACE(to_number, '[^0-9]', '') = ?");
            params.push(toDigits);
          }
          if (fromDigits) {
            where.push("REGEXP_REPLACE(from_number, '[^0-9]', '') = ?");
            params.push(fromDigits);
          }

          // Select the nearest candidate row by start time.
          let rowId = null;
          try {
            const [rows] = await pool.query(
              `SELECT id
               FROM ai_call_logs
               WHERE ${where.join(' AND ')}
               ORDER BY ABS(TIMESTAMPDIFF(SECOND, time_start, ?)) ASC, time_start DESC, id DESC
               LIMIT 1`,
              params.concat([ts])
            );
            rowId = rows && rows[0] ? rows[0].id : null;
          } catch (e) {
            if (DEBUG) console.warn('[daily.webhook] time-window candidate lookup failed:', e.message || e);
            rowId = null;
          }

          if (!rowId) return { updated: false };

          const [r] = await pool.execute(sqlById, buildParamsById(rowId));
          if (r && r.affectedRows) return { updated: true, via: 'time_window', rowId };
          return { updated: false };
        }

        // Timestamps are seconds since epoch.
        const tsRaw = src.timestamp != null
          ? Number(src.timestamp)
          : (evt.timestamp != null
            ? Number(evt.timestamp)
            : (evt.event_ts != null ? Number(evt.event_ts) : null));
        const ts = (tsRaw && Number.isFinite(tsRaw)) ? new Date(tsRaw * 1000) : new Date();

        if (DEBUG) {
          console.log('[daily.webhook]', {
            type: typeRaw,
            callId,
            callDomain,
            to: toNumber || null,
            from: fromNumber || null,
            ts: ts.toISOString()
          });
        }

        async function findDialerCallLogByCallId() {
          for (const cid of callIdCandidates) {
            const [rows] = await pool.execute(
              'SELECT id, lead_id, campaign_id, user_id FROM dialer_call_logs WHERE call_id = ? ORDER BY id DESC LIMIT 1',
              [cid]
            );
            const row = rows && rows[0] ? rows[0] : null;
            if (row) return row;
          }
          return null;
        }

        async function findDialerLeadByToDigits() {
          if (!toDigits) return null;
          try {
            const [rows] = await pool.query(
              `SELECT id, campaign_id, user_id, status
               FROM dialer_leads
               WHERE REGEXP_REPLACE(phone_number, '[^0-9]', '') = ?
                 AND status IN ('queued','dialing','pending','answered','voicemail','transferred')
                 AND COALESCE(last_call_at, created_at) >= DATE_SUB(?, INTERVAL 12 HOUR)
               ORDER BY COALESCE(last_call_at, created_at) DESC, id DESC
               LIMIT 1`,
              [toDigits, ts]
            );
            return rows && rows[0] ? rows[0] : null;
          } catch (e) {
            if (DEBUG) console.warn('[daily.webhook] Dialer lead lookup failed:', e?.message || e);
            return null;
          }
        }

        function dialoutOutcomeFromReason(reasonLower) {
          if (!reasonLower) return null;
          if (reasonLower.includes('voicemail')) {
            return { result: 'voicemail', leadStatus: 'voicemail' };
          }
          if (reasonLower.includes('transfer')) {
            return { result: 'transferred', leadStatus: 'transferred' };
          }
          if (
            reasonLower.includes('busy')
            || reasonLower.includes('no_answer')
            || reasonLower.includes('no answer')
            || reasonLower.includes('failed')
            || reasonLower.includes('error')
            || reasonLower.includes('cancel')
          ) {
            return { result: 'failed', leadStatus: 'failed' };
          }
          if (reasonLower.includes('answer') || reasonLower.includes('connect')) {
            return { result: 'answered', leadStatus: 'answered' };
          }
          return null;
        }

        if (type.startsWith('dialout.')) {
          const reasonRaw = String(
            src.reason
              || src.status
              || src.result
              || src.error
              || src.error_message
              || src.errorMessage
              || ''
          ).trim();
          const reasonLower = reasonRaw.toLowerCase();

          const durationRaw = (
            src.duration
              ?? src.duration_sec
              ?? src.duration_seconds
              ?? src.durationSeconds
              ?? src.billsec
              ?? src.call_duration
              ?? src.callDuration
          );
          let durationSec = null;
          if (durationRaw != null && String(durationRaw).trim() !== '') {
            const parsed = Number(durationRaw);
            if (Number.isFinite(parsed)) durationSec = Math.max(0, Math.round(parsed));
          }

          const outcome = dialoutOutcomeFromReason(reasonLower);
          let leadStatus = null;
          let logStatus = null;
          let resultVal = null;

          if (type === 'dialout.started' || type === 'dialout.ringing') {
            leadStatus = 'dialing';
            logStatus = 'dialing';
          } else if (type === 'dialout.connected' || type === 'dialout.answered') {
            leadStatus = 'answered';
            logStatus = 'connected';
            resultVal = 'answered';
          } else if (type === 'dialout.warning') {
            logStatus = 'warning';
          } else if (type === 'dialout.error') {
            leadStatus = 'failed';
            logStatus = 'error';
            resultVal = 'failed';
          } else if (type === 'dialout.stopped') {
            if (outcome) {
              leadStatus = outcome.leadStatus;
              resultVal = outcome.result;
            }
            if (!logStatus) logStatus = (resultVal === 'failed') ? 'error' : 'completed';
          }

          let logRow = null;
          let leadRow = null;
          try { logRow = await findDialerCallLogByCallId(); } catch {}
          if (logRow && logRow.lead_id) {
            leadRow = { id: logRow.lead_id, campaign_id: logRow.campaign_id, user_id: logRow.user_id };
          }
          if (!leadRow) {
            try { leadRow = await findDialerLeadByToDigits(); } catch {}
          }

          // Enhanced debug logging for dialout events
          if (DEBUG) {
            console.log('[daily.webhook] Processing dialout event:', {
              type: typeRaw,
              leadStatus,
              logStatus,
              resultVal,
              durationSec,
              foundLogRow: !!logRow,
              foundLeadRow: !!leadRow,
              callIdCandidates: callIdCandidates.slice(0, 3),
              toDigits: toDigits || null
            });
          }

          let leadUpdated = false;
          if (leadRow) {
            if (type === 'dialout.stopped') {
              const statusToken = leadStatus || '';
              const [r] = await pool.execute(
                `UPDATE dialer_leads
                 SET status = CASE
                   WHEN ? = 'failed' THEN 'failed'
                   WHEN ? = 'voicemail' THEN 'voicemail'
                   WHEN ? = 'transferred' THEN 'transferred'
                   WHEN status IN ('answered','voicemail','transferred') THEN status
                   ELSE 'completed'
                 END
                 WHERE id = ? AND user_id = ? LIMIT 1`,
                [statusToken, statusToken, statusToken, leadRow.id, leadRow.user_id]
              );
              if (r && r.affectedRows) leadUpdated = true;
            } else if (leadStatus) {
              const setLastCall = (type === 'dialout.started' || type === 'dialout.connected' || type === 'dialout.answered');
              const [r] = await pool.execute(
                `UPDATE dialer_leads
                 SET status = ?${setLastCall ? ', last_call_at = NOW()' : ''}
                 WHERE id = ? AND user_id = ? LIMIT 1`,
                [leadStatus, leadRow.id, leadRow.user_id]
              );
              if (r && r.affectedRows) leadUpdated = true;
            }
          }

          const notes = reasonRaw ? String(reasonRaw).slice(0, 1000) : '';
          const statusVal = logStatus || null;
          const resultOut = resultVal || null;
          const durationOut = Number.isFinite(durationSec) ? durationSec : null;

          async function updateDialerCallLog(whereClause, whereParams) {
            const [r] = await pool.execute(
              `UPDATE dialer_call_logs
               SET status = COALESCE(?, status),
                   result = COALESCE(?, result),
                   duration_sec = COALESCE(?, duration_sec),
                   notes = CASE WHEN ? <> '' THEN COALESCE(NULLIF(notes,''), ?) ELSE notes END
               ${whereClause}`,
              [statusVal, resultOut, durationOut, notes, notes].concat(whereParams)
            );
            return r && r.affectedRows;
          }

          let logUpdated = false;
          try {
            if (logRow && logRow.id) {
              logUpdated = await updateDialerCallLog('WHERE id = ? LIMIT 1', [logRow.id]);
            } else if (leadRow && leadRow.id) {
              logUpdated = await updateDialerCallLog('WHERE lead_id = ? ORDER BY id DESC LIMIT 1', [leadRow.id]);
            }
          } catch (e) {
            if (DEBUG) console.warn('[daily.webhook] Dialer call log update failed:', e?.message || e);
          }

          if ((leadUpdated || logUpdated) && (leadRow || logRow)) {
            updated += 1;
          } else {
            ignored += 1;
            if (DEBUG) {
              console.warn('[daily.webhook] No matching dialer rows for dialout event', {
                type: typeRaw,
                callIdCandidates: callIdCandidates.slice(0, 5),
                toDigits: toDigits || null
              });
            }
          }
          continue;
        }

        if (type === 'dialin.connected') {
          // 1) Preferred: match by event_call_id/event_call_domain (once mapped).
          let result = await tryUpdateAiCallLog({
            sqlByDomain: `UPDATE ai_call_logs
             SET status = ?,
                 time_connect = COALESCE(time_connect, ?),
                 updated_at = CURRENT_TIMESTAMP
             WHERE event_call_id = ? AND event_call_domain = ? AND time_end IS NULL
             LIMIT 1`,
            buildParamsByDomain: (cid, dom) => ['connected', ts, cid, dom],
            sqlById: `UPDATE ai_call_logs
               SET status = ?,
                   time_connect = COALESCE(time_connect, ?),
                   updated_at = CURRENT_TIMESTAMP
               WHERE event_call_id = ? AND time_end IS NULL
               LIMIT 1`,
            buildParamsById: (cid) => ['connected', ts, cid]
          });

          // 2) Fallback: some Daily payloads may still use the original room_creation_api call ids.
          if (!result.updated) {
            result = await tryUpdateAiCallLog({
              sqlByDomain: `UPDATE ai_call_logs
               SET status = ?,
                   time_connect = COALESCE(time_connect, ?),
                   updated_at = CURRENT_TIMESTAMP
               WHERE call_id = ? AND call_domain = ? AND time_end IS NULL
               LIMIT 1`,
              buildParamsByDomain: (cid, dom) => ['connected', ts, cid, dom],
              sqlById: `UPDATE ai_call_logs
                 SET status = ?,
                     time_connect = COALESCE(time_connect, ?),
                     updated_at = CURRENT_TIMESTAMP
                 WHERE call_id = ? AND time_end IS NULL
                 LIMIT 1`,
              buildParamsById: (cid) => ['connected', ts, cid]
            });
          }

          // 3) Fallback: match by phone numbers (if present) and store the event ids.
          if (!result.updated) {
            const numResult = await tryUpdateAiCallLogByNumbers({
              sqlToFrom: `UPDATE ai_call_logs
                SET status = ?,
                    time_connect = COALESCE(time_connect, ?),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND REGEXP_REPLACE(from_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToFrom: ['connected', ts, callId || null, callDomain || null, toDigits, fromDigits, ts],
              sqlToOnly: `UPDATE ai_call_logs
                SET status = ?,
                    time_connect = COALESCE(time_connect, ?),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToOnly: ['connected', ts, callId || null, callDomain || null, toDigits, ts]
            });
            result = numResult.updated ? numResult : result;
          }

          // 4) Last resort: match by a tight time window and store the event ids.
          if (!result.updated) {
            const timeResult = await tryUpdateAiCallLogByTimeWindow({
              sqlById: `UPDATE ai_call_logs
                SET status = ?,
                    time_connect = COALESCE(time_connect, ?),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                LIMIT 1`,
              buildParamsById: (rowId) => ['connected', ts, callId || null, callDomain || null, rowId]
            });
            result = timeResult.updated ? timeResult : result;
          }

          if (!result.updated && DEBUG) {
            console.warn('[daily.webhook] No matching ai_call_logs row for dialin.connected', {
              callIdCandidates: callIdCandidates.slice(0, 10),
              callDomainCandidates: callDomainCandidates.slice(0, 10),
              toDigits: toDigits || null,
              fromDigits: fromDigits || null
            });
          }

          if (result.updated) updated += 1;
          continue;
        }

        if (type === 'dialin.stopped') {
          // 1) Preferred: match by event_call_id/event_call_domain (once mapped).
          let result = await tryUpdateAiCallLog({
            sqlByDomain: `UPDATE ai_call_logs
             SET status = CASE WHEN status = 'error' THEN 'error' WHEN status LIKE 'blocked%' THEN status ELSE CASE WHEN time_connect IS NULL THEN 'missed' ELSE 'completed' END END,
                 time_end = COALESCE(time_end, ?),
                 duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                 billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                 updated_at = CURRENT_TIMESTAMP
             WHERE event_call_id = ? AND event_call_domain = ?
             LIMIT 1`,
            buildParamsByDomain: (cid, dom) => [ts, ts, ts, ts, ts, cid, dom],
            sqlById: `UPDATE ai_call_logs
               SET status = CASE WHEN status = 'error' THEN 'error' WHEN status LIKE 'blocked%' THEN status ELSE CASE WHEN time_connect IS NULL THEN 'missed' ELSE 'completed' END END,
                   time_end = COALESCE(time_end, ?),
                   duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   updated_at = CURRENT_TIMESTAMP
               WHERE event_call_id = ?
               LIMIT 1`,
            buildParamsById: (cid) => [ts, ts, ts, ts, ts, cid]
          });

          // 2) Fallback: legacy match by call_id/call_domain and store the event ids.
          if (!result.updated) {
            result = await tryUpdateAiCallLog({
              sqlByDomain: `UPDATE ai_call_logs
               SET status = CASE WHEN status = 'error' THEN 'error' WHEN status LIKE 'blocked%' THEN status ELSE CASE WHEN time_connect IS NULL THEN 'missed' ELSE 'completed' END END,
                   time_end = COALESCE(time_end, ?),
                   duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   event_call_id = COALESCE(event_call_id, ?),
                   event_call_domain = COALESCE(event_call_domain, ?),
                   updated_at = CURRENT_TIMESTAMP
               WHERE call_id = ? AND call_domain = ?
               LIMIT 1`,
              buildParamsByDomain: (cid, dom) => [ts, ts, ts, ts, ts, callId || null, callDomain || null, cid, dom],
              sqlById: `UPDATE ai_call_logs
                 SET status = CASE WHEN status = 'error' THEN 'error' WHEN status LIKE 'blocked%' THEN status ELSE CASE WHEN time_connect IS NULL THEN 'missed' ELSE 'completed' END END,
                     time_end = COALESCE(time_end, ?),
                     duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                     billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                     event_call_id = COALESCE(event_call_id, ?),
                     event_call_domain = COALESCE(event_call_domain, ?),
                     updated_at = CURRENT_TIMESTAMP
                 WHERE call_id = ?
                 LIMIT 1`,
              buildParamsById: (cid) => [ts, ts, ts, ts, ts, callId || null, callDomain || null, cid]
            });
          }

          // 3) Fallback: match by phone numbers (if present) and store the event ids.
          if (!result.updated) {
            const numResult = await tryUpdateAiCallLogByNumbers({
              sqlToFrom: `UPDATE ai_call_logs
                SET status = CASE WHEN status = 'error' THEN 'error' WHEN status LIKE 'blocked%' THEN status ELSE CASE WHEN time_connect IS NULL THEN 'missed' ELSE 'completed' END END,
                    time_end = COALESCE(time_end, ?),
                    duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND REGEXP_REPLACE(from_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToFrom: [ts, ts, ts, ts, ts, callId || null, callDomain || null, toDigits, fromDigits, ts],
              sqlToOnly: `UPDATE ai_call_logs
                SET status = CASE WHEN status = 'error' THEN 'error' WHEN status LIKE 'blocked%' THEN status ELSE CASE WHEN time_connect IS NULL THEN 'missed' ELSE 'completed' END END,
                    time_end = COALESCE(time_end, ?),
                    duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToOnly: [ts, ts, ts, ts, ts, callId || null, callDomain || null, toDigits, ts]
            });
            result = numResult.updated ? numResult : result;
          }

          // 4) Last resort: match by a tight time window and store the event ids.
          if (!result.updated) {
            const timeResult = await tryUpdateAiCallLogByTimeWindow({
              sqlById: `UPDATE ai_call_logs
                SET status = CASE WHEN status = 'error' THEN 'error' WHEN status LIKE 'blocked%' THEN status ELSE CASE WHEN time_connect IS NULL THEN 'missed' ELSE 'completed' END END,
                    time_end = COALESCE(time_end, ?),
                    duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                LIMIT 1`,
              buildParamsById: (rowId) => [ts, ts, ts, ts, ts, callId || null, callDomain || null, rowId]
            });
            result = timeResult.updated ? timeResult : result;
          }

          if (!result.updated && DEBUG) {
            console.warn('[daily.webhook] No matching ai_call_logs row for dialin.stopped', {
              callIdCandidates: callIdCandidates.slice(0, 10),
              callDomainCandidates: callDomainCandidates.slice(0, 10),
              toDigits: toDigits || null,
              fromDigits: fromDigits || null
            });
          }

          // Store completed AI call in local CDR table
          if (result.updated) {
            try {
              // Fetch the updated ai_call_log row to get complete details
              let logRow = null;
              for (const cid of callIdCandidates) {
                const [rows] = await pool.execute(
                  `SELECT * FROM ai_call_logs
                   WHERE (event_call_id = ? OR call_id = ?)
                     AND time_end IS NOT NULL
                   ORDER BY updated_at DESC
                   LIMIT 1`,
                  [cid, cid]
                );
                if (rows && rows[0]) {
                  logRow = rows[0];
                  break;
                }
              }

              if (logRow && logRow.user_id) {
                // Calculate cost based on billsec and inbound rate
                const billsecVal = Number(logRow.billsec) || 0;
                const rate = rateAiInboundCallPrice({ toNumber: logRow.to_number, billsec: billsecVal });
                const cost = Number(rate.price || 0);

                // Charge once per ai_call_logs row (idempotent via billed/billing_history_id).
                const typeLabel = rate.isTollfree ? 'toll-free' : 'local';
                const chargeDesc = `AI inbound ${typeLabel} call – ${String(logRow.to_number || '').trim() || String(logRow.call_id || '')}`.slice(0, 255);
                const chargeRes = await chargeAiInboundCallLog({
                  aiCallLogId: logRow.id,
                  userId: logRow.user_id,
                  amount: cost,
                  description: chargeDesc
                });
                if (DEBUG && chargeRes && chargeRes.ok && !chargeRes.alreadyBilled && !chargeRes.skipped) {
                  console.log('[daily.webhook] Charged inbound AI call:', {
                    userId: logRow.user_id,
                    callId: logRow.call_id,
                    amount: cost,
                    billingId: chargeRes.billingId || null
                  });
                }

                const cdrWrite = await storeCdr({
                  userId: logRow.user_id,
                  direction: 'inbound',
                  callType: 'ai_agent',
                  fromNumber: logRow.from_number || '',
                  toNumber: logRow.to_number || '',
                  startTime: logRow.time_start,
                  connectTime: logRow.time_connect || null,
                  endTime: logRow.time_end,
                  duration: Number(logRow.duration) || 0,
                  billsec: billsecVal,
                  cost: cost,
                  status: logRow.status || 'completed',
                  agentId: logRow.agent_id || null
                });
                if (cdrWrite && cdrWrite.success) {
                  if (DEBUG) {
                    console.log('[daily.webhook] Stored CDR for completed AI call:', {
                      userId: logRow.user_id,
                      callId: logRow.call_id,
                      duration: logRow.duration,
                      cost: cost.toFixed(4)
                    });
                  }
                } else if (DEBUG) {
                  console.warn('[daily.webhook] CDR insert failed for completed AI call:', {
                    userId: logRow.user_id,
                    callId: logRow.call_id,
                    error: cdrWrite?.error || 'unknown_error'
                  });
                }
              }
            } catch (e) {
              if (DEBUG) console.warn('[daily.webhook] Failed to store CDR:', e?.message || e);
            }

            updated += 1;
          }
          continue;
        }

        if (type === 'dialin.error') {
          // 1) Preferred: match by event_call_id/event_call_domain (once mapped).
          let result = await tryUpdateAiCallLog({
            sqlByDomain: `UPDATE ai_call_logs
             SET status = 'error',
                 time_end = COALESCE(time_end, ?),
                 duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                 billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                 updated_at = CURRENT_TIMESTAMP
             WHERE event_call_id = ? AND event_call_domain = ? AND time_end IS NULL
             LIMIT 1`,
            buildParamsByDomain: (cid, dom) => [ts, ts, ts, ts, ts, cid, dom],
            sqlById: `UPDATE ai_call_logs
               SET status = 'error',
                   time_end = COALESCE(time_end, ?),
                   duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   updated_at = CURRENT_TIMESTAMP
               WHERE event_call_id = ? AND time_end IS NULL
               LIMIT 1`,
            buildParamsById: (cid) => [ts, ts, ts, ts, ts, cid]
          });

          // 2) Fallback: legacy match by call_id/call_domain and store the event ids.
          if (!result.updated) {
            result = await tryUpdateAiCallLog({
              sqlByDomain: `UPDATE ai_call_logs
               SET status = 'error',
                   time_end = COALESCE(time_end, ?),
                   duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                   event_call_id = COALESCE(event_call_id, ?),
                   event_call_domain = COALESCE(event_call_domain, ?),
                   updated_at = CURRENT_TIMESTAMP
               WHERE call_id = ? AND call_domain = ? AND time_end IS NULL
               LIMIT 1`,
              buildParamsByDomain: (cid, dom) => [ts, ts, ts, ts, ts, callId || null, callDomain || null, cid, dom],
              sqlById: `UPDATE ai_call_logs
                 SET status = 'error',
                     time_end = COALESCE(time_end, ?),
                     duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                     billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                     event_call_id = COALESCE(event_call_id, ?),
                     event_call_domain = COALESCE(event_call_domain, ?),
                     updated_at = CURRENT_TIMESTAMP
                 WHERE call_id = ? AND time_end IS NULL
                 LIMIT 1`,
              buildParamsById: (cid) => [ts, ts, ts, ts, ts, callId || null, callDomain || null, cid]
            });
          }

          // 3) Fallback: match by phone numbers (if present) and store the event ids.
          if (!result.updated) {
            const numResult = await tryUpdateAiCallLogByNumbers({
              sqlToFrom: `UPDATE ai_call_logs
                SET status = 'error',
                    time_end = COALESCE(time_end, ?),
                    duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND REGEXP_REPLACE(from_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToFrom: [ts, ts, ts, ts, ts, callId || null, callDomain || null, toDigits, fromDigits, ts],
              sqlToOnly: `UPDATE ai_call_logs
                SET status = 'error',
                    time_end = COALESCE(time_end, ?),
                    duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToOnly: [ts, ts, ts, ts, ts, callId || null, callDomain || null, toDigits, ts]
            });
            result = numResult.updated ? numResult : result;
          }

          // 4) Last resort: match by a tight time window and store the event ids.
          if (!result.updated) {
            const timeResult = await tryUpdateAiCallLogByTimeWindow({
              sqlById: `UPDATE ai_call_logs
                SET status = 'error',
                    time_end = COALESCE(time_end, ?),
                    duration = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    billsec = GREATEST(0, TIMESTAMPDIFF(SECOND, COALESCE(time_connect, time_start, ?), COALESCE(time_end, ?))),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                LIMIT 1`,
              buildParamsById: (rowId) => [ts, ts, ts, ts, ts, callId || null, callDomain || null, rowId]
            });
            result = timeResult.updated ? timeResult : result;
          }

          if (!result.updated && DEBUG) {
            console.warn('[daily.webhook] No matching ai_call_logs row for dialin.error', {
              callIdCandidates: callIdCandidates.slice(0, 10),
              callDomainCandidates: callDomainCandidates.slice(0, 10),
              toDigits: toDigits || null,
              fromDigits: fromDigits || null
            });
          }

          if (result.updated) updated += 1;
          continue;
        }

        if (type === 'dialin.warning') {
          // 1) Preferred: match by event_call_id/event_call_domain (once mapped).
          let result = await tryUpdateAiCallLog({
            sqlByDomain: `UPDATE ai_call_logs
             SET status = IF(status IS NULL OR status = '', 'warning', status),
                 updated_at = CURRENT_TIMESTAMP
             WHERE event_call_id = ? AND event_call_domain = ?
             LIMIT 1`,
            buildParamsByDomain: (cid, dom) => [cid, dom],
            sqlById: `UPDATE ai_call_logs
               SET status = IF(status IS NULL OR status = '', 'warning', status),
                   updated_at = CURRENT_TIMESTAMP
               WHERE event_call_id = ?
               LIMIT 1`,
            buildParamsById: (cid) => [cid]
          });

          // 2) Fallback: legacy match by call_id/call_domain and store the event ids.
          if (!result.updated) {
            result = await tryUpdateAiCallLog({
              sqlByDomain: `UPDATE ai_call_logs
               SET status = IF(status IS NULL OR status = '', 'warning', status),
                   event_call_id = COALESCE(event_call_id, ?),
                   event_call_domain = COALESCE(event_call_domain, ?),
                   updated_at = CURRENT_TIMESTAMP
               WHERE call_id = ? AND call_domain = ?
               LIMIT 1`,
              buildParamsByDomain: (cid, dom) => [callId || null, callDomain || null, cid, dom],
              sqlById: `UPDATE ai_call_logs
                 SET status = IF(status IS NULL OR status = '', 'warning', status),
                     event_call_id = COALESCE(event_call_id, ?),
                     event_call_domain = COALESCE(event_call_domain, ?),
                     updated_at = CURRENT_TIMESTAMP
                 WHERE call_id = ?
                 LIMIT 1`,
              buildParamsById: (cid) => [callId || null, callDomain || null, cid]
            });
          }

          // 3) Fallback: match by phone numbers (if present) and store the event ids.
          if (!result.updated) {
            const numResult = await tryUpdateAiCallLogByNumbers({
              sqlToFrom: `UPDATE ai_call_logs
                SET status = IF(status IS NULL OR status = '', 'warning', status),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND REGEXP_REPLACE(from_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToFrom: [callId || null, callDomain || null, toDigits, fromDigits, ts],
              sqlToOnly: `UPDATE ai_call_logs
                SET status = IF(status IS NULL OR status = '', 'warning', status),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE time_end IS NULL
                  AND REGEXP_REPLACE(to_number, '[^0-9]', '') = ?
                  AND time_start >= DATE_SUB(?, INTERVAL 12 HOUR)
                ORDER BY time_start DESC, id DESC
                LIMIT 1`,
              paramsToOnly: [callId || null, callDomain || null, toDigits, ts]
            });
            result = numResult.updated ? numResult : result;
          }

          // 4) Last resort: match by a tight time window and store the event ids.
          if (!result.updated) {
            const timeResult = await tryUpdateAiCallLogByTimeWindow({
              sqlById: `UPDATE ai_call_logs
                SET status = IF(status IS NULL OR status = '', 'warning', status),
                    event_call_id = COALESCE(event_call_id, ?),
                    event_call_domain = COALESCE(event_call_domain, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                LIMIT 1`,
              buildParamsById: (rowId) => [callId || null, callDomain || null, rowId]
            });
            result = timeResult.updated ? timeResult : result;
          }

          if (!result.updated && DEBUG) {
            console.warn('[daily.webhook] No matching ai_call_logs row for dialin.warning', {
              callIdCandidates: callIdCandidates.slice(0, 10),
              callDomainCandidates: callDomainCandidates.slice(0, 10),
              toDigits: toDigits || null,
              fromDigits: fromDigits || null
            });
          }

          if (result.updated) updated += 1;
          continue;
        }

        // Other dialin.* events we don't currently act on.
        ignored += 1;
      } catch (e) {
        // Never fail the whole delivery because one event row failed.
        if (DEBUG) console.warn('[daily.webhook] Failed to process event:', e?.message || e);
      }
    }

    return res.status(200).json({ success: true, processed, updated, ignored });
  } catch (e) {
    if (DEBUG) console.error('[daily.webhook] Unhandled error:', e.message || e);
    return res.status(200).json({ success: true });
  }
});


// Pipecat Cloud (Control plane)
// NOTE: Pipecat Cloud API details can vary; keep URLs configurable via env.
// Private key is used for control-plane operations (create agents, manage secrets, etc.)
const PIPECAT_API_KEY = process.env.PIPECAT_PRIVATE_API_KEY || process.env.PIPECAT_API_KEY || '';
// Public key is required for /public/* endpoints like /public/{agentName}/start
const PIPECAT_PUBLIC_API_KEY = process.env.PIPECAT_PUBLIC_API_KEY || process.env.PIPECAT_PUBLIC_KEY || '';
const PIPECAT_API_URL = process.env.PIPECAT_API_URL || process.env.PIPECAT_BASE_URL || 'https://api.pipecat.daily.co/v1';
const PIPECAT_ORG_ID = process.env.PIPECAT_ORG_ID || '';
const PIPECAT_AGENT_IMAGE = process.env.PIPECAT_AGENT_IMAGE || '';
const PIPECAT_REGION = process.env.PIPECAT_REGION || 'us-west';
const PIPECAT_AGENT_PROFILE = process.env.PIPECAT_AGENT_PROFILE || 'agent-1x';
// Optional autoscaling + integrations
const PIPECAT_MIN_AGENTS = process.env.PIPECAT_MIN_AGENTS;
const PIPECAT_MAX_AGENTS = process.env.PIPECAT_MAX_AGENTS;
const PIPECAT_ENABLE_KRISP = process.env.PIPECAT_ENABLE_KRISP;
const PIPECAT_IMAGE_PULL_SECRET_SET = process.env.PIPECAT_IMAGE_PULL_SECRET_SET || '';
const PIPECAT_DIALIN_WEBHOOK_BASE = process.env.PIPECAT_DIALIN_WEBHOOK_BASE || 'https://api.pipecat.daily.co/v1/public/webhooks';

// Optional: route Daily pinless dial-in callbacks through this server so we can log inbound AI calls.
// If not set, we fall back to PIPECAT_DIALIN_WEBHOOK_BASE.
const AI_DIALIN_WEBHOOK_BASE = process.env.AI_DIALIN_WEBHOOK_BASE || '';
// Optional shared secret appended to room_creation_api as ?token=... and verified by our webhook.
const DAILY_DIALIN_WEBHOOK_TOKEN = process.env.DAILY_DIALIN_WEBHOOK_TOKEN || '';

function parseOptionalIntEnv(v) {
  if (v === undefined || v === null || String(v).trim() === '') return undefined;
  const n = parseInt(String(v), 10);
  return Number.isFinite(n) ? n : undefined;
}

function parseOptionalBoolEnv(v) {
  if (v === undefined || v === null || String(v).trim() === '') return undefined;
  const s = String(v).trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

function buildPipecatAutoScaling() {
  const minAgents = parseOptionalIntEnv(PIPECAT_MIN_AGENTS);
  const maxAgents = parseOptionalIntEnv(PIPECAT_MAX_AGENTS);
  const obj = {};
  if (Number.isFinite(minAgents)) obj.minAgents = minAgents;
  if (Number.isFinite(maxAgents)) obj.maxAgents = maxAgents;
  return Object.keys(obj).length ? obj : undefined;
}

// Daily Telephony
const DAILY_API_KEY = process.env.DAILY_API_KEY || '';
const DAILY_API_URL = process.env.DAILY_API_URL || 'https://api.daily.co/v1';

// Cartesia (used to list voices in the dashboard UI)
const CARTESIA_API_KEY = process.env.CARTESIA_API_KEY || '';
const CARTESIA_API_URL = process.env.CARTESIA_API_URL || 'https://api.cartesia.ai';
const CARTESIA_VERSION = process.env.CARTESIA_VERSION || '2025-04-16';

function pipecatAxios({ kind } = {}) {
  const k = String(kind || 'private').trim().toLowerCase();
  const apiKey = (k === 'public') ? PIPECAT_PUBLIC_API_KEY : PIPECAT_API_KEY;

  if (!apiKey) {
    throw new Error(k === 'public'
      ? 'PIPECAT_PUBLIC_API_KEY not configured'
      : 'PIPECAT_PRIVATE_API_KEY not configured'
    );
  }

  return axios.create({
    baseURL: PIPECAT_API_URL,
    timeout: 30000,
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    validateStatus: () => true
  });
}

function dailyAxios() {
  if (!DAILY_API_KEY) {
    throw new Error('DAILY_API_KEY not configured');
  }
  return axios.create({
    baseURL: DAILY_API_URL,
    timeout: 30000,
    headers: {
      'Authorization': `Bearer ${DAILY_API_KEY}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    validateStatus: () => true
  });
}

function cartesiaAxios() {
  if (!CARTESIA_API_KEY) {
    throw new Error('CARTESIA_API_KEY not configured');
  }
  return axios.create({
    baseURL: CARTESIA_API_URL,
    timeout: 30000,
    headers: {
      'Authorization': `Bearer ${CARTESIA_API_KEY}`,
      'Cartesia-Version': CARTESIA_VERSION,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    validateStatus: () => true
  });
}

async function pipecatApiCall({ method, path, body }) {
  const isPublic = String(path || '').startsWith('/public/');
  const client = pipecatAxios({ kind: isPublic ? 'public' : 'private' });
  const resp = await client.request({ method, url: path, data: body });
  if (!(resp.status >= 200 && resp.status < 300)) {
    if (DEBUG) console.error('[pipecatApiCall] Request failed:', { status: resp.status, path, data: resp.data });
    const msg = (resp.data && (resp.data.message || resp.data.error)) ? (resp.data.message || resp.data.error) : `Pipecat API error (${resp.status})`;
    const err = new Error(msg);
    err.status = resp.status;
    err.data = resp.data;
    throw err;
  }
  return resp.data;
}

async function dailyApiCall({ method, path, body, params }) {
  const client = dailyAxios();
  const resp = await client.request({ method, url: path, data: body, params });
  if (!(resp.status >= 200 && resp.status < 300)) {
    if (DEBUG) console.error('[dailyApiCall] Request failed:', { status: resp.status, path, data: resp.data });
    const msg = (resp.data && (resp.data.message || resp.data.error)) ? (resp.data.message || resp.data.error) : `Daily API error (${resp.status})`;
    const err = new Error(msg);
    err.status = resp.status;
    err.data = resp.data;
    throw err;
  }
  return resp.data;
}

async function cartesiaApiCall({ method, path, body, params }) {
  const client = cartesiaAxios();
  const resp = await client.request({ method, url: path, data: body, params });
  if (!(resp.status >= 200 && resp.status < 300)) {
    if (DEBUG) console.error('[cartesiaApiCall] Request failed:', { status: resp.status, path, data: resp.data });
    const msg = (resp.data && (resp.data.message || resp.data.error)) ? (resp.data.message || resp.data.error) : `Cartesia API error (${resp.status})`;
    const err = new Error(msg);
    err.status = resp.status;
    err.data = resp.data;
    throw err;
  }
  return resp.data;
}

// ── Daily events webhook auto-registration ──
// Ensures a Daily domain webhook is configured to send dialin.*/dialout.* events
// to our /webhooks/daily/events endpoint so AI call durations/status are tracked.
const DAILY_EVENTS_WEBHOOK_EVENT_TYPES = [
  'dialin.connected', 'dialin.stopped', 'dialin.warning', 'dialin.error',
  'dialout.connected', 'dialout.answered', 'dialout.stopped', 'dialout.error', 'dialout.warning'
];

async function ensureDailyEventsWebhook() {
  if (!DAILY_API_KEY) {
    console.warn('[daily.webhook.setup] DAILY_API_KEY not configured; skipping webhook registration');
    return;
  }

  const publicBase = String(process.env.PUBLIC_BASE_URL || '').trim().replace(/\/$/, '');
  if (!publicBase) {
    console.warn('[daily.webhook.setup] PUBLIC_BASE_URL not configured; skipping webhook registration');
    return;
  }

  // Build the desired webhook URL (include token if configured).
  let desiredUrl = `${publicBase}/webhooks/daily/events`;
  if (DAILY_DIALIN_WEBHOOK_TOKEN) {
    try {
      const u = new URL(desiredUrl);
      u.searchParams.set('token', DAILY_DIALIN_WEBHOOK_TOKEN);
      desiredUrl = u.toString();
    } catch {}
  }

  try {
    // List existing webhooks (Daily allows at most one per domain).
    const listResp = await dailyApiCall({ method: 'GET', path: '/webhooks' });
    const existing = Array.isArray(listResp?.data)
      ? listResp.data
      : (Array.isArray(listResp) ? listResp : []);

    const webhook = existing[0] || null;

    if (webhook && webhook.uuid) {
      // Check if URL and event types match; also re-activate if FAILED.
      const currentUrl = String(webhook.url || '').trim();
      const currentEvents = Array.isArray(webhook.eventTypes) ? webhook.eventTypes : [];
      const currentState = String(webhook.state || '').trim().toUpperCase();

      const urlMatch = currentUrl === desiredUrl;
      const eventsMatch = DAILY_EVENTS_WEBHOOK_EVENT_TYPES.every(e => currentEvents.includes(e));
      const isFailed = currentState === 'FAILED';

      if (urlMatch && eventsMatch && !isFailed) {
        console.log('[daily.webhook.setup] Webhook already configured:', webhook.uuid);
        return;
      }

      // Update existing webhook to fix URL, events, or re-activate.
      console.log('[daily.webhook.setup] Updating existing webhook:', webhook.uuid, {
        urlMatch, eventsMatch, isFailed
      });
      await dailyApiCall({
        method: 'POST',
        path: `/webhooks/${encodeURIComponent(webhook.uuid)}`,
        body: {
          url: desiredUrl,
          eventTypes: DAILY_EVENTS_WEBHOOK_EVENT_TYPES
        }
      });
      console.log('[daily.webhook.setup] Webhook updated successfully:', webhook.uuid);
      return;
    }

    // No webhook exists — create one.
    console.log('[daily.webhook.setup] Creating new Daily events webhook:', desiredUrl);
    const created = await dailyApiCall({
      method: 'POST',
      path: '/webhooks',
      body: {
        url: desiredUrl,
        eventTypes: DAILY_EVENTS_WEBHOOK_EVENT_TYPES
      }
    });
    console.log('[daily.webhook.setup] Webhook created:', created?.uuid || '(unknown uuid)');
  } catch (e) {
    console.error('[daily.webhook.setup] Failed to ensure Daily events webhook:', e?.message || e);
  }
}

// NOWPayments configuration
const NOWPAYMENTS_API_KEY = process.env.NOWPAYMENTS_API_KEY || '';
const NOWPAYMENTS_IPN_SECRET = process.env.NOWPAYMENTS_IPN_SECRET || '';
const NOWPAYMENTS_API_URL = process.env.NOWPAYMENTS_API_URL || 'https://api.nowpayments.io/v1';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || '';

// Bill.com / BILL configuration (invoice + payment link)
const BILLCOM_ENVIRONMENT = String(process.env.BILLCOM_ENVIRONMENT || 'sandbox').trim().toLowerCase(); // sandbox | production
const BILLCOM_API_URL = process.env.BILLCOM_API_URL || (
  BILLCOM_ENVIRONMENT === 'production'
    ? 'https://gateway.prod.bill.com/connect'
    : 'https://gateway.stage.bill.com/connect'
);
const BILLCOM_USERNAME = process.env.BILLCOM_USERNAME || '';
const BILLCOM_PASSWORD = process.env.BILLCOM_PASSWORD || '';
const BILLCOM_ORGANIZATION_ID = process.env.BILLCOM_ORGANIZATION_ID || '';
const BILLCOM_DEV_KEY = process.env.BILLCOM_DEV_KEY || '';
const BILLCOM_WEBHOOK_SECURITY_KEY = process.env.BILLCOM_WEBHOOK_SECURITY_KEY || '';

// Card payment provider selector
// - square (default): Square Payment Links
// - stripe: Stripe hosted Checkout
const CARD_PAYMENT_PROVIDER = String(process.env.CARD_PAYMENT_PROVIDER || 'square').trim().toLowerCase();

// Stripe configuration (hosted Checkout)
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';

let stripeApiClient;
function getStripeApiClient() {
  if (stripeApiClient) return stripeApiClient;
  const key = String(STRIPE_SECRET_KEY || '').trim();
  if (!key) throw new Error('STRIPE_SECRET_KEY not configured');
  stripeApiClient = Stripe(key);
  return stripeApiClient;
}

let stripeWebhookClient;
function getStripeWebhookClient() {
  if (stripeWebhookClient) return stripeWebhookClient;
  // Webhook signature verification does not require live API calls; use a dummy key if unset.
  const key = String(STRIPE_SECRET_KEY || '').trim() || 'sk_test_dummy';
  stripeWebhookClient = Stripe(key);
  return stripeWebhookClient;
}

function nowpaymentsAxios() {
  if (!NOWPAYMENTS_API_KEY) {
    throw new Error('NOWPayments API key not configured');
  }
  return axios.create({
    baseURL: NOWPAYMENTS_API_URL,
    timeout: 15000,
    headers: {
      'x-api-key': NOWPAYMENTS_API_KEY,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    }
  });
}

// ========== Bill.com / BILL helpers ==========
let billcomSessionId = null;
let billcomSessionLastUsedAt = 0;

function isBillcomConfigured() {
  return !!(BILLCOM_USERNAME && BILLCOM_PASSWORD && BILLCOM_ORGANIZATION_ID && BILLCOM_DEV_KEY);
}

async function billcomLogin() {
  if (!isBillcomConfigured()) {
    throw new Error('BILL.com is not configured');
  }

  const payload = {
    username: String(BILLCOM_USERNAME),
    password: String(BILLCOM_PASSWORD),
    organizationId: String(BILLCOM_ORGANIZATION_ID),
    devKey: String(BILLCOM_DEV_KEY)
  };

  const resp = await axios.post(`${BILLCOM_API_URL}/v3/login`, payload, {
    timeout: 30000,
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    validateStatus: () => true
  });

  if (!(resp.status >= 200 && resp.status < 300)) {
    const msg = (resp.data && (resp.data.message || resp.data.error))
      ? String(resp.data.message || resp.data.error)
      : `BILL.com login failed (${resp.status})`;
    throw new Error(msg);
  }

  const sid = resp?.data?.sessionId;
  if (!sid) {
    throw new Error('BILL.com login failed (missing sessionId)');
  }

  billcomSessionId = String(sid);
  billcomSessionLastUsedAt = Date.now();
  return billcomSessionId;
}

async function ensureBillcomSessionId() {
  const now = Date.now();
  const maxIdleMs = 34 * 60 * 1000; // keep below BILL's 35-minute idle expiry
  if (billcomSessionId && (now - billcomSessionLastUsedAt) < maxIdleMs) {
    billcomSessionLastUsedAt = now;
    return billcomSessionId;
  }
  return billcomLogin();
}

async function billcomApiCall({ method, path, body, params, retry = true }) {
  if (!isBillcomConfigured()) {
    throw new Error('BILL.com is not configured');
  }

  const sid = await ensureBillcomSessionId();
  const resp = await axios.request({
    method,
    url: `${BILLCOM_API_URL}${path}`,
    data: body,
    params,
    timeout: 30000,
    headers: {
      devKey: String(BILLCOM_DEV_KEY),
      sessionId: String(sid),
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    validateStatus: () => true
  });

  // If the session expired, retry once after re-login.
  if (retry && (resp.status === 401 || resp.status === 403)) {
    billcomSessionId = null;
    billcomSessionLastUsedAt = 0;
    return billcomApiCall({ method, path, body, params, retry: false });
  }

  if (!(resp.status >= 200 && resp.status < 300)) {
    const msg = (resp.data && (resp.data.message || resp.data.error))
      ? String(resp.data.message || resp.data.error)
      : `BILL.com API error (${resp.status})`;
    const err = new Error(msg);
    err.status = resp.status;
    err.data = resp.data;
    throw err;
  }

  billcomSessionLastUsedAt = Date.now();
  return resp.data;
}

// Square configuration (card payments via Square Payment Links)
const SQUARE_ENVIRONMENT = process.env.SQUARE_ENVIRONMENT || 'sandbox'; // 'sandbox' or 'production'
const SQUARE_APPLICATION_ID = process.env.SQUARE_APPLICATION_ID || '';
const SQUARE_ACCESS_TOKEN = process.env.SQUARE_ACCESS_TOKEN || '';
const SQUARE_LOCATION_ID = process.env.SQUARE_LOCATION_ID || '';
const SQUARE_WEBHOOK_SIGNATURE_KEY = process.env.SQUARE_WEBHOOK_SIGNATURE_KEY || '';
const SQUARE_WEBHOOK_NOTIFICATION_URL = process.env.SQUARE_WEBHOOK_NOTIFICATION_URL || '';

function squareApiBaseUrl() {
  return SQUARE_ENVIRONMENT === 'production'
    ? 'https://connect.squareup.com'
    : 'https://connect.squareupsandbox.com';
}

let cachedSquareLocationId = SQUARE_LOCATION_ID || null;
async function getSquareLocationId() {
  if (cachedSquareLocationId) return cachedSquareLocationId;
  if (!SQUARE_ACCESS_TOKEN) {
    throw new Error('Square access token not configured');
  }
  try {
    const baseUrl = squareApiBaseUrl();
    const resp = await axios.get(`${baseUrl}/v2/locations`, {
      timeout: 15000,
      headers: {
        'Authorization': `Bearer ${SQUARE_ACCESS_TOKEN}`,
        'Square-Version': '2025-10-16',
        'Accept': 'application/json'
      }
    });
    const locations = resp?.data?.locations || [];
    const active = locations.find(l => l.status === 'ACTIVE') || locations[0];
    if (active && active.id) {
      cachedSquareLocationId = active.id;
      if (DEBUG) console.log('[square.locations] Using location id:', cachedSquareLocationId);
      return cachedSquareLocationId;
    }
    if (DEBUG) console.warn('[square.locations] No locations returned from Square');
    return null;
  } catch (e) {
    if (DEBUG) console.error('[square.locations] Failed to fetch locations:', e.response?.data || e.message || e);
    return null;
  }
}

// Verify NOWPayments IPN HMAC signature (x-nowpayments-sig)
function verifyNowpaymentsSignature(req) {
  try {
    if (!NOWPAYMENTS_IPN_SECRET) {
      if (DEBUG) console.warn('[nowpayments.ipn] NOWPAYMENTS_IPN_SECRET not set; skipping signature verification');
      return true; // Do not block if secret not configured yet, but log
    }
    const signature = req.headers['x-nowpayments-sig'] || req.headers['x-nowpayments-signature'];
    if (!signature) return false;
    const raw = req.rawBody;
    if (!raw || !Buffer.isBuffer(raw)) return false;
    const hmac = crypto.createHmac('sha512', NOWPAYMENTS_IPN_SECRET);
    hmac.update(raw);
    const expected = hmac.digest('hex');
    // Use timing-safe compare
    const a = Buffer.from(expected, 'utf8');
    const b = Buffer.from(String(signature), 'utf8');
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch (e) {
    if (DEBUG) console.warn('[nowpayments.ipn] Signature verification error:', e.message || e);
    return false;
  }
}

// Verify Square webhook signature (x-square-hmacsha256-signature)
// Square signs the body as: HMAC_SHA256( notification_url + raw_body ) using the
// subscription's "Signature key". We try both the configured notification URL
// and the actual request URL in case there is a mismatch (e.g. proxy changes).
function verifySquareSignature(req) {
  try {
    if (!SQUARE_WEBHOOK_SIGNATURE_KEY) {
      if (DEBUG) console.warn('[square.webhook] Signature key not set; skipping verification');
      return true; // do not block if key not configured yet
    }

    const signature = req.headers['x-square-hmacsha256-signature'];
    if (!signature) {
      if (DEBUG) console.warn('[square.webhook] Missing x-square-hmacsha256-signature header');
      return false;
    }

    const raw = req.rawBody;
    if (!raw || !Buffer.isBuffer(raw)) {
      if (DEBUG) console.warn('[square.webhook] Missing rawBody for signature verification');
      return false;
    }

    const urlsToTry = [];
    if (SQUARE_WEBHOOK_NOTIFICATION_URL) urlsToTry.push(SQUARE_WEBHOOK_NOTIFICATION_URL);
    const actualUrl = `${req.protocol}://${req.get('host')}${req.originalUrl}`;
    if (!urlsToTry.includes(actualUrl)) urlsToTry.push(actualUrl);

    const headerSig = String(signature);

    for (const url of urlsToTry) {
      const payload = String(url || '') + raw.toString('utf8');
      const expected = crypto
        .createHmac('sha256', SQUARE_WEBHOOK_SIGNATURE_KEY)
        .update(payload)
        .digest('base64');
      const a = Buffer.from(expected, 'utf8');
      const b = Buffer.from(headerSig, 'utf8');
      if (a.length === b.length && crypto.timingSafeEqual(a, b)) {
        if (DEBUG) console.log('[square.webhook] Signature verified', { urlUsed: url });
        return true;
      }
    }

    if (DEBUG) {
      console.warn('[square.webhook] Signature mismatch', {
        header: headerSig,
        triedUrls: urlsToTry
      });
    }
    return false;
  } catch (e) {
    if (DEBUG) console.warn('[square.webhook] Signature verification error:', e.message || e);
    return false;
  }
}

// Verify Bill.com / BILL webhook signature (x-bill-sha-signature)
// Bill computes: base64(HMAC_SHA256(raw_json_body, securityKey))
function verifyBillcomWebhookSignature(req) {
  try {
    if (!BILLCOM_WEBHOOK_SECURITY_KEY) {
      if (DEBUG) console.warn('[billcom.webhook] Security key not set; skipping verification');
      return true;
    }

    const signature = req.headers['x-bill-sha-signature'];
    if (!signature) {
      if (DEBUG) console.warn('[billcom.webhook] Missing x-bill-sha-signature header');
      return false;
    }

    const raw = req.rawBody;
    if (!raw || !Buffer.isBuffer(raw)) {
      if (DEBUG) console.warn('[billcom.webhook] Missing rawBody for signature verification');
      return false;
    }

    const expected = crypto
      .createHmac('sha256', String(BILLCOM_WEBHOOK_SECURITY_KEY))
      .update(raw)
      .digest('base64');

    const a = Buffer.from(expected, 'utf8');
    const b = Buffer.from(String(signature), 'utf8');
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch (e) {
    if (DEBUG) console.warn('[billcom.webhook] Signature verification error:', e.message || e);
    return false;
  }
}


// ========== Phone.System (telecom.center) ==========
const PHONE_SYSTEM_MAX_CUSTOMERS = Math.max(1, parseInt(process.env.PHONE_SYSTEM_MAX_CUSTOMERS || '5', 10) || 5);

function tcId(val) {
  return String(val || '').trim();
}

function toE164Plus(digits) {
  const d = digitsOnly(digits);
  if (!d) return '';
  return d.startsWith('1') && d.length === 11 ? `+${d}` : `+${d}`;
}

function genTcCustomerCreateId() {
  // telecom.center Customer create requires a client-supplied id (numeric).
  // Keep it within signed 32-bit range to be safe.
  const n = Math.floor(1000000000 + Math.random() * 1000000000);
  return String(n);
}

async function getPhoneSystemCustomerMapping({ userId, tcCustomerId }) {
  const [rows] = await pool.execute(
    'SELECT tc_customer_id, name, created_at, updated_at FROM phonesystem_customers WHERE user_id = ? AND tc_customer_id = ? LIMIT 1',
    [userId, tcCustomerId]
  );
  return rows && rows[0] ? rows[0] : null;
}

async function getPhoneSystemIncomingTrunkMapping({ userId, tcCustomerId }) {
  const [rows] = await pool.execute(
    'SELECT id, tc_incoming_trunk_id, domain, destination_field, forwarding_trunk_id FROM phonesystem_incoming_trunks WHERE user_id = ? AND tc_customer_id = ? ORDER BY ID DESC LIMIT 1',
    [userId, tcCustomerId]
  );
  return rows && rows[0] ? rows[0] : null;
}

async function tcDeleteIgnoreNotFound(path) {
  const rel = String(path || '').trim();
  if (!rel) return { ok: true, deleted: false };
  try {
    await telecomCenterApiCall({ method: 'DELETE', path: rel });
    return { ok: true, deleted: true };
  } catch (e) {
    const status = e?.status != null ? Number(e.status) : null;
    if (status === 404) return { ok: true, deleted: false };
    throw e;
  }
}

async function tcGetDefaultIncomingTrunkForCustomer(tcCustomerId) {
  const cid = tcId(tcCustomerId);
  if (!cid) return null;

  // JSON:API filter by customer.id
  const path = `/incoming_trunks?filter[customer.id]=${encodeURIComponent(cid)}`;
  const tc = await telecomCenterApiCall({ method: 'GET', path });

  const list = Array.isArray(tc?.data) ? tc.data : [];
  if (!list.length) return null;

  const pick = list.find(t => String(t?.attributes?.name || '').trim().toLowerCase() === 'default') || list[0];
  const domain = String(pick?.attributes?.domain || '').trim();

  return {
    tc_incoming_trunk_id: tcId(pick?.id),
    domain: domain || null,
    destination_field: String(pick?.attributes?.destination_field || '').trim() || null,
    name: String(pick?.attributes?.name || '').trim() || null
  };
}


function detectTollfreeUsCaByNpa(number) {
  const tollfreeNpas = new Set(['800', '833', '844', '855', '866', '877', '888']);
  const rawNum = String(number || '').replace(/\D/g, '');
  if (!rawNum) return false;
  const digits = rawNum.length > 11 ? rawNum.slice(-11) : rawNum;
  const npa = digits.startsWith('1') ? digits.slice(1, 4) : digits.slice(0, 3);
  return tollfreeNpas.has(npa);
}

function rateAiInboundCallPrice({ toNumber, billsec }) {
  const sec = Number(billsec || 0);
  if (!Number.isFinite(sec) || sec <= 0) return { price: 0, isTollfree: false, ratePerMin: 0, billedUnits: 0 };

  const isTollfree = detectTollfreeUsCaByNpa(toNumber);
  const ratePerMin = isTollfree ? AI_INBOUND_TOLLFREE_RATE_PER_MIN : AI_INBOUND_LOCAL_RATE_PER_MIN;
  if (!ratePerMin || ratePerMin <= 0) return { price: 0, isTollfree, ratePerMin: 0, billedUnits: 0 };

  let price = 0;
  let billedUnits = 0;
  if (AI_INBOUND_BILLING_ROUND_UP_TO_MINUTE) {
    billedUnits = Math.ceil(sec / 60); // whole minutes
    price = billedUnits * ratePerMin;
  } else {
    billedUnits = sec; // seconds
    price = sec * (ratePerMin / 60);
  }

  // Keep enough precision for per-second pricing while staying reasonable for UI
  const rounded = Number(price.toFixed(6));
  return { price: Number.isFinite(rounded) ? rounded : 0, isTollfree, ratePerMin, billedUnits };
}
async function chargeAiInboundCallLog({ aiCallLogId, userId, amount, description }) {
  if (!pool || !aiCallLogId || !userId) return { ok: false, reason: 'missing_params' };

  const callLogId = Number(aiCallLogId);
  const uid = Number(userId);
  const amt = Math.max(0, Number(amount) || 0);
  if (!Number.isFinite(callLogId) || !Number.isFinite(uid)) return { ok: false, reason: 'invalid_params' };

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const [rows] = await conn.query(
      `SELECT id, call_id, to_number, price, billed, billing_history_id
       FROM ai_call_logs
       WHERE id = ? AND user_id = ?
       LIMIT 1
       FOR UPDATE`,
      [callLogId, uid]
    );
    const row = rows && rows[0] ? rows[0] : null;
    if (!row) {
      await conn.rollback();
      return { ok: false, reason: 'call_log_not_found' };
    }

    const alreadyBilled = Number(row.billed || 0) === 1 || !!row.billing_history_id;
    if (alreadyBilled) {
      await conn.commit();
      return { ok: true, alreadyBilled: true, billingId: row.billing_history_id ? Number(row.billing_history_id) : null };
    }

    const amountToCharge = amt > 0 ? amt : Math.max(0, Number(row.price || 0));
    if (!(amountToCharge > 0)) {
      await conn.query(
        'UPDATE ai_call_logs SET billed = 1, price = COALESCE(price, 0), updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
        [callLogId]
      );
      await conn.commit();
      return { ok: true, skipped: true, reason: 'zero_amount' };
    }

    const [uRows] = await conn.query(
      'SELECT balance FROM users WHERE id = ? AND is_active = 1 LIMIT 1 FOR UPDATE',
      [uid]
    );
    const u = uRows && uRows[0] ? uRows[0] : null;
    if (!u) throw new Error('User not found or inactive');

    const balanceBefore = Number(u.balance || 0);
    const balanceAfter = balanceBefore - amountToCharge;
    const desc = String(description || `AI inbound call – ${row.to_number || row.call_id || callLogId}`)
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 255);

    await conn.query(
      'UPDATE users SET balance = ?, updated_at = NOW() WHERE id = ?',
      [balanceAfter, uid]
    );

    const [ins] = await conn.query(
      `INSERT INTO billing_history
       (user_id, amount, description, transaction_type, payment_method, reference_id,
        balance_before, balance_after, status, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', NOW())`,
      [uid, -Math.abs(amountToCharge), desc, 'debit', 'ai_inbound', row.call_id || null, balanceBefore, balanceAfter]
    );
    const billingId = ins && ins.insertId ? Number(ins.insertId) : null;

    await conn.query(
      'UPDATE ai_call_logs SET billed = 1, billing_history_id = ?, price = COALESCE(price, ?), updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
      [billingId, amountToCharge, callLogId]
    );

    await conn.commit();
    return { ok: true, billingId, amount: amountToCharge, balanceBefore, balanceAfter };
  } catch (e) {
    try { await conn.rollback(); } catch {}
    if (DEBUG) console.warn('[ai.call.bill] chargeAiInboundCallLog failed:', e?.message || e);
    return { ok: false, reason: e?.message || 'charge_failed' };
  } finally {
    conn.release();
  }
}

async function billUnchargedAiInboundCallsForUser({ localUserId, limit = 200 }) {
  if (!pool || !localUserId) return { processed: 0, charged: 0, skipped: 0, failed: 0 };
  try {
    const safeLimit = Math.max(1, Math.min(1000, parseInt(String(limit || '200'), 10) || 200));
    const [rows] = await pool.query(
      `SELECT id, call_id, to_number, billsec, duration, price
       FROM ai_call_logs
       WHERE user_id = ?
         AND direction = 'inbound'
         AND time_end IS NOT NULL
         AND time_connect IS NOT NULL
         AND COALESCE(billsec, duration, 0) > 0
         AND billed = 0
       ORDER BY time_end ASC, id ASC
       LIMIT ${safeLimit}`,
      [localUserId]
    );

    let processed = 0;
    let charged = 0;
    let skipped = 0;
    let failed = 0;

    for (const r of rows || []) {
      processed += 1;
      const billsec = (r.billsec != null ? Number(r.billsec) : (r.duration != null ? Number(r.duration) : 0)) || 0;
      const rate = rateAiInboundCallPrice({ toNumber: r.to_number, billsec });
      const amount = (Number(r.price || 0) > 0) ? Number(r.price || 0) : Number(rate.price || 0);
      const typeLabel = rate.isTollfree ? 'toll-free' : 'local';
      const desc = `AI inbound ${typeLabel} call – ${String(r.to_number || '').trim() || String(r.call_id || '')}`.slice(0, 255);

      const charge = await chargeAiInboundCallLog({
        aiCallLogId: r.id,
        userId: localUserId,
        amount,
        description: desc
      });

      if (charge && charge.ok && !charge.alreadyBilled && !charge.skipped) charged += 1;
      else if (charge && (charge.alreadyBilled || charge.skipped)) skipped += 1;
      else failed += 1;
    }

    return { processed, charged, skipped, failed };
  } catch (e) {
    if (DEBUG) console.warn('[ai.call.bill] billUnchargedAiInboundCallsForUser failed:', e?.message || e);
    return { processed: 0, charged: 0, skipped: 0, failed: 0 };
  }
}

// Backfill/compute per-call price for AI call logs. This is separate from charging the user.
async function backfillAiCallPricesForUser({ localUserId, limit = 500 }) {
  if (!pool || !localUserId) return;
  try {
    const safeLimit = Math.max(1, Math.min(2000, parseInt(String(limit || '500'), 10) || 500));

    // Only price ended calls; price is based on billsec (fallback: duration)
    const [rows] = await pool.query(
      `SELECT id, to_number, billsec, duration
       FROM ai_call_logs
       WHERE user_id = ?
         AND direction = 'inbound'
         AND time_end IS NOT NULL
         AND time_connect IS NOT NULL
         AND COALESCE(billsec, duration, 0) > 0
         AND (price IS NULL OR price <= 0)
       ORDER BY time_end DESC, id DESC
       LIMIT ${safeLimit}`,
      [localUserId]
    );

    for (const r of rows || []) {
      const billsec = (r.billsec != null ? Number(r.billsec) : (r.duration != null ? Number(r.duration) : 0)) || 0;
      const { price } = rateAiInboundCallPrice({ toNumber: r.to_number, billsec });
      if (!price || price <= 0) continue;
      try {
        await pool.execute(
          'UPDATE ai_call_logs SET price = ? WHERE id = ? AND (price IS NULL OR price <= 0)',
          [price, r.id]
        );
      } catch (e) {
        if (DEBUG) console.warn('[ai.call.rate] Failed to update ai_call_logs.price:', e.message || e);
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[ai.call.rate] Backfill failed:', e.message || e);
  }
}

function rateDialerOutboundCallPrice({ billsec }) {
  const sec = Number(billsec || 0);
  const ratePerMin = Number(DIALER_OUTBOUND_RATE_PER_MIN || 0);
  if (!Number.isFinite(sec) || sec <= 0 || !ratePerMin || ratePerMin <= 0) {
    return { price: 0, ratePerMin: ratePerMin || 0, billedUnits: 0 };
  }

  let price = 0;
  let billedUnits = 0;
  if (DIALER_OUTBOUND_BILLING_ROUND_UP_TO_MINUTE) {
    billedUnits = Math.ceil(sec / 60);
    price = billedUnits * ratePerMin;
  } else {
    billedUnits = sec;
    price = sec * (ratePerMin / 60);
  }

  const rounded = Number(price.toFixed(6));
  return { price: Number.isFinite(rounded) ? rounded : 0, ratePerMin, billedUnits };
}

// Backfill/compute per-call price for dialer outbound calls.
async function backfillDialerCallPricesForUser({ localUserId, limit = 500 }) {
  if (!pool || !localUserId) return;
  if (!DIALER_OUTBOUND_RATE_PER_MIN || DIALER_OUTBOUND_RATE_PER_MIN <= 0) return;
  try {
    const safeLimit = Math.max(1, Math.min(2000, parseInt(String(limit || '500'), 10) || 500));
    const [rows] = await pool.query(
      `SELECT l.id, l.duration_sec
       FROM dialer_call_logs l
       WHERE l.user_id = ?
         AND COALESCE(l.duration_sec, 0) > 0
         AND (l.price IS NULL OR l.price <= 0)
         AND l.status IN ('completed','connected')
         AND (l.result IS NULL OR l.result <> 'failed')
       ORDER BY l.created_at DESC, l.id DESC
       LIMIT ${safeLimit}`,
      [localUserId]
    );

    for (const r of rows || []) {
      const billsec = Number(r.duration_sec || 0);
      const { price } = rateDialerOutboundCallPrice({ billsec });
      if (!price || price <= 0) continue;
      try {
        await pool.execute(
          'UPDATE dialer_call_logs SET price = ? WHERE id = ? AND (price IS NULL OR price <= 0)',
          [price, r.id]
        );
      } catch (e) {
        if (DEBUG) console.warn('[dialer.call.rate] Failed to update dialer_call_logs.price:', e.message || e);
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[dialer.call.rate] Backfill failed:', e.message || e);
  }
}

function addMonthsClamped(date, months) {
  const d = new Date(date);
  if (!Number.isFinite(d.getTime())) return null;
  const day = d.getDate();
  d.setMonth(d.getMonth() + (Number(months) || 0));
  // Clamp overflow (e.g. Jan 31 + 1 month)
  if (d.getDate() < day) {
    d.setDate(0);
  }
  return d;
}


async function ensurePhoneSystemIncomingTrunkMapping({ userId, tcCustomerId, maxAttempts = 4 }) {
  if (!pool) throw new Error('Database not configured');

  const cid = tcId(tcCustomerId);
  if (!cid) return null;

  const existing = await getPhoneSystemIncomingTrunkMapping({ userId, tcCustomerId: cid });
  if (existing && existing.domain) return existing;

  let found = null;
  let lastErr = null;

  for (let attempt = 0; attempt < Math.max(1, Number(maxAttempts) || 4); attempt += 1) {
    try {
      found = await tcGetDefaultIncomingTrunkForCustomer(cid);
      if (found && found.domain) break;
    } catch (e) {
      lastErr = e;
    }

    // Give telecom.center a moment if trunks are created asynchronously.
    await new Promise(r => setTimeout(r, 250));
  }

  if (!found || !found.domain) {
    if (lastErr && DEBUG) console.warn('[phonesystem.incoming-trunks.default] Failed to fetch default trunk:', lastErr?.message || lastErr);
    return existing;
  }

  if (existing && existing.id) {
    try {
      await pool.execute(
        'UPDATE phonesystem_incoming_trunks SET tc_incoming_trunk_id = ?, domain = ?, destination_field = ? WHERE id = ? LIMIT 1',
        [found.tc_incoming_trunk_id || null, found.domain || null, found.destination_field || null, existing.id]
      );
    } catch {}

    existing.tc_incoming_trunk_id = found.tc_incoming_trunk_id || existing.tc_incoming_trunk_id;
    existing.domain = found.domain || existing.domain;
    existing.destination_field = found.destination_field || existing.destination_field;
    return existing;
  }

  try {
    await pool.execute(
      'INSERT INTO phonesystem_incoming_trunks (user_id, tc_customer_id, tc_incoming_trunk_id, domain, destination_field, forwarding_trunk_id) VALUES (?, ?, ?, ?, ?, ?)',
      [userId, cid, found.tc_incoming_trunk_id || null, found.domain || null, found.destination_field || null, null]
    );
  } catch {}

  return await getPhoneSystemIncomingTrunkMapping({ userId, tcCustomerId: cid });
}

app.get('/api/me/phonesystem/customers', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;

    const [rows] = await pool.execute(
      `SELECT 
         c.tc_customer_id,
         c.name,
         c.created_at,
         t.tc_incoming_trunk_id,
         t.domain,
         t.destination_field,
         t.forwarding_trunk_id,
         (SELECT COUNT(*) FROM phonesystem_dids d WHERE d.user_id = c.user_id AND d.tc_customer_id = c.tc_customer_id) AS did_count
       FROM phonesystem_customers c
       LEFT JOIN phonesystem_incoming_trunks t
         ON t.user_id = c.user_id AND t.tc_customer_id = c.tc_customer_id
       WHERE c.user_id = ?
       ORDER BY c.id DESC`,
      [userId]
    );

    const out = rows || [];

    // If telecom.center is configured to auto-create a default incoming trunk per customer,
    // sync the domain into our local mapping so the UI can display it.
    for (const r of out) {
      const cid = tcId(r?.tc_customer_id);
      if (!cid) continue;
      const hasDomain = String(r?.domain || '').trim();
      if (hasDomain) continue;

      try {
        const incoming = await ensurePhoneSystemIncomingTrunkMapping({ userId, tcCustomerId: cid });
        if (incoming && incoming.domain) {
          r.tc_incoming_trunk_id = incoming.tc_incoming_trunk_id || r.tc_incoming_trunk_id;
          r.domain = incoming.domain || r.domain;
          r.destination_field = incoming.destination_field || r.destination_field;
          r.forwarding_trunk_id = incoming.forwarding_trunk_id || r.forwarding_trunk_id;
        }
      } catch (e) {
        if (DEBUG) console.warn('[phonesystem.customers.list] Failed to sync default incoming trunk:', cid, e?.message || e);
      }
    }

    return res.json({ success: true, data: out });
  } catch (e) {
    if (DEBUG) console.error('[phonesystem.customers.list] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to load Phone.System customers' });
  }
});

app.post('/api/me/phonesystem/customers', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;
    const nameRaw = String(req.body?.name || '').trim();
    if (!nameRaw) {
      return res.status(400).json({ success: false, message: 'name is required' });
    }
    const name = nameRaw.slice(0, 255);

    const [cntRows] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM phonesystem_customers WHERE user_id = ?',
      [userId]
    );
    const cnt = cntRows && cntRows[0] && cntRows[0].cnt != null ? Number(cntRows[0].cnt) : 0;
    if (cnt >= PHONE_SYSTEM_MAX_CUSTOMERS) {
      return res.status(409).json({
        success: false,
        message: `You can only create up to ${PHONE_SYSTEM_MAX_CUSTOMERS} Phone.System accounts.`
      });
    }

    // telecom.center Customer create requires data.id (client-supplied).
    let tc = null;
    let lastErr = null;

    for (let attempt = 0; attempt < 6; attempt += 1) {
      const customerId = genTcCustomerCreateId();
      const payload = {
        data: {
          type: 'customers',
          id: customerId,
          attributes: {
            name,
            // Allow operator-managed termination plus customer-managed if needed.
            trm_mode: 'operator_customer'
          }
        }
      };

      try {
        tc = await telecomCenterApiCall({ method: 'POST', path: '/customers', body: payload });
        break;
      } catch (err) {
        lastErr = err;
        const msg = String(err?.message || '').toLowerCase();
        const canRetry = msg.includes('has already been taken') || msg.includes('already exists');
        if (!canRetry) throw err;
      }
    }

    if (!tc) throw lastErr || new Error('Failed to create telecom.center customer');

    const tcIdVal = tcId(tc?.data?.id);
    if (!tcIdVal) return res.status(502).json({ success: false, message: 'telecom.center customer create succeeded but returned no id' });

    await pool.execute(
      'INSERT INTO phonesystem_customers (user_id, tc_customer_id, name) VALUES (?, ?, ?)',
      [userId, tcIdVal, name]
    );

    // Best-effort: sync the default incoming trunk domain created by phone.systems.
    let incoming = null;
    try {
      incoming = await ensurePhoneSystemIncomingTrunkMapping({ userId, tcCustomerId: tcIdVal, maxAttempts: 6 });
    } catch {}

    return res.json({
      success: true,
      data: {
        tc_customer_id: tcIdVal,
        name,
        domain: incoming && incoming.domain ? String(incoming.domain) : null
      }
    });
  } catch (e) {
    const msg = e?.message || 'Failed to create Phone.System customer';
    const status = e?.status && Number.isFinite(Number(e.status)) ? Number(e.status) : 500;
    if (DEBUG) console.error('[phonesystem.customers.create] error:', { status, msg, data: e?.data });
    return res.status(status >= 400 && status < 600 ? status : 500).json({ success: false, message: msg });
  }
});

app.delete('/api/me/phonesystem/customers/:customerId', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;
    const tcCustomerId = tcId(req.params.customerId);
    if (!tcCustomerId) return res.status(400).json({ success: false, message: 'Missing customerId' });

    const row = await getPhoneSystemCustomerMapping({ userId, tcCustomerId });
    if (!row) return res.status(404).json({ success: false, message: 'Phone.System customer not found' });

    // Load local mappings so we can clean up remote resources.
    const [didRows] = await pool.execute(
      'SELECT tc_available_did_number_id FROM phonesystem_dids WHERE user_id = ? AND tc_customer_id = ?',
      [userId, tcCustomerId]
    );
    const [incomingRows] = await pool.execute(
      'SELECT tc_incoming_trunk_id FROM phonesystem_incoming_trunks WHERE user_id = ? AND tc_customer_id = ?',
      [userId, tcCustomerId]
    );
    const [gwRows] = await pool.execute(
      'SELECT tc_termination_gateway_id FROM phonesystem_termination_gateways WHERE user_id = ? AND tc_customer_id = ?',
      [userId, tcCustomerId]
    );
    const [rtRows] = await pool.execute(
      'SELECT tc_termination_route_id FROM phonesystem_termination_routes WHERE user_id = ? AND tc_customer_id = ?',
      [userId, tcCustomerId]
    );

    const tcDidIds = Array.from(new Set((didRows || []).map(r => String(r.tc_available_did_number_id || '').trim()).filter(Boolean)));
    const tcIncomingIds = Array.from(new Set((incomingRows || []).map(r => String(r.tc_incoming_trunk_id || '').trim()).filter(Boolean)));
    const tcGatewayIds = Array.from(new Set((gwRows || []).map(r => String(r.tc_termination_gateway_id || '').trim()).filter(Boolean)));
    const tcRouteIds = Array.from(new Set((rtRows || []).map(r => String(r.tc_termination_route_id || '').trim()).filter(Boolean)));

    // Delete telecom.center resources (best-effort per resource). Order matters.
    for (const id of tcRouteIds) {
      try { await tcDeleteIgnoreNotFound(`/termination_routes/${encodeURIComponent(id)}`); } catch (e) {
        if (DEBUG) console.warn('[phonesystem.delete.tc] termination_route delete failed:', id, e?.message || e);
      }
    }
    for (const id of tcGatewayIds) {
      try { await tcDeleteIgnoreNotFound(`/termination_gateways/${encodeURIComponent(id)}`); } catch (e) {
        if (DEBUG) console.warn('[phonesystem.delete.tc] termination_gateway delete failed:', id, e?.message || e);
      }
    }
    for (const id of tcDidIds) {
      try { await tcDeleteIgnoreNotFound(`/available_did_numbers/${encodeURIComponent(id)}`); } catch (e) {
        if (DEBUG) console.warn('[phonesystem.delete.tc] available_did_number delete failed:', id, e?.message || e);
      }
    }
    for (const id of tcIncomingIds) {
      try { await tcDeleteIgnoreNotFound(`/incoming_trunks/${encodeURIComponent(id)}`); } catch (e) {
        if (DEBUG) console.warn('[phonesystem.delete.tc] incoming_trunk delete failed:', id, e?.message || e);
      }
    }

    // Delete telecom.center customer (required). If this fails, keep local mapping so user can retry.
    try {
      await tcDeleteIgnoreNotFound(`/customers/${encodeURIComponent(tcCustomerId)}`);
    } catch (e) {
      const msg = e?.message || 'Failed to delete telecom.center customer';
      const status = e?.status && Number.isFinite(Number(e.status)) ? Number(e.status) : 502;
      if (DEBUG) console.error('[phonesystem.customers.delete.remote] error:', { status, msg, data: e?.data });
      return res.status(status >= 400 && status < 600 ? status : 502).json({ success: false, message: msg });
    }

    // Local cleanup (mappings/settings)
    try { await pool.execute('DELETE FROM phonesystem_dids WHERE user_id = ? AND tc_customer_id = ?', [userId, tcCustomerId]); } catch {}
    try { await pool.execute('DELETE FROM phonesystem_incoming_trunks WHERE user_id = ? AND tc_customer_id = ?', [userId, tcCustomerId]); } catch {}
    try { await pool.execute('DELETE FROM phonesystem_termination_routes WHERE user_id = ? AND tc_customer_id = ?', [userId, tcCustomerId]); } catch {}
    try { await pool.execute('DELETE FROM phonesystem_termination_gateways WHERE user_id = ? AND tc_customer_id = ?', [userId, tcCustomerId]); } catch {}

    await pool.execute('DELETE FROM phonesystem_customers WHERE user_id = ? AND tc_customer_id = ? LIMIT 1', [userId, tcCustomerId]);

    return res.json({ success: true });
  } catch (e) {
    if (DEBUG) console.error('[phonesystem.customers.delete] error:', e.message || e);
    return res.status(500).json({ success: false, message: 'Failed to delete Phone.System customer' });
  }
});

app.post('/api/me/phonesystem/customers/:customerId/incoming-trunks', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;
    const tcCustomerId = tcId(req.params.customerId);
    if (!tcCustomerId) return res.status(400).json({ success: false, message: 'Missing customerId' });

    const customer = await getPhoneSystemCustomerMapping({ userId, tcCustomerId });
    if (!customer) return res.status(404).json({ success: false, message: 'Phone.System customer not found' });

    // phone.systems can auto-create a default incoming trunk for new customers.
    // This endpoint now syncs that default trunk into our local mapping.
    const incoming = await ensurePhoneSystemIncomingTrunkMapping({ userId, tcCustomerId, maxAttempts: 6 });
    if (!incoming || !incoming.domain) {
      return res.status(409).json({
        success: false,
        message: 'Incoming trunk domain not found for this customer. Please verify phone.systems inbound trunk setup.'
      });
    }

    return res.json({ success: true, data: incoming, reused: true });
  } catch (e) {
    const msg = e?.message || 'Failed to sync incoming trunk';
    const status = e?.status && Number.isFinite(Number(e.status)) ? Number(e.status) : 500;
    if (DEBUG) console.error('[phonesystem.incoming-trunks.sync] error:', { status, msg, data: e?.data });
    return res.status(status >= 400 && status < 600 ? status : 500).json({ success: false, message: msg });
  }
});

app.post('/api/me/phonesystem/customers/:customerId/dids', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;
    const tcCustomerId = tcId(req.params.customerId);
    if (!tcCustomerId) return res.status(400).json({ success: false, message: 'Missing customerId' });

    const didIds = Array.isArray(req.body?.did_ids) ? req.body.did_ids.map(v => String(v || '').trim()).filter(Boolean) : [];
    if (!didIds.length) return res.status(400).json({ success: false, message: 'did_ids is required' });

    const customer = await getPhoneSystemCustomerMapping({ userId, tcCustomerId });
    if (!customer) return res.status(404).json({ success: false, message: 'Phone.System customer not found' });

    const incoming = await ensurePhoneSystemIncomingTrunkMapping({ userId, tcCustomerId, maxAttempts: 6 });
    if (!incoming || !incoming.domain) {
      return res.status(409).json({
        success: false,
        message: 'Incoming trunk domain not found for this customer. Please verify phone.systems inbound trunk setup.'
      });
    }

    const created = [];
    const errors = [];

    for (const didId of didIds) {
      try {
        // Ensure DID belongs to user and is active
        const [dRows] = await pool.execute(
          'SELECT id, did_number, cancel_pending FROM user_dids WHERE user_id = ? AND did_id = ? LIMIT 1',
          [userId, didId]
        );
        const dRow = dRows && dRows[0] ? dRows[0] : null;
        if (!dRow) throw new Error('You do not own this DID');
        if (dRow.cancel_pending) throw new Error('This DID is being cancelled and cannot be assigned');

        const didNumber = digitsOnly(dRow.did_number);
        if (!didNumber) throw new Error('Invalid DID number');

        // Prevent re-assigning the same DID to multiple customers
        const [existingRows] = await pool.execute(
          'SELECT tc_customer_id, tc_available_did_number_id FROM phonesystem_dids WHERE user_id = ? AND did_id = ? LIMIT 1',
          [userId, didId]
        );
        const existing = existingRows && existingRows[0] ? existingRows[0] : null;
        if (existing) {
          if (String(existing.tc_customer_id) !== tcCustomerId) {
            throw new Error('This DID is already assigned to another Phone.System customer');
          }
        } else {
          const externalId = parseInt(didNumber, 10);

          const payload = {
            data: {
              type: 'available_did_numbers',
              attributes: {
                number: didNumber,
                enabled: true,
                external_id: Number.isFinite(externalId) ? externalId : undefined
              },
              relationships: {
                customer: { data: { type: 'customers', id: tcCustomerId } }
              }
            }
          };

          // Remove undefined keys (JSON:API params must be strict)
          if (payload.data.attributes.external_id === undefined) {
            delete payload.data.attributes.external_id;
          }

          const tc = await telecomCenterApiCall({ method: 'POST', path: '/available_did_numbers', body: payload });
          const tcDidId = tcId(tc?.data?.id);
          if (!tcDidId) throw new Error('telecom.center DID create returned no id');

          await pool.execute(
            'INSERT INTO phonesystem_dids (user_id, tc_customer_id, tc_available_did_number_id, did_id, did_number, enabled) VALUES (?, ?, ?, ?, ?, 1)',
            [userId, tcCustomerId, tcDidId, didId, didNumber]
          );

          created.push({ did_id: didId, did_number: didNumber, tc_available_did_number_id: tcDidId });
        }
      } catch (err) {
        errors.push({ did_id: didId, message: err?.message || String(err) });
      }
    }

    return res.json({
      success: true,
      data: {
        domain: String(incoming.domain),
        sip_uri_example: `${toE164Plus('1' + '0'.repeat(10))}@${String(incoming.domain)}`,
        created,
        errors
      }
    });
  } catch (e) {
    const msg = e?.message || 'Failed to assign DIDs';
    const status = e?.status && Number.isFinite(Number(e.status)) ? Number(e.status) : 500;
    if (DEBUG) console.error('[phonesystem.dids.assign] error:', { status, msg, data: e?.data });
    return res.status(status >= 400 && status < 600 ? status : 500).json({ success: false, message: msg });
  }
});

app.post('/api/me/phonesystem/customers/:customerId/outbound', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;
    const tcCustomerId = tcId(req.params.customerId);
    if (!tcCustomerId) return res.status(400).json({ success: false, message: 'Missing customerId' });

    const sipId = String(req.body?.sip_id || '').trim();
    if (!sipId) return res.status(400).json({ success: false, message: 'sip_id is required' });

    const customer = await getPhoneSystemCustomerMapping({ userId, tcCustomerId });
    if (!customer) return res.status(404).json({ success: false, message: 'Phone.System customer not found' });

    // Idempotency: if we already have a gateway+route for this customer, return it.
    const [gwRows] = await pool.execute(
      'SELECT tc_termination_gateway_id, host FROM phonesystem_termination_gateways WHERE user_id = ? AND tc_customer_id = ? ORDER BY id DESC LIMIT 1',
      [userId, tcCustomerId]
    );
    const [rtRows] = await pool.execute(
      'SELECT tc_termination_route_id, name FROM phonesystem_termination_routes WHERE user_id = ? AND tc_customer_id = ? ORDER BY id DESC LIMIT 1',
      [userId, tcCustomerId]
    );
    const existingGw = gwRows && gwRows[0] ? gwRows[0] : null;
    const existingRt = rtRows && rtRows[0] ? rtRows[0] : null;
    if (existingGw && existingRt) {
      return res.json({
        success: true,
        reused: true,
        data: {
          tc_termination_gateway_id: String(existingGw.tc_termination_gateway_id),
          tc_termination_route_id: String(existingRt.tc_termination_route_id),
          host: existingGw.host || null,
          name: existingRt.name || null
        }
      });
    }

    // SIP user management via external billing system has been removed.
    // Outbound trunking requires manual SIP configuration.
    const sipHost = process.env.SIP_DOMAIN || '';
    if (!sipHost) return res.status(500).json({ success: false, message: 'SIP_DOMAIN not configured' });

    // Use sipId as the SIP username directly
    const sipUsername = sipId;
    const sipPassword = String(req.body?.sip_password || '').trim();
    if (!sipPassword) return res.status(400).json({ success: false, message: 'sip_password is required' });

    const gwName = `Phone.System-${sipUsername}`.slice(0, 255);
    // telecom.center expects `authorization_name`/`authorization_password`.
    const gatewayPayload = {
      data: {
        type: 'termination_gateways',
        attributes: {
          name: gwName,
          host: sipHost,
          port: 5060,
          authorization_name: sipUsername,
          authorization_password: sipPassword
        },
        relationships: {
          customer: { data: { type: 'customers', id: tcCustomerId } }
        }
      }
    };

    const gw = await telecomCenterApiCall({ method: 'POST', path: '/termination_gateways', body: gatewayPayload });
    const tcGatewayId = tcId(gw?.data?.id);
    if (!tcGatewayId) return res.status(502).json({ success: false, message: 'telecom.center termination gateway create returned no id' });

    await pool.execute(
      'INSERT INTO phonesystem_termination_gateways (user_id, tc_customer_id, tc_termination_gateway_id, host) VALUES (?, ?, ?, ?)',
      [userId, tcCustomerId, tcGatewayId, sipHost]
    );

    const routeName = `Default route (${gwName})`.slice(0, 255);
    const routePayload = {
      data: {
        type: 'termination_routes',
        attributes: {
          name: routeName
        },
        relationships: {
          customer: { data: { type: 'customers', id: tcCustomerId } },
          gateway: { data: { type: 'termination_gateways', id: tcGatewayId } }
        }
      }
    };

    const rt = await telecomCenterApiCall({ method: 'POST', path: '/termination_routes', body: routePayload });
    const tcRouteId = tcId(rt?.data?.id);
    if (!tcRouteId) return res.status(502).json({ success: false, message: 'telecom.center termination route create returned no id' });

    await pool.execute(
      'INSERT INTO phonesystem_termination_routes (user_id, tc_customer_id, tc_termination_route_id, name) VALUES (?, ?, ?, ?)',
      [userId, tcCustomerId, tcRouteId, routeName]
    );

    return res.json({
      success: true,
      data: {
        tc_termination_gateway_id: tcGatewayId,
        tc_termination_route_id: tcRouteId,
        host: sipHost,
        name: routeName
      }
    });
  } catch (e) {
    const msg = e?.message || 'Failed to configure outbound trunk';
    const status = e?.status && Number.isFinite(Number(e.status)) ? Number(e.status) : 500;
    if (DEBUG) console.error('[phonesystem.outbound] error:', { status, msg, data: e?.data });
    return res.status(status >= 400 && status < 600 ? status : 500).json({ success: false, message: msg });
  }
});

app.post('/api/me/phonesystem/customers/:customerId/session', requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ success: false, message: 'Database not configured' });

    const userId = req.session.userId;
    const tcCustomerId = tcId(req.params.customerId);
    if (!tcCustomerId) return res.status(400).json({ success: false, message: 'Missing customerId' });

    const customer = await getPhoneSystemCustomerMapping({ userId, tcCustomerId });
    if (!customer) return res.status(404).json({ success: false, message: 'Phone.System customer not found' });

    const payload = {
      data: {
        type: 'sessions',
        relationships: {
          customer: { data: { type: 'customers', id: tcCustomerId } }
        }
      }
    };

    const tc = await telecomCenterApiCall({ method: 'POST', path: '/sessions', body: payload });
    const uri = String(tc?.data?.attributes?.uri || '').trim();
    if (!uri) return res.status(502).json({ success: false, message: 'telecom.center session create returned no uri' });

    return res.json({ success: true, data: { uri } });
  } catch (e) {
    const msg = e?.message || 'Failed to create phone.systems session';
    const status = e?.status && Number.isFinite(Number(e.status)) ? Number(e.status) : 500;
    if (DEBUG) console.error('[phonesystem.session] error:', { status, msg, data: e?.data });
    return res.status(status >= 400 && status < 600 ? status : 500).json({ success: false, message: msg });
  }
});

// Email verification: start
app.post('/api/verify-email', otpSendLimiter, async (req, res) => {
  try {
    const { username, password, firstname, lastname, email, phone } = req.body || {};
    const address1 = req.body?.address1;
    const city = req.body?.city;
    const state = req.body?.state;
    const postalRaw = req.body?.postal_code ?? req.body?.postalCode ?? req.body?.zip ?? req.body?.zipcode;
    const countryRaw = req.body?.country;

    if (!username || !password || !firstname || !lastname || !email) {
      return res.status(400).json({ success: false, message: 'All fields are required: username, password, firstname, lastname, email' });
    }
    if (!phone || !/^\d{11}$/.test(String(phone))) {
      return res.status(400).json({ success: false, message: 'Phone number is required and must be 11 digits (numbers only).' });
    }

    // New required signup fields
    if (!address1 || !city || !state || !postalRaw || !countryRaw) {
      return res.status(400).json({ success: false, message: 'Address fields are required: address1, city, state, postal_code, country' });
    }

    const country = normalizeSignupCountry(countryRaw);
    if (!country) {
      return res.status(400).json({ success: false, message: 'Country must be US or CA' });
    }

    const postalCode = normalizePostalCode(postalRaw, { country });
    if (!postalCode) {
      return res.status(400).json({ success: false, message: 'Postal code is required' });
    }

    const address1Trim = String(address1 || '').trim();
    const cityTrim = String(city || '').trim();
    const stateTrim = String(state || '').trim();

    if (!address1Trim || address1Trim.length > 255) {
      return res.status(400).json({ success: false, message: 'Street address is required (max 255 characters).' });
    }
    if (!cityTrim || cityTrim.length > 100) {
      return res.status(400).json({ success: false, message: 'City is required (max 100 characters).' });
    }
    if (!stateTrim || stateTrim.length > 100) {
      return res.status(400).json({ success: false, message: 'State is required (max 100 characters).' });
    }
    if (postalCode.length > 32) {
      return res.status(400).json({ success: false, message: 'Postal code is too long.' });
    }

    if (!usernameIsAlnumMax10(username)) {
      return res.status(400).json({ success: false, message: 'Username must be letters and numbers only, maximum 10 characters.' });
    }
    if (!passwordIsAlnumMax10(password)) {
      return res.status(400).json({ success: false, message: 'Password must be letters and numbers only, maximum 10 characters.' });
    }

    const availability = await checkAvailability({ username, email });
    if (!availability.usernameAvailable || !availability.emailAvailable) {
      return res.status(409).json({ success: false, message: 'Username or email already exists', availability });
    }

    const token = crypto.randomBytes(16).toString('hex');
    const code = otp();
    const record = {
      codeHash: sha256(code),
      email,
      expiresAt: Date.now() + OTP_TTL_MS,
      fields: {
        username,
        password,
        firstname,
        lastname,
        email,
        phone,
        address1: address1Trim,
        city: cityTrim,
        state: stateTrim,
        postal_code: postalCode,
        country,
        signup_ip: getClientIp(req)
      }
    };

    await storeVerification({ token, codeHash: record.codeHash, email, expiresAt: record.expiresAt, fields: record.fields });
    await sendVerificationEmail(email, code);
    if (DEBUG) console.debug('Verification code sent', { email, token });
    return res.status(200).json({ success: true, token });
  } catch (err) {
    console.error('verify-email error', err.response?.data || err.message || err);
    return res.status(500).json({ success: false, message: 'Failed to send verification code' });
  }
});

// Email verification: complete -> create account
app.post('/api/complete-signup', otpVerifyLimiter, async (req, res) => {
  try {
    const { token, code, email: reqEmail } = req.body || {};
    if (DEBUG) console.log('[complete-signup] Request received:', { token, hasCode: !!code, hasEmail: !!reqEmail });
    if (!code) { return res.status(400).json({ success: false, message: 'code is required' }); }
    // Support legacy flow (email+code) and token+code. Prefer token when provided.
    let rec = null;
    if (token) {
      rec = await fetchVerification(token);
      if (!rec) { if (DEBUG) console.log('[complete-signup] No record found for token:', token); return res.status(400).json({ success: false, message: 'Invalid or expired token' }); }
    } else if (reqEmail) {
      rec = await fetchVerificationLatestByEmail(reqEmail);
      if (!rec) { if (DEBUG) console.log('[complete-signup] No active record for email:', reqEmail); return res.status(400).json({ success: false, message: 'Invalid or expired code' }); }
    } else {
      return res.status(400).json({ success: false, message: 'email or token is required with code' });
    }
    // Ensure we have token for attempts/used updates
    const activeToken = rec.token;
    if (rec.used) { if (DEBUG) console.log('[complete-signup] Code already used:', { token, email: rec.email }); return res.status(400).json({ success: false, message: 'This code was already used' }); }
    if (Date.now() > Number(rec.expires_at)) { if (DEBUG) console.log('[complete-signup] Code expired:', { token: activeToken, email: rec.email, expiresAt: rec.expires_at, now: Date.now() }); await markVerificationUsed(activeToken); return res.status(400).json({ success: false, message: 'Code expired, please request a new one' }); }
    const attempts = Number(rec.attempts || 0);
    if (attempts >= OTP_MAX_ATTEMPTS) {
      if (DEBUG) console.log('[complete-signup] Too many OTP attempts for token:', { token: activeToken, email: rec.email, attempts });
      await markVerificationUsed(activeToken);
      return res.status(400).json({ success: false, message: 'Too many invalid attempts. Please request a new verification code.' });
    }
    if (sha256(code) !== rec.code_hash) {
      if (DEBUG) console.log('[complete-signup] Code mismatch:', { token: activeToken, email: rec.email, attempts: attempts + 1 });
      try {
        await pool.execute('UPDATE email_verifications SET attempts = attempts + 1 WHERE token = ?', [activeToken]);
      } catch (e) {
        if (DEBUG) console.warn('[complete-signup] Failed to increment OTP attempts:', e.message || e);
      }
      if (attempts + 1 >= OTP_MAX_ATTEMPTS) {
        try { await markVerificationUsed(activeToken); } catch {}
        return res.status(400).json({ success: false, message: 'Too many invalid attempts. Please request a new verification code.' });
      }
      return res.status(400).json({ success: false, message: 'Invalid code' });
    }
    if (DEBUG) console.log('[complete-signup] Code validated successfully:', { token: activeToken, email: rec.email });
    // Don't mark as used yet - only after successful account creation

  // proceed to create account using the same logic as /api/signup
  const { username, firstname, lastname, email, phone } = rec;
  const address1 = rec?.address1 != null ? String(rec.address1).trim() : '';
  const city = rec?.city != null ? String(rec.city).trim() : '';
  const state = rec?.state != null ? String(rec.state).trim() : '';
  const country = rec?.country != null ? String(rec.country).trim().toUpperCase() : '';
  const postalCode = rec?.postal_code != null ? String(rec.postal_code).trim() : '';
  const signupIp = (rec?.signup_ip != null ? String(rec.signup_ip).trim() : '') || getClientIp(req) || null;

  if (DEBUG) console.log('[complete-signup] Creating account for:', { username, email });
  if (!usernameIsAlnumMax10(username)) {
    if (DEBUG) console.log('[complete-signup] Username validation failed');
    return res.status(400).json({ success: false, message: 'Username must be letters and numbers only, maximum 10 characters.' });
  }
  // Using stored plaintext password (legacy behavior)
  let plainPassword = String(rec.password || '');
  const looksLikeBcrypt = plainPassword.startsWith('$2a$') || plainPassword.startsWith('$2b$') || plainPassword.startsWith('$2y$');
  if (looksLikeBcrypt || !plainPassword) {
    if (DEBUG) console.log('[complete-signup] Stored password appears to be a hash/empty; cannot proceed');
    return res.status(400).json({ success: false, message: 'Stored verification record is invalid. Please restart signup.' });
  }
  if (!passwordIsAlnumMax10(plainPassword)) {
    if (DEBUG) console.log('[complete-signup] Password validation failed');
    return res.status(400).json({ success: false, message: 'Password must be letters and numbers only, maximum 10 characters.' });
  }

  // Check if user already exists in local database
  const exists = await userExists(username, email);
  if (exists) {
    if (DEBUG) console.log('[complete-signup] Username or email already taken');
    return res.status(409).json({ success: false, message: 'Username or email already exists' });
  }

  // Hash password
  let passwordHash;
  try {
    passwordHash = await bcrypt.hash(plainPassword, 12);
  } catch (e) {
    console.error('[complete-signup] Failed to hash password:', e.message);
    return res.status(500).json({ success: false, message: 'Account creation failed' });
  }

  // Create user in local database
  const userData = {
    username,
    email,
    passwordHash,
    name: `${firstname} ${lastname}`,
    phone: phone || null,
    balance: 0.00,
    signupIp,
    address1: address1 || null,
    city: city || null,
    state: state || null,
    postalCode: postalCode || null,
    country: country || null
  };

  const result = await createUser(userData);
  if (!result.success) {
    console.error('[complete-signup] Failed to create user:', result.error);
    return res.status(500).json({ success: false, message: 'Failed to create account' });
  }

  const userId = result.userId;
  if (DEBUG) console.log('[complete-signup] User created in local database:', { userId, username });

  // Mark verification as used now that account creation succeeded
  try { await markVerificationUsed(activeToken); } catch (e) { if (DEBUG) console.warn('Failed to mark verification as used:', e.message); }
  try { await purgeExpired(); } catch {}

  // Send welcome email
  try {
    await sendWelcomeEmail(email, username);
  } catch (e) {
    if (DEBUG) console.warn('Welcome email failed', e.message || e);
  }

  // Send admin notification
  try {
    await sendAdminSignupCopyEmail({
      email,
      username,
      firstname,
      lastname,
      phone,
      address1,
      city,
      state,
      postalCode,
      country,
      signupIp,
    });
  } catch (e) {
    if (DEBUG) console.warn('Admin signup copy email failed', e.message || e);
  }

  if (DEBUG) console.log('[complete-signup] Signup complete! Returning success.');
  return res.status(200).json({ success: true, message: 'Account created successfully!', data: { username, userId } });

  } catch (error) {
    // Log error
    console.error('[complete-signup] Error:', error.message || error);
    if (DEBUG) console.error('[complete-signup] Stack:', error.stack);
    
    if (error.message === 'Database not configured') {
      return res.status(500).json({ success: false, message: 'Database connection error. Please try again later.' });
    }
    
    return res.status(500).json({ success: false, message: 'Failed to create account' });
  }
});

// Username/email availability endpoint
app.get('/api/check-availability', async (req, res) => {
  try {
    const { username, email } = req.query;
    const availability = await checkAvailability({ username, email });
    res.json({ success: true, ...availability });
  } catch (e) {
    res.status(500).json({ success: false, message: 'Error checking availability' });
  }
});




// Signup API endpoint (legacy direct)
app.post('/api/signup', async (req, res) => {
  try {
    const { username, password, firstname, lastname, email, phone } = req.body || {};
    const address1 = req.body?.address1;
    const city = req.body?.city;
    const state = req.body?.state;
    const postalRaw = req.body?.postal_code ?? req.body?.postalCode ?? req.body?.zip ?? req.body?.zipcode;
    const countryRaw = req.body?.country;
    const country = normalizeSignupCountry(countryRaw);
    const postalCode = normalizePostalCode(postalRaw, { country: country || undefined });
    const signupIp = getClientIp(req);

// Validate required fields
    if (!username || !password || !firstname || !lastname || !email) {
      return res.status(400).json({
        success: false,
        message: 'All fields are required: username, password, firstname, lastname, email'
      });
    }
    if (!usernameIsAlnumMax10(username)) {
      return res.status(400).json({ success: false, message: 'Username must be letters and numbers only, maximum 10 characters.' });
    }
    if (!passwordIsAlnumMax10(password)) {
      return res.status(400).json({ success: false, message: 'Password must be letters and numbers only, maximum 10 characters.' });
    }

    // Create user locally (no external billing system)
    let passwordHash = null;
    try { passwordHash = await bcrypt.hash(password, 12); } catch {}

    try {
      await saveUserRow({
        username,
        email,
        firstname,
        lastname,
        phone,
        address1: address1 != null ? String(address1).trim() : null,
        city: city != null ? String(city).trim() : null,
        state: state != null ? String(state).trim() : null,
        postalCode: postalCode || null,
        country: country || null,
        signupIp,
        passwordHash
      });
    } catch (e) {
      if (DEBUG) console.warn('Save user failed', e.message || e);
      return res.status(500).json({ success: false, message: 'Failed to create account' });
    }

    // Fire-and-forget welcome email
    try { await sendWelcomeEmail(email, username); } catch (e) { if (DEBUG) console.warn('Welcome email failed', e.message || e); }
    try {
      await sendAdminSignupCopyEmail({
        email,
        username,
        firstname,
        lastname,
        phone,
        address1: address1 != null ? String(address1).trim() : null,
        city: city != null ? String(city).trim() : null,
        state: state != null ? String(state).trim() : null,
        postalCode: postalCode || null,
        country: country || null,
        signupIp
      });
    } catch (e) {
      if (DEBUG) console.warn('Admin signup copy email failed', e.message || e);
    }

    return res.status(200).json({
      success: true,
      message: 'Account created successfully!',
      data: {
        username
      }
    });
  } catch (error) {
    console.error('Signup error', error.message || error);
    return res.status(500).json({ success: false, message: 'Failed to create account' });
  }
});

// Lightweight periodic billing runner (runs independently of HTTP requests)
// To keep things safe, this uses the same DB-level idempotency as the
// DID markup billing route, so running both is harmless.
async function runMarkupBillingTick() {
  if (!pool) return;
  try {
    const [users] = await pool.execute('SELECT id FROM signup_users');
    if (!users || users.length === 0) {
      if (DEBUG) console.log('[billing.markup] No users; skipping tick');
      return;
    }

    const userIdSet = new Set(users.map(u => String(u.id)));

    // AI numbers: build lookup per user
    // Skip cancel_pending numbers so we don't keep billing a number that is being cancelled.
    const aiNumbersByUser = new Map(); // localUserId -> [{id, daily_number_id, phone_number, dialin_config_id, created_at}]
    try {
      const [aiRows] = await pool.execute(
        'SELECT id, user_id, daily_number_id, phone_number, dialin_config_id, created_at FROM ai_numbers WHERE cancel_pending = 0'
      );
      for (const r of aiRows || []) {
        const uid = String(r.user_id);
        if (!userIdSet.has(uid)) continue;
        if (!aiNumbersByUser.has(uid)) aiNumbersByUser.set(uid, []);
        aiNumbersByUser.get(uid).push(r);
      }
    } catch (e) {
      if (DEBUG) console.warn('[billing.ai] Failed to load ai_numbers:', e.message || e);
    }

    for (const u of users) {
      const localUserId = String(u.id);

      // Process any pending AI number cancellations.
      if (AI_MONTHLY_CANCEL_ON_INSUFFICIENT_BALANCE) {
        try {
          await processAiCancelPendingForUser({ localUserId });
        } catch (e) {
          if (DEBUG) console.warn('[ai.nonpayment] cancel_pending tick failed:', e?.message || e);
        }
      }

      // AI monthly number fees
      const userAiNumbers = aiNumbersByUser.get(localUserId) || [];
      if (userAiNumbers.length) {
        try {
          await billAiNumberMonthlyFeesForUser({ localUserId, aiNumbers: userAiNumbers });
        } catch (e) {
          if (DEBUG) console.warn('[billing.ai.monthly] Failed:', e?.message || e);
        }
      }

      // Bill any completed inbound AI calls that are still unbilled (idempotent).
      try {
        await billUnchargedAiInboundCallsForUser({ localUserId, limit: 200 });
      } catch (e) {
        if (DEBUG) console.warn('[billing.ai.inbound] Failed:', e?.message || e);
      }

      // If enabled, keep AI number routing synced with balance (disable when credit < threshold, re-enable when sufficient)
      if (userAiNumbers.length) {
        try {
          await syncAiDialinRoutingForUser({ localUserId });
        } catch (e) {
          if (DEBUG) console.warn('[ai.dialin.sync] Failed during billing tick:', e?.message || e);
        }
      }
    }
  } catch (e) {
    if (DEBUG) console.warn('[billing.markup] Error during scheduler tick:', e.message || e);
  }
}

// Start a simple interval-based scheduler.
// Controlled by BILLING_MARKUP_INTERVAL_MINUTES; if unset or 0, scheduler is disabled.
function startBillingScheduler() {
  const intervalMs = (parseInt(process.env.BILLING_MARKUP_INTERVAL_MINUTES || '0', 10) || 0) * 60 * 1000;
    if (!intervalMs) {
      if (DEBUG) console.log('[billing.scheduler] Disabled (no interval set)');
      return;
    }
    if (DEBUG) console.log('[billing.scheduler] Enabled with interval (ms):', intervalMs);
    setInterval(() => {
      runMarkupBillingTick();
    }, intervalMs);
}

// Start server after DB + session store are initialized
async function startServer() {
  try {
    await initDb().catch(err => console.error('DB init error', err));

    if (sessionStore) {
      sessionConfig.store = sessionStore;
      sessionMiddleware = session(sessionConfig);
      console.log('MySQL session store initialized');
    } else {
      sessionMiddleware = session(sessionConfig);
      console.warn('MySQL session store not available; using in-memory sessions');
    }

    app.listen(PORT, '0.0.0.0', () => {
      console.log(`Phone.System Signup Server running on port ${PORT}`);
      console.log(`Environment: ${process.env.NODE_ENV}`);
      if (DEBUG) console.log('Debug logging enabled');
      startBillingScheduler();
      startDialerWorker();
      // Auto-register Daily events webhook (non-blocking; failures logged but don't prevent startup).
      ensureDailyEventsWebhook().catch(e => console.warn('[startup] Daily webhook setup failed:', e?.message || e));
    });
  } catch (e) {
    console.error('Fatal startup error', e.message || e);
    process.exit(1);
  }
}

startServer();
