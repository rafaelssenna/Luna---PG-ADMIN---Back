// server.js
// Servidor Express para a aplicação Luna

require('dotenv').config();

const express = require('express');
const { Pool } = require('pg');
const EventEmitter = require('events');
const multer = require('multer');
const path = require('path');

const app = express();

/* ======================  Janela e cota diária  ====================== */
// Até 30 mensagens por dia, janela 08:00–17:30
const DAILY_MESSAGE_COUNT = 30;
const DAILY_START_TIME = '08:00:00';
const DAILY_END_TIME = '17:30:00';

/* ======================  Utils de tempo  ====================== */
function hmsToSeconds(hms) {
  const parts = String(hms || '').split(':').map((p) => parseInt(p, 10) || 0);
  const h = parts[0] || 0;
  const m = parts[1] || 0;
  const s = parts[2] || 0;
  return h * 3600 + m * 60 + s;
}

/**
 * Gera delays (em segundos) para envios entre start e end, a partir de "agora".
 * O primeiro delay inclui o tempo até o início efetivo (se necessário).
 */
function generateScheduleDelays(count, startStr, endStr) {
  const now = new Date();
  const nowSec = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();
  const startSec = hmsToSeconds(startStr);
  const endSec = hmsToSeconds(endStr);

  const effectiveStart = Math.max(nowSec, startSec);
  if (endSec <= effectiveStart) return [];
  const span = endSec - effectiveStart;
  const msgCount = Math.min(count, span);

  const offsets = new Set();
  while (offsets.size < msgCount) offsets.add(Math.floor(Math.random() * (span + 1)));
  const sortedOffsets = Array.from(offsets).sort((a, b) => a - b);

  const delays = [];
  let prev = 0;
  for (let i = 0; i < sortedOffsets.length; i++) {
    const off = sortedOffsets[i];
    if (i === 0) delays.push((effectiveStart - nowSec) + off);
    else delays.push(off - prev);
    prev = off;
  }
  return delays;
}

/* ======================  CORS  ====================== */
const CORS_ANY = process.env.CORS_ANY === 'true';
const CORS_ORIGINS = (process.env.CORS_ORIGINS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (CORS_ANY) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  } else if (CORS_ORIGINS.length > 0) {
    if (origin && CORS_ORIGINS.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
    } else {
      res.setHeader('Access-Control-Allow-Origin', '*');
    }
  } else {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
  res.setHeader('Vary', 'Origin, Access-Control-Request-Headers');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.setHeader(
    'Access-Control-Allow-Headers',
    req.headers['access-control-request-headers'] || 'Content-Type, Authorization, token'
  );
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

/* ======================  Banco de Dados  ====================== */
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
});

app.use(express.json());
app.use(express.static(path.join(__dirname)));

/* ======================  Compat: endpoints sem /api  ====================== */
const COMPAT_ENDPOINTS = new Set([
  'clients',
  'client-settings',
  'stats',
  'queue',
  'totals',
  'contacts',
  'import',
  'progress',
  'loop',
  'delete-client',
  'healthz',
  'quota',
  /* >>> ADIÇÕES */
  'leads',
  'loop-state',
  'sent-today',           // <— compat para /api/sent-today
]);

app.use((req, _res, next) => {
  const seg = (req.path || '').replace(/^\/+/, '').split('/')[0];
  if (seg && COMPAT_ENDPOINTS.has(seg) && !req.path.startsWith('/api/')) {
    req.url = '/api' + (req.url.startsWith('/') ? req.url : '/' + req.url);
  }
  next();
});

const upload = multer({ storage: multer.memoryStorage() });

function validateSlug(slug) {
  // Permite slugs com ou sem o prefixo "cliente_"
  return /^cliente_[a-z0-9_]+$/.test(slug) || /^[a-z0-9_]+$/.test(slug);
}

/* ======================  Estado e SSE por cliente  ====================== */
const runningClients = new Set();
const progressEmitters = new Map();
function getEmitter(slug) {
  if (!progressEmitters.has(slug)) progressEmitters.set(slug, new EventEmitter());
  return progressEmitters.get(slug);
}
const progressStates = new Map();
function snapshotStart(slug, total) {
  progressStates.set(slug, {
    lastStart: { type: 'start', total, at: new Date().toISOString() },
    items: [],
    lastEnd: null,
  });
}
function snapshotPush(slug, evt) {
  const st = progressStates.get(slug);
  if (!st) return;
  st.items.push(evt);
  if (st.items.length > 200) st.items.shift();
}
function snapshotEnd(slug, processed, extra = {}) {
  const st = progressStates.get(slug);
  if (!st) return;
  st.lastEnd = { type: 'end', processed, ...extra, at: new Date().toISOString() };
}

/* ======================  Tabela de settings por cliente  ====================== */
async function ensureSettingsTable() {
  await pool.query(`
CREATE TABLE IF NOT EXISTS client_settings (
  slug TEXT PRIMARY KEY,
  auto_run BOOLEAN DEFAULT false,
  ia_auto BOOLEAN DEFAULT false,
  instance_url TEXT,
  instance_token TEXT,
  instance_auth_header TEXT,
  instance_auth_scheme TEXT,
  loop_status TEXT DEFAULT 'idle',
  last_run_at TIMESTAMPTZ
);
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS instance_token TEXT;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS instance_auth_header TEXT;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS instance_auth_scheme TEXT;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS loop_status TEXT DEFAULT 'idle';
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS last_run_at TIMESTAMPTZ;
`);
}
ensureSettingsTable().catch((e) => console.error('ensureSettingsTable', e));

async function getClientSettings(slug) {
  const { rows } = await pool.query(
    `SELECT auto_run, ia_auto, instance_url, loop_status, last_run_at,
            instance_token, instance_auth_header, instance_auth_scheme
       FROM client_settings
      WHERE slug = $1`,
    [slug]
  );
  if (!rows.length) {
    return {
      auto_run: false,
      ia_auto: false,
      instance_url: null,
      instance_token: null,
      instance_auth_header: null,
      instance_auth_scheme: null,
      loop_status: 'idle',
      last_run_at: null,
    };
  }
  return rows[0];
}

async function saveClientSettings(
  slug,
  { autoRun, iaAuto, instanceUrl, instanceToken, instanceAuthHeader, instanceAuthScheme }
) {
  await pool.query(
    `INSERT INTO client_settings
       (slug, auto_run, ia_auto, instance_url, instance_token, instance_auth_header, instance_auth_scheme)
     VALUES ($1,   $2,       $3,     $4,           $5,             $6,                   $7)
     ON CONFLICT (slug)
     DO UPDATE SET
       auto_run = EXCLUDED.auto_run,
       ia_auto = EXCLUDED.ia_auto,
       instance_url = EXCLUDED.instance_url,
       instance_token = EXCLUDED.instance_token,
       instance_auth_header = EXCLUDED.instance_auth_header,
       instance_auth_scheme = EXCLUDED.instance_auth_scheme`,
    [
      slug,
      !!autoRun,
      !!iaAuto,
      instanceUrl || null,
      instanceToken || null,
      instanceAuthHeader || 'token',
      instanceAuthScheme ?? '',
    ]
  );
}

/* ======================  IA (UAZAPI) ====================== */
function normalizePhoneE164BR(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (!digits) return '';
  if (digits.startsWith('55')) return `+${digits}`;
  if (digits.length === 11) return `+55${digits}`;
  return `+${digits}`;
}

function fillTemplate(tpl, vars) {
  return String(tpl || '').replace(/\{(NAME|CLIENT|PHONE)\}/gi, (_, k) => {
    const key = k.toUpperCase();
    return vars[key] ?? '';
  });
}

const UAZ = {
  token: process.env.UAZAPI_TOKEN || '',
  authHeader: process.env.UAZAPI_AUTH_HEADER || 'token',
  authScheme: process.env.UAZAPI_AUTH_SCHEME ?? '',
  phoneField: process.env.UAZAPI_PHONE_FIELD || 'phone',
  textField: process.env.UAZAPI_TEXT_FIELD || 'message',
  digitsOnly: (process.env.UAZAPI_PHONE_DIGITS_ONLY || 'true') === 'true',
  payloadStyle: (process.env.UAZAPI_PAYLOAD_STYLE || 'json').toLowerCase(),
  methodPref: (process.env.UAZAPI_METHOD || 'post').toLowerCase(),
  extra: (() => {
    try { return JSON.parse(process.env.UAZAPI_EXTRA || '{}'); }
    catch { return {}; }
  })(),
  template: process.env.MESSAGE_TEMPLATE || 'Olá {NAME}, aqui é do {CLIENT}.',
};

function buildUazRequest(instanceUrl, { e164, digits, text }) {
  const hasTpl = /\{(NUMBER|PHONE_E164|TEXT)\}/.test(instanceUrl);
  const hasQueryParams = /\?[^#]*=/.test(instanceUrl);
  const style = UAZ.payloadStyle;
  const methodEnv = UAZ.methodPref;

  const decideMethod = () => {
    if (methodEnv === 'get') return 'GET';
    if (methodEnv === 'post') return 'POST';
    return (hasTpl || hasQueryParams) ? 'GET' : 'POST';
  };

  const method = decideMethod();
  const phoneValue = UAZ.digitsOnly ? digits : e164;

  const makeJson = () => {
    const payload = { ...UAZ.extra };
    payload[UAZ.phoneField] = phoneValue;
    payload[UAZ.textField] = text;
    return {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    };
  };

  if (style === 'template' || hasTpl) {
    if (method === 'GET') {
      const url = instanceUrl
        .replace(/\{NUMBER\}/g, digits)
        .replace(/\{PHONE_E164\}/g, encodeURIComponent(e164))
        .replace(/\{TEXT\}/g, encodeURIComponent(text));
      return { url, method: 'GET' };
    }
    let cleanUrl;
    try { const u = new URL(instanceUrl); cleanUrl = u.origin + u.pathname; }
    catch { cleanUrl = instanceUrl.split('?')[0]; }
    const j = makeJson();
    return { url: cleanUrl, method: 'POST', headers: j.headers, body: j.body };
  }

  if (style === 'query' || (hasQueryParams && style === 'auto')) {
    const u = new URL(instanceUrl);
    if (method === 'GET') {
      u.searchParams.set(UAZ.phoneField, phoneValue);
      u.searchParams.set(UAZ.textField, text);
      Object.entries(UAZ.extra || {}).forEach(([k, v]) => {
        if (['string', 'number', 'boolean'].includes(typeof v)) u.searchParams.set(k, String(v));
      });
      return { url: u.toString(), method: 'GET' };
    }
    const cleanUrl = u.origin + u.pathname;
    const j = makeJson();
    return { url: cleanUrl, method: 'POST', headers: j.headers, body: j.body };
  }

  if (style === 'form') {
    const form = new URLSearchParams();
    Object.entries(UAZ.extra || {}).forEach(([k, v]) =>
      form.set(k, typeof v === 'object' ? JSON.stringify(v) : String(v))
    );
    form.set(UAZ.phoneField, phoneValue);
    form.set(UAZ.textField, text);
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
    return { url: instanceUrl, method: 'POST', headers, body: form.toString() };
  }

  const j = makeJson();
  return { url: instanceUrl, method: 'POST', headers: j.headers, body: j.body };
}

async function httpSend({ url, method, headers, body }) {
  if (typeof fetch === 'function') {
    return fetch(url, { method, headers, body });
  }
  try {
    const nf = require('node-fetch');
    if (nf) return nf(url, { method, headers, body });
  } catch {}
  return new Promise((resolve, reject) => {
    try {
      const URLmod = new URL(url);
      const httpMod = URLmod.protocol === 'https:' ? require('https') : require('http');
      const req = httpMod.request(
        {
          hostname: URLmod.hostname,
          port: URLmod.port || (URLmod.protocol === 'https:' ? 443 : 80),
          path: URLmod.pathname + URLmod.search,
          method: method || 'GET',
          headers: headers || {},
        },
        (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => {
            resolve({
              ok: res.statusCode >= 200 && res.statusCode < 300,
              status: res.statusCode,
              json: async () => { try { return JSON.parse(data); } catch { return { raw: data }; } },
              text: async () => data,
            });
          });
        }
      );
      req.on('error', reject);
      if (body) req.write(body);
      req.end();
    } catch (err) {
      reject(err);
    }
  });
}

async function runIAForContact({
  client,
  name,
  phone,
  instanceUrl,
  instanceToken,
  instanceAuthHeader,
  instanceAuthScheme,
}) {
  const SHOULD_CALL = process.env.IA_CALL === 'true';
  if (!SHOULD_CALL || !instanceUrl) return { ok: true, simulated: true };

  try {
    const e164 = normalizePhoneE164BR(phone);
    const digits = String(e164).replace(/\D/g, '');
    const text = fillTemplate(UAZ.template, { NAME: name, CLIENT: client, PHONE: e164 });

    const req = buildUazRequest(instanceUrl, { e164, digits, text });

    const hdrName =
      (instanceAuthHeader && instanceAuthHeader.trim()) || UAZ.authHeader || 'token';
    const hdrScheme =
      instanceAuthScheme !== undefined ? instanceAuthScheme : UAZ.authScheme || '';
    const tokenVal = (instanceToken && String(instanceToken)) || UAZ.token || '';
    if (tokenVal) {
      req.headers = req.headers || {};
      req.headers[hdrName] = `${hdrScheme}${tokenVal}`;
    }

    if (process.env.DEBUG === 'true') {
      const maskedHeaders = Object.fromEntries(
        Object.entries(req.headers || {}).map(([k, v]) => [
          k,
          k.toLowerCase().includes('token') || k.toLowerCase().includes('authorization') ? '***' : v,
        ])
      );
      console.log('[UAZAPI] request', {
        url: req.url,
        method: req.method,
        headers: maskedHeaders,
        hasBody: !!req.body,
      });
    }

    const resp = await httpSend(req);
    let body;
    try { body = await resp.json(); } catch { body = await resp.text(); }
    if (!resp.ok) {
      console.error('UAZAPI FAIL', { status: resp.status, body });
    }
    return { ok: resp.ok, status: resp.status, body };
  } catch (err) {
    console.error('UAZAPI ERROR', instanceUrl, err);
    return { ok: false, error: String(err) };
  }
}

/* ======================  Helpers  ====================== */
async function tableExists(tableName) {
  const { rows } = await pool.query('SELECT to_regclass($1) AS reg;', [`public.${tableName}`]);
  return !!rows[0].reg;
}

function norm(s) {
  return (s ?? '')
    .toString()
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}
function detectDelimiter(firstLine) {
  const commas = (firstLine.match(/,/g) || []).length;
  const semis = (firstLine.match(/;/g) || []).length;
  return semis > commas ? ';' : ',';
}
function parseCSV(text, delim) {
  const rows = [];
  let row = [], val = '', inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') { val += '"'; i++; }
      else { inQuotes = !inQuotes; }
      continue;
    }
    if (ch === '\r') continue;
    if (ch === '\n' && !inQuotes) { row.push(val); rows.push(row); row = []; val = ''; continue; }
    if (ch === delim && !inQuotes) { row.push(val); val = ''; continue; }
    val += ch;
  }
  if (val.length > 0 || row.length > 0) { row.push(val); rows.push(row); }
  return rows;
}
function mapHeader(headerCells) {
  const idx = { name: -1, phone: -1, niche: -1 };
  const names = headerCells.map((h) => norm(h));
  const isId = (h) => ['id', 'identificador', 'codigo', 'código'].includes(h);
  const nameKeys = new Set(['nome','name','full_name','fullname','contato','empresa','nomefantasia','razaosocial']);
  const phoneKeys = new Set(['telefone','numero','número','phone','whatsapp','celular','mobile','telemovel']);
  const nicheKeys = new Set(['nicho','niche','segmento','categoria','industry']);
  names.forEach((h, i) => {
    if (isId(h)) return;
    if (idx.name === -1 && nameKeys.has(h)) idx.name = i;
    if (idx.phone === -1 && phoneKeys.has(h)) idx.phone = i;
    if (idx.niche === -1 && nicheKeys.has(h)) idx.niche = i;
  });
  return idx;
}

/* ======================  ADIÇÃO: integração com buscador de leads  ====================== */
const { searchLeads } = require('./leadsSearcher');

async function ensureRegionColumns(slug) {
  try {
    await pool.query(`ALTER TABLE "${slug}" ADD COLUMN IF NOT EXISTS region TEXT;`);
  } catch (e) { console.warn('ensureRegionColumns fila', slug, e?.message); }
  try {
    await pool.query(`ALTER TABLE "${slug}_totais" ADD COLUMN IF NOT EXISTS region TEXT;`);
  } catch (e) { console.warn('ensureRegionColumns totais', slug, e?.message); }
}

/* ======================  Endpoints ====================== */

/** Healthcheck */
app.get('/api/healthz', (_req, res) => res.json({ up: true }));

/* ---------- NOVA ROTA: Estado do loop / cota de hoje ---------- */
app.get('/api/loop-state', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  try {
    const cap = DAILY_MESSAGE_COUNT;

    let sent_today = 0;
    try {
      const r = await pool.query(
        `SELECT COUNT(*)::int AS c
           FROM "${slug}_totais"
          WHERE mensagem_enviada = true
            AND updated_at::date = CURRENT_DATE;`
      );
      sent_today = Number(r.rows?.[0]?.c || 0);
    } catch {}

    let loop_status = 'idle', last_run_at = null;
    try {
      const r2 = await pool.query(
        `SELECT loop_status, last_run_at FROM client_settings WHERE slug = $1;`,
        [slug]
      );
      if (r2.rows[0]) {
        loop_status = r2.rows[0].loop_status || 'idle';
        last_run_at = r2.rows[0].last_run_at || null;
      }
    } catch {}

    const remaining_today = Math.max(0, cap - sent_today);
    res.json({
      cap,
      sent_today,
      remaining_today,
      window_start: DAILY_START_TIME,
      window_end: DAILY_END_TIME,
      loop_status,
      last_run_at,
      now: new Date().toISOString(),
    });
  } catch (err) {
    console.error('Erro em /api/loop-state', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});
/* ---------- FIM: loop-state ---------- */

/* ---------- ROTA DE BUSCA DE LEADS (consulta) ---------- */
app.get('/api/leads/search', async (req, res) => {
  try {
    const region = req.query.region || req.query.local || req.query.city || '';
    const niche  = req.query.niche  || req.query.nicho || req.query.segment || '';
    const limit  = parseInt(req.query.limit || req.query.n || '0', 10) || undefined;

    const items = await searchLeads({ region, niche, limit });
    return res.json({ items, count: items.length });
  } catch (err) {
    console.error('Erro em /api/leads/search', err);
    return res.status(500).json({ error: 'Erro interno ao consultar leads' });
  }
});
/* ---------- FIM: NOVA ROTA DE BUSCA ---------- */

/** NOVA ROTA: Enviados (hoje) — para pré-carregar no feed */
app.get('/api/sent-today', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const table = `${slug}_totais`;
  const exists = await tableExists(table);
  if (!exists) return res.json({ items: [], total: 0 });

  const limit = Math.min(Math.max(parseInt(req.query.limit || '100', 10) || 100, 1), 500);
  const offset = Math.max(parseInt(req.query.offset || '0', 10) || 0, 0);

  try {
    const itemsSql = `
      SELECT name, phone, niche, updated_at
        FROM "${table}"
       WHERE mensagem_enviada = true
         AND updated_at::date = CURRENT_DATE
       ORDER BY updated_at DESC
       LIMIT $1 OFFSET $2;`;
    const itemsRes = await pool.query(itemsSql, [limit, offset]);
    const items = itemsRes.rows;

    const countSql = `
      SELECT COUNT(*)::int AS total
        FROM "${table}"
       WHERE mensagem_enviada = true
         AND updated_at::date = CURRENT_DATE;`;
    const countRes = await pool.query(countSql);
    const total = Number(countRes.rows?.[0]?.total || 0);

    res.json({ items, total });
  } catch (err) {
    console.error('Erro em /api/sent-today', err);
    res.status(500).json({ error: 'Erro interno ao consultar enviados de hoje' });
  }
});

/** Lista clientes (slug e fila) + flags salvas */
app.get('/api/clients', async (_req, res) => {
  try {
    // Ao listar clientes, considere qualquer tabela "base" que possua uma
    // tabela correspondente "_totais". Isso permite slugs sem o prefixo "cliente_".
    const result = await pool.query(
      `SELECT t.table_name AS slug
         FROM information_schema.tables t
        WHERE t.table_schema = 'public'
          AND t.table_type   = 'BASE TABLE'
          AND t.table_name NOT LIKE '%\\_totais'
          AND EXISTS (
                SELECT 1
                  FROM information_schema.tables t2
                 WHERE t2.table_schema = 'public'
                   AND t2.table_name   = t.table_name || '_totais'
          )
        ORDER BY t.table_name;`
    );
    const tables = result.rows.map((r) => r.slug);
    const clients = [];
    for (const slug of tables) {
      try {
        const [countRes, cfgRes] = await Promise.all([
          pool.query(`SELECT COUNT(*) AS count FROM "${slug}";`),
          pool.query(
            `SELECT auto_run, ia_auto, instance_url, loop_status, last_run_at
               FROM client_settings WHERE slug = $1;`,
            [slug]
          ),
        ]);
        const queueCount = Number(countRes.rows[0].count);
        const autoRun = !!cfgRes.rows[0]?.auto_run;
        const iaAuto = !!cfgRes.rows[0]?.ia_auto;
        const instanceUrl = cfgRes.rows[0]?.instance_url || null;
        const loopStatus = cfgRes.rows[0]?.loop_status || 'idle';
        const lastRunAt = cfgRes.rows[0]?.last_run_at || null;
        clients.push({ slug, queueCount, autoRun, iaAuto, instanceUrl, loopStatus, lastRunAt });
      } catch (innerErr) {
        console.error('Erro ao contar fila para', slug, innerErr);
        clients.push({ slug });
      }
    }
    res.json(clients);
  } catch (err) {
    console.error('Erro ao listar clientes', err);
    res.status(500).json({ error: 'Erro interno ao listar clientes' });
  }
});

/** Cria novo cliente */
app.post('/api/clients', async (req, res) => {
  const { slug } = req.body;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Slug inválido' });
  try {
    await pool.query('SELECT create_full_client_structure($1);', [slug]);
    res.status(201).json({ message: 'Cliente criado com sucesso' });
  } catch (err) {
    console.error('Erro ao criar cliente', err);
    res.status(500).json({ error: 'Erro interno ao criar cliente' });
  }
});

/** KPIs (inclui info do último envio) */
app.get('/api/stats', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const filaTable = `${slug}`;
  const totaisTable = `${slug}_totais`;

  try {
    const [hasFila, hasTotais] = await Promise.all([tableExists(filaTable), tableExists(totaisTable)]);
    let totais = 0, enviados = 0, fila = 0, lastSentAt = null, lastSentName = null, lastSentPhone = null;

    if (hasTotais) {
      const r = await pool.query(
        `SELECT
           (SELECT COUNT(*) FROM "${totaisTable}") AS totais,
           (SELECT COUNT(*) FROM "${totaisTable}" WHERE mensagem_enviada = true) AS enviados;`
      );
      totais = Number(r.rows[0].totais);
      enviados = Number(r.rows[0].enviados);

      const r3 = await pool.query(
        `SELECT name, phone, updated_at
           FROM "${totaisTable}"
          WHERE mensagem_enviada = true
          ORDER BY updated_at DESC
          LIMIT 1;`
      );
      if (r3.rows[0]) {
        lastSentAt = r3.rows[0].updated_at;
        lastSentName = r3.rows[0].name;
        lastSentPhone = r3.rows[0].phone;
      }
    }

    if (hasFila) {
      const r2 = await pool.query(`SELECT COUNT(*) AS fila FROM "${filaTable}";`);
      fila = Number(r2.rows[0].fila);
    }

    res.json({
      totais,
      enviados,
      pendentes: totais - enviados,
      fila,
      last_sent_at: lastSentAt,
      last_sent_name: lastSentName,
      last_sent_phone: lastSentPhone,
    });
  } catch (err) {
    console.error('Erro ao obter estatísticas', err);
    res.status(500).json({ error: 'Erro interno ao obter estatísticas' });
  }
});

/** Quota diária (cap, enviados hoje, restantes, janela) */
app.get('/api/quota', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });
  try {
    const cap = DAILY_MESSAGE_COUNT;
    const r = await pool.query(
      `SELECT COUNT(*)::int AS c
         FROM "${slug}_totais"
        WHERE mensagem_enviada = true
          AND updated_at::date = CURRENT_DATE;`
    );
    const sent_today = Number(r.rows?.[0]?.c || 0);
    const remaining = Math.max(0, cap - sent_today);
    res.json({
      cap,
      sent_today,
      remaining,
      window_start: DAILY_START_TIME,
      window_end: DAILY_END_TIME,
      now: new Date().toISOString()
    });
  } catch (err) {
    console.error('Erro em /api/quota', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});

/** Fila (pagina/filtra) — blindado para tabela ausente */
app.get('/api/queue', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const exists = await tableExists(slug);
  if (!exists) return res.json({ items: [], total: 0 });

  const page = parseInt(req.query.page) || 1;
  const pageSize = parseInt(req.query.pageSize) || 25;
  const search = req.query.search || '';
  const offset = (page - 1) * pageSize;
  const values = [];
  let whereClause = '';

  if (search) {
    values.push(`%${search}%`);
    whereClause = `WHERE name ILIKE $1 OR phone ILIKE $1`;
  }

  try {
    const itemsSql = `
      SELECT name, phone
        FROM "${slug}"
      ${whereClause}
      ORDER BY name
      LIMIT $${values.length + 1} OFFSET $${values.length + 2};
    `;
    const itemsRes = await pool.query(itemsSql, [...values, pageSize, offset]);
    const items = itemsRes.rows;

    const countSql = `SELECT COUNT(*) AS total FROM "${slug}" ${whereClause};`;
    const countRes = await pool.query(countSql, values);
    const total = Number(countRes.rows[0].total);

    res.json({ items, total });
  } catch (err) {
    console.error('Erro ao consultar fila', err);
    res.status(500).json({ error: 'Erro interno ao consultar fila' });
  }
});

/** Remoção/Marcação manual a partir da Fila (usado pelos botões do front) */
app.delete('/api/queue', async (req, res) => {
  try {
    const client = req.body?.client;
    const phone = req.body?.phone;
    const markSent = !!req.body?.markSent;

    if (!client || !validateSlug(client) || !phone) {
      return res.status(400).json({ error: 'Parâmetros inválidos' });
    }

    await pool.query(`DELETE FROM "${client}" WHERE phone = $1;`, [phone]);

    let name = null;
    if (markSent) {
      await pool.query(
        `UPDATE "${client}_totais" SET mensagem_enviada = true, updated_at = NOW() WHERE phone = $1;`,
        [phone]
      );
      const nm = await pool.query(
        `SELECT name FROM "${client}_totais" WHERE phone = $1 ORDER BY updated_at DESC LIMIT 1;`,
        [phone]
      );
      name = nm.rows[0]?.name || null;
    }

    const evt = {
      type: 'item',
      name: name || '-',
      phone,
      ok: !!markSent,
      status: markSent ? 'success' : 'skipped',
      at: new Date().toISOString(),
    };
    snapshotPush(client, evt);
    getEmitter(client).emit('progress', evt);

    res.json({ ok: true });
  } catch (err) {
    console.error('Erro em DELETE /api/queue', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});

/** Históricos (pagina/filtra) — blindado para tabela ausente */
app.get('/api/totals', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const totaisTable = `${slug}_totais`;
  const exists = await tableExists(totaisTable);
  if (!exists) return res.json({ items: [], total: 0 });

  const page = parseInt(req.query.page) || 1;
  const pageSize = parseInt(req.query.pageSize) || 25;
  const search = req.query.search || '';
  const sent = (req.query.sent || 'all').toLowerCase();
  const offset = (page - 1) * pageSize;

  const conditions = [];
  const params = [];

  if (search) {
    params.push(`%${search}%`);
    conditions.push(
      `(name ILIKE $${params.length} OR phone ILIKE $${params.length} OR niche ILIKE $${params.length})`
    );
  }

  if (sent !== 'all') {
    if (sent === 'sim') conditions.push('mensagem_enviada = true');
    else if (sent === 'nao') conditions.push('mensagem_enviada = false');
  }

  const whereClause = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

  try {
    const itemsSql = `
      SELECT name, phone, niche, mensagem_enviada, updated_at
        FROM "${totaisTable}"
      ${whereClause}
      ORDER BY updated_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2};
    `;
    const itemsRes = await pool.query(itemsSql, [...params, pageSize, offset]);
    const items = itemsRes.rows;

    const countSql = `SELECT COUNT(*) AS total FROM "${totaisTable}" ${whereClause};`;
    const countRes = await pool.query(countSql, params);
    const total = Number(countRes.rows[0].total);

    res.json({ items, total });
  } catch (err) {
    console.error('Erro ao consultar totais', err);
    res.status(500).json({ error: 'Erro interno ao consultar totais' });
  }
});

/** Adiciona um contato individual */
app.post('/api/contacts', async (req, res) => {
  const { client, name, phone, niche } = req.body;
  if (!client || !validateSlug(client)) return res.status(400).json({ error: 'Cliente inválido' });
  if (!name || !phone) return res.status(400).json({ error: 'Nome e telefone são obrigatórios' });

  try {
    const result = await pool.query(
      'SELECT client_add_contact($1, $2, $3, $4) AS status;',
      [client, name, phone, niche || null]
    );
    const status = result.rows[0]?.status || 'inserted';
    res.json({ status });
  } catch (err) {
    if (err.code === '23505') return res.json({ status: 'skipped_conflict' });
    console.error('Erro ao adicionar contato', err);
    res.status(500).json({ error: 'Erro interno ao adicionar contato' });
  }
});

/** Importa CSV (arquivo + slug) */
app.post('/api/import', upload.single('file'), async (req, res) => {
  try {
    const slug = req.body?.client;
    if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });
    if (!req.file || !req.file.buffer) return res.status(400).json({ error: 'Arquivo não enviado' });

    const text = req.file.buffer.toString('utf8');
    const firstLine = text.split(/\r?\n/)[0] || '';
    const delim = detectDelimiter(firstLine);
    const rows = parseCSV(text, delim);

    if (!rows.length) return res.json({ inserted: 0, skipped: 0, errors: 0 });

    const header = rows[0] || [];
    const idx = mapHeader(header);
    if (idx.name === -1 || idx.phone === -1) {
      return res.status(400).json({ error: 'Cabeçalho inválido. Precisa conter colunas de nome e telefone.' });
    }

    let inserted = 0, skipped = 0, errors = 0;

    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      if (!r || !r.length) continue;

      const name = (r[idx.name] || '').toString().trim();
      const phone = (r[idx.phone] || '').toString().trim();
      const niche = idx.niche !== -1 ? (r[idx.niche] || '').toString().trim() : null;

      if (!name || !phone) { skipped++; continue; }

      try {
        const q = await pool.query('SELECT client_add_contact($1, $2, $3, $4) AS status;', [
          slug, name, phone, niche,
        ]);
        const status = q.rows[0]?.status || 'inserted';
        if (status === 'inserted') inserted++; else skipped++;
      } catch (e) {
        console.error('Erro linha CSV', i, e);
        errors++;
      }
    }

    res.json({ inserted, skipped, errors });
  } catch (err) {
    console.error('Erro no import CSV', err);
    res.status(500).json({ error: 'Erro interno ao importar CSV' });
  }
});

/** Lê configurações do cliente (inclui token/header/scheme) */
app.get('/api/client-settings', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  try {
    const cfg = await getClientSettings(slug);
    res.json({
      autoRun: !!cfg.auto_run,
      iaAuto: !!cfg.ia_auto,
      instanceUrl: cfg.instance_url || null,
      instanceToken: cfg.instance_token || '',
      instanceAuthHeader: cfg.instance_auth_header || 'token',
      instanceAuthScheme: cfg.instance_auth_scheme || '',
      loopStatus: cfg.loop_status || 'idle',
      lastRunAt: cfg.last_run_at || null,
    });
  } catch (err) {
    console.error('Erro ao obter configurações', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});

/** Salva configurações do cliente (inclui token/header/scheme) */
app.post('/api/client-settings', async (req, res) => {
  const {
    client,
    autoRun,
    iaAuto,
    instanceUrl,
    instanceToken,
    instanceAuthHeader,
    instanceAuthScheme,
  } = req.body || {};
  if (!client || !validateSlug(client)) return res.status(400).json({ error: 'Cliente inválido' });

  try {
    if (instanceUrl) {
      try { new URL(instanceUrl); }
      catch { return res.status(400).json({ error: 'instanceUrl inválida' }); }
    }

    await saveClientSettings(client, {
      autoRun,
      iaAuto,
      instanceUrl,
      instanceToken,
      instanceAuthHeader,
      instanceAuthScheme,
    });

    const cfg = await getClientSettings(client);
    res.json({ ok: true, settings: cfg });
  } catch (err) {
    console.error('Erro ao salvar configurações', err);
    res.status(500).json({ error: 'Erro interno ao salvar configurações' });
  }
});

/** Apaga completamente as tabelas e as configurações de um cliente */
app.delete('/api/delete-client', async (req, res) => {
  try {
    const client = req.body?.client || req.query?.client;
    if (!client || !validateSlug(client)) return res.status(400).json({ error: 'Cliente inválido' });

    if (runningClients.has(client)) {
      return res.status(409).json({ error: 'Loop em execução para este cliente. Tente novamente em instantes.' });
    }

    await pool.query('BEGIN');
    await pool.query(`DROP TABLE IF EXISTS "${client}" CASCADE;`);
    await pool.query(`DROP TABLE IF EXISTS "${client}_totais" CASCADE;`);
    await pool.query(`DELETE FROM client_settings WHERE slug = $1;`, [client]);
    await pool.query('COMMIT');

    runningClients.delete(client);
    res.json({ status: 'ok', deleted: client });
  } catch (err) {
    console.error('Erro ao apagar cliente', err);
    try { await pool.query('ROLLBACK'); } catch {}
    res.status(500).json({ error: 'Erro interno ao apagar cliente' });
  }
});

/* ======================  >>> ADIÇÃO: Parada manual do loop  ====================== */
const stopRequests = new Set();
async function sleepAbortable(ms, slug) {
  const step = 250;
  let elapsed = 0;
  while (elapsed < ms) {
    if (stopRequests.has(slug)) return 'aborted';
    const toWait = Math.min(step, ms - elapsed);
    await new Promise((r) => setTimeout(r, toWait));
    elapsed += toWait;
  }
  return 'ok';
}

app.post('/api/stop-loop', async (req, res) => {
  const client = req.body?.client;
  if (!client || !validateSlug(client)) {
    return res.status(400).json({ ok: false, message: 'Cliente inválido' });
  }
  if (!runningClients.has(client)) {
    return res.status(404).json({ ok: false, message: `Nenhum loop ativo para ${client}` });
  }

  stopRequests.add(client);
  try {
    await pool.query(`UPDATE client_settings SET loop_status='stopping', last_run_at=NOW() WHERE slug=$1`, [client]);
  } catch {}

  console.log(`[STOP] Parada solicitada para ${client}`);
  return res.json({ ok: true, message: `Parada solicitada para ${client}` });
});

/* ======================  >>> ADIÇÃO: Buscar & salvar LEADS  ====================== */
app.post('/api/leads', async (req, res) => {
  try {
    const { client, region, niche, limit } = req.body || {};
    if (!client || !validateSlug(client)) {
      return res.status(400).json({ error: 'Cliente inválido' });
    }

    await ensureRegionColumns(client);

    const raw = await searchLeads({ region, niche, limit });
    const results = Array.isArray(raw) ? raw : [];

    let inserted = 0, skipped = 0, errors = 0;

    for (const item of results) {
      const name   = (item.name && String(item.name).trim()) || String(item.phone || '').trim();
      const phone  = String(item.phone || '').trim();
      const reg    = (item.region ?? region) || null;
      const nich   = (item.niche  ?? niche ) || null;

      if (!phone) { skipped++; continue; }

      try {
        const r = await pool.query(`SELECT client_add_lead($1,$2,$3,$4,$5) AS status;`,
          [client, name, phone, reg, nich]);
        const status = r.rows?.[0]?.status || 'inserted';
        if (status === 'inserted' || status === 'queued_existing') inserted++;
        else skipped++;
      } catch (e) {
        console.error('Erro ao inserir lead', client, phone, e);
        errors++;
      }
    }

    res.json({ found: results.length, inserted, skipped, errors });
  } catch (err) {
    console.error('Erro em /api/leads', err);
    res.status(500).json({ error: 'Erro interno na busca de leads' });
  }
});

/* ======================  >>> ADIÇÃO: Renomear cliente (SLUG)  ====================== */
/**
 * Endpoint para renomear completamente um cliente.
 * POST /api/rename-client
 * Body: { oldSlug: string, newSlug: string }
 *
 * - Renomeia as tabelas "<oldSlug>" -> "<newSlug>" e "<oldSlug>_totais" -> "<newSlug>_totais"
 * - Atualiza client_settings.slug
 * - Ajusta estruturas em memória (SSE, estado de progresso, flags de stop/loop)
 */
app.post('/api/rename-client', async (req, res) => {
  const oldSlug = req.body?.oldSlug;
  const newSlug = req.body?.newSlug;

  // Validação básica dos slugs
  if (!validateSlug(oldSlug) || !validateSlug(newSlug)) {
    return res.status(400).json({ error: 'Slugs inválidos. Use [a-z0-9_], 1..64 chars.' });
  }
  if (oldSlug === newSlug) {
    return res.status(400).json({ error: 'oldSlug e newSlug são iguais.' });
  }

  try {
    // Não renomear se o loop estiver em execução
    if (runningClients.has(oldSlug)) {
      return res.status(409).json({ error: `Loop em execução para ${oldSlug}. Pare antes de renomear.` });
    }

    // Verifica se tabelas antigas existem
    const oldExists = await tableExists(oldSlug);
    const oldTotExists = await tableExists(`${oldSlug}_totais`);
    if (!oldExists || !oldTotExists) {
      return res.status(404).json({ error: `Tabelas de ${oldSlug} não encontradas.` });
    }

    // Não permitir sobrescrever tabelas existentes
    const newExists = await tableExists(newSlug);
    const newTotExists = await tableExists(`${newSlug}_totais`);
    if (newExists || newTotExists) {
      return res.status(409).json({ error: `Já existem tabelas para ${newSlug}.` });
    }

    // Renomeia tabelas e ajusta client_settings
    await pool.query('BEGIN');
    await pool.query(`ALTER TABLE "${oldSlug}" RENAME TO "${newSlug}";`);
    await pool.query(`ALTER TABLE "${oldSlug}_totais" RENAME TO "${newSlug}_totais";`);

    const cs = await pool.query(`SELECT 1 FROM client_settings WHERE slug = $1;`, [oldSlug]);
    if (cs.rowCount) {
      await pool.query(`UPDATE client_settings SET slug = $1 WHERE slug = $2;`, [newSlug, oldSlug]);
    } else {
      // Cria registro padrão caso não exista
      await pool.query(
        `INSERT INTO client_settings (slug, loop_status, last_run_at) VALUES ($1, 'idle', NOW());`,
        [newSlug]
      );
    }
    await pool.query('COMMIT');

    // Ajustar referências em memória para SSE e estados
    if (progressEmitters.has(oldSlug)) {
      progressEmitters.set(newSlug, progressEmitters.get(oldSlug));
      progressEmitters.delete(oldSlug);
    }
    if (progressStates.has(oldSlug)) {
      progressStates.set(newSlug, progressStates.get(oldSlug));
      progressStates.delete(oldSlug);
    }
    stopRequests.delete(oldSlug);
    runningClients.delete(oldSlug);

    return res.json({ ok: true, oldSlug, newSlug });
  } catch (err) {
    console.error('Erro em /api/rename-client', err);
    try { await pool.query('ROLLBACK'); } catch {}
    return res.status(500).json({ error: 'Erro interno ao renomear cliente' });
  }
});

/**
 * Endpoint para iniciar manualmente o loop de processamento de um cliente.
 * Espera body JSON: { client: 'cliente_x', iaAuto?: boolean }
 */
app.post('/api/loop', async (req, res) => {
  const clientSlug = req.body?.client;
  const iaAutoOverride = req.body?.iaAuto;
  if (!clientSlug || !validateSlug(clientSlug)) return res.status(400).json({ error: 'Cliente inválido' });

  try {
    const result = await runLoopForClient(clientSlug, { iaAutoOverride });
    res.json({ message: 'Loop executado', processed: result.processed, status: result.status || 'ok' });
  } catch (err) {
    console.error('Erro ao executar loop manual', err);
    res.status(500).json({ error: 'Erro interno ao executar loop' });
  }
});

/** SSE de progresso por cliente (com replay do último estado) */
app.get('/api/progress', (req, res) => {
  try {
    const client = req.query?.client;
    if (!client || !validateSlug(client)) return res.status(400).json({ error: 'Cliente inválido' });

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    res.write(`event: ping\ndata: {}\n\n`);

    try {
      const st = progressStates.get(client);
      if (st?.lastStart) res.write(`data: ${JSON.stringify(st.lastStart)}\n\n`);
      if (st?.items?.length) for (const it of st.items) res.write(`data: ${JSON.stringify(it)}\n\n`);
      if (st?.lastEnd) res.write(`data: ${JSON.stringify(st.lastEnd)}\n\n`);
    } catch {}

    const em = getEmitter(client);
    const onProgress = (payload) => {
      try { res.write(`data: ${JSON.stringify(payload)}\n\n`); } catch {}
    };
    em.on('progress', onProgress);

    const ka = setInterval(() => {
      try { res.write(`event: ping\ndata: {}\n\n`); } catch {}
    }, 15000);

    req.on('close', () => {
      em.off('progress', onProgress);
      clearInterval(ka);
      try { res.end(); } catch {}
    });
  } catch {
    try { res.end(); } catch {}
  }
});

/* ======================  Loop de processamento por cliente  ====================== */
async function runLoopForClient(clientSlug, opts = {}) {
  if (!validateSlug(clientSlug)) throw new Error('Slug inválido');
  if (runningClients.has(clientSlug)) return { processed: 0, status: 'already_running' };

  runningClients.add(clientSlug);
  const batchSize = parseInt(process.env.LOOP_BATCH_SIZE, 10) || opts.batchSize || DAILY_MESSAGE_COUNT;

  try {
    await pool.query(
      `INSERT INTO client_settings (slug, loop_status, last_run_at)
       VALUES ($1, 'running', NOW())
       ON CONFLICT (slug) DO UPDATE SET loop_status = 'running', last_run_at = NOW()`,
      [clientSlug]
    );

    const exists = await tableExists(clientSlug);
    if (!exists) {
      await pool.query(
        `INSERT INTO client_settings (slug, loop_status, last_run_at)
         VALUES ($1, 'idle', NOW())
         ON CONFLICT (slug) DO UPDATE SET loop_status = 'idle', last_run_at = NOW()`,
        [clientSlug]
      );
      return { processed: 0, status: 'ok' };
    }

    // total na fila
    let totalCount = 0;
    try {
      const _cnt = await pool.query(`SELECT COUNT(*) AS count FROM "${clientSlug}";`);
      totalCount = Number(_cnt.rows?.[0]?.count || 0);
    } catch {}

    // Snapshot de início para SSE
    try {
      snapshotStart(clientSlug, totalCount);
      getEmitter(clientSlug).emit('progress', { type: 'start', total: totalCount, at: new Date().toISOString() });
    } catch {}

    const settings = await getClientSettings(clientSlug);
    let processed = 0;
    let manualStop = false;
    const useIA = typeof opts.iaAutoOverride === 'boolean' ? opts.iaAutoOverride : !!settings.ia_auto;

    // Cota diária: contar enviados hoje
    let alreadySentToday = 0;
    try {
      const sentTodayRes = await pool.query(
        `SELECT COUNT(*)::int AS c
           FROM "${clientSlug}_totais"
          WHERE mensagem_enviada = true
            AND updated_at::date = CURRENT_DATE;`
      );
      alreadySentToday = Number(sentTodayRes.rows?.[0]?.c || 0);
      console.log(`[${clientSlug}] Enviadas hoje: ${alreadySentToday}/${DAILY_MESSAGE_COUNT}`);
    } catch (e) {
      console.warn(`[${clientSlug}] Falha ao contar envios de hoje`, e);
    }

    if (stopRequests.has(clientSlug)) {
      manualStop = true;
    }

    const remainingToday = Math.max(0, DAILY_MESSAGE_COUNT - alreadySentToday);
    if (!manualStop && remainingToday <= 0) {
      console.log(`[${clientSlug}] Cota diária (${DAILY_MESSAGE_COUNT}) atingida. Encerrando.`);
      try {
        snapshotEnd(clientSlug, processed, { reason: 'daily_quota' });
        getEmitter(clientSlug).emit('progress', { type: 'end', processed, at: new Date().toISOString(), reason: 'daily_quota' });
      } catch {}
      await pool.query(`UPDATE client_settings SET loop_status='idle', last_run_at=NOW() WHERE slug=$1;`, [clientSlug]);
      return { processed, status: 'quota_reached' };
    }

    const scheduleDelays = generateScheduleDelays(DAILY_MESSAGE_COUNT, DAILY_START_TIME, DAILY_END_TIME);
    const messageLimit = Math.min(batchSize, scheduleDelays.length);

    const planCount = Math.min(messageLimit, remainingToday);
    if (!manualStop) {
      try {
        let acc = 0;
        const planned = [];
        for (let i = 0; i < planCount; i++) {
          acc += scheduleDelays[i];
          planned.push(new Date(Date.now() + acc * 1000).toISOString());
        }
        getEmitter(clientSlug).emit('progress', { type: 'schedule', planned, remainingToday, cap: DAILY_MESSAGE_COUNT });
      } catch {}
    }

    const attemptedPhones = new Set();

    for (let i = 0; i < messageLimit; i++) {
      if (stopRequests.has(clientSlug)) { manualStop = true; break; }
      if (i >= remainingToday) {
        console.log(`[${clientSlug}] Cota diária atingida durante o ciclo. Encerrando.`);
        break;
      }

      const delaySec = scheduleDelays[i];
      if (delaySec > 0) {
        const when = new Date(Date.now() + delaySec * 1000);
        console.log(
          `[${clientSlug}] Aguardando ${delaySec}s (${when.toTimeString().split(' ')[0]}) para enviar a mensagem ${i + 1}/${messageLimit}.`
        );
        const slept = await sleepAbortable(delaySec * 1000, clientSlug);
        if (slept === 'aborted') { manualStop = true; break; }
      }

      if (stopRequests.has(clientSlug)) { manualStop = true; break; }

      let whereNotIn = '';
      let params = [];
      if (attemptedPhones.size) {
        const arr = Array.from(attemptedPhones);
        const ph = arr.map((_, idx) => `$${idx + 1}`).join(',');
        whereNotIn = `WHERE phone NOT IN (${ph})`;
        params = arr;
      }

      const next = await pool.query(
        `SELECT name, phone FROM "${clientSlug}" ${whereNotIn} ORDER BY name LIMIT 1;`,
        params
      );
      if (next.rows.length === 0) break;

      const { name, phone } = next.rows[0];
      attemptedPhones.add(phone);

      let sendRes = null;
      let status = 'skipped';
      let shouldMark = false;

      if (!manualStop) {
        if (useIA) {
          sendRes = await runIAForContact({
            client: clientSlug,
            name,
            phone,
            instanceUrl: settings.instance_url,
            instanceToken: settings.instance_token,
            instanceAuthHeader: settings.instance_auth_header,
            instanceAuthScheme: settings.instance_auth_scheme,
          });
          status = sendRes && sendRes.ok ? 'success' : 'error';
          shouldMark = status === 'success';
        } else {
          status = 'skipped';
          shouldMark = false;
        }
      }

      if (stopRequests.has(clientSlug)) { manualStop = true; }

      if (shouldMark && !manualStop) {
        try {
          await pool.query(`DELETE FROM "${clientSlug}" WHERE phone = $1;`, [phone]);
        } catch (err) {
          console.error('Erro ao deletar da fila', clientSlug, phone, err);
        }
        try {
          await pool.query(
            `UPDATE "${clientSlug}_totais" SET mensagem_enviada = true, updated_at = NOW() WHERE phone = $1;`,
            [phone]
          );
        } catch (err) {
          console.error('Erro ao atualizar histórico', clientSlug, phone, err);
        }
        processed++;
      } else if (!manualStop) {
        console.warn(`[${clientSlug}] NÃO marcou como enviada (${status}). Mantendo na fila: ${phone}`);
      }

      try {
        const evt = {
          type: 'item',
          name,
          phone,
          ok: shouldMark && !manualStop,
          status: manualStop ? 'stopped' : status,
          at: new Date().toISOString(),
        };
        snapshotPush(clientSlug, evt);
        getEmitter(clientSlug).emit('progress', evt);
      } catch {}

      if (manualStop) break;
    }

    await pool.query(
      `INSERT INTO client_settings (slug, loop_status, last_run_at)
       VALUES ($1, 'idle', NOW())
       ON CONFLICT (slug) DO UPDATE SET loop_status = 'idle', last_run_at = NOW()`,
      [clientSlug]
    );

    try {
      snapshotEnd(clientSlug, processed, manualStop ? { reason: 'manual_stop' } : {});
      getEmitter(clientSlug).emit('progress', {
        type: 'end',
        processed,
        at: new Date().toISOString(),
        ...(manualStop ? { reason: 'manual_stop' } : {})
      });
    } catch {}

    if (manualStop) {
      console.log(`[${clientSlug}] Loop encerrado manualmente.`);
    }

    return { processed, status: manualStop ? 'stopped' : 'ok' };
  } catch (err) {
    console.error('Erro no runLoopForClient', clientSlug, err);
    return { processed: 0, status: 'error' };
  } finally {
    stopRequests.delete(clientSlug);
    runningClients.delete(clientSlug);
  }
}

/* =====================  Scheduler: Auto-run diário  ===================== */
function scheduleDailyAutoRun() {
  const now = new Date();
  const nextRun = new Date(now);
  nextRun.setHours(8, 0, 0, 0);
  if (now >= nextRun) nextRun.setDate(nextRun.getDate() + 1);
  const msUntilNext = nextRun.getTime() - now.getTime();

  setTimeout(async () => {
    try {
      const { rows } = await pool.query(`SELECT slug FROM client_settings WHERE auto_run = true;`);
      for (const { slug } of rows) {
        try {
          if (runningClients.has(slug)) continue;
          const exists = await tableExists(slug);
          if (!exists) continue;
          const cnt = await pool.query(`SELECT COUNT(*) AS count FROM "${slug}";`);
          const queueCount = Number(cnt.rows[0].count);
          if (queueCount > 0) {
            runLoopForClient(slug).catch((e) => console.error('Auto-run erro', slug, e));
          }
        } catch (err) {
          console.error('Erro ao executar loop automático para', slug, err);
        }
      }
    } catch (err) {
      console.error('Erro no scheduler de loop automático', err);
    } finally {
      scheduleDailyAutoRun();
    }
  }, msUntilNext);
}
scheduleDailyAutoRun();

/* =====================  Catch-all  ===================== */
app.get('*', (_req, res) => res.status(404).json({ error: 'Not found' }));

/* =====================  Boot  ===================== */
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
