// server.js
// Servidor Express para a aplicação Luna

require('dotenv').config();

const express = require('express');
const { Pool } = require('pg');
const EventEmitter = require('events');
const multer = require('multer');
const path = require('path');
const axios = require('axios');

const app = express();

// ==== Helpers reutilizáveis ====
// Importa funções utilitárias (extractChatId, pickArrayList, etc.) do novo módulo utils/helpers.
// Embora muitas destas funções ainda estejam definidas localmente neste arquivo por questões de compatibilidade,
// trazer a dependência aqui esclarece onde estão centralizadas as implementações e permite reutilização futura.
const helpers = require('./utils/helpers');

// ==== Integração com UAZAPI ====
// Carrega o cliente UAZAPI a partir do novo serviço em services/uazapi
const { buildClient } = require('./services/uazapi');

// Variáveis UAZAPI
const UAZAPI_BASE_URL = process.env.UAZAPI_BASE_URL;
const UAZAPI_ADMIN_TOKEN = process.env.UAZAPI_ADMIN_TOKEN;

// Origens permitidas (aceita CORS_ORIGINS OU FRONT_ORIGINS)
const ORIGINS_RAW = process.env.CORS_ORIGINS || process.env.FRONT_ORIGINS || '';
const CORS_ANY = process.env.CORS_ANY === 'true';
const CORS_ORIGINS = ORIGINS_RAW.split(',').map(s => s.trim()).filter(Boolean);

// Hosts extras permitidos para o proxy de mídia
const MEDIA_PROXY_ALLOW = (process.env.MEDIA_PROXY_ALLOW || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

// Caminho opcional para encaminhar "button reply" do NativeFlow à UAZAPI
const UAZAPI_INTERACTIVE_REPLY_PATH = process.env.UAZAPI_INTERACTIVE_REPLY_PATH || '';

// Cliente da UAZAPI
const uaz = buildClient(UAZAPI_BASE_URL);

// === Cache de instâncias para Supervisão ===
let instanceCache = new Map();
let lastInstancesRefresh = 0;
const INSTANCES_TTL_MS = 30 * 1000;

// Utils para chats/mensagens
function extractChatId(chat) {
  return (
    chat?.wa_chatid ||
    chat?.wa_fastid ||
    chat?.wa_id ||
    chat?.jid ||
    chat?.number ||
    chat?.id ||
    chat?.chatid ||
    chat?.wa_jid ||
    null
  );
}

function pickArrayList(data) {
  if (Array.isArray(data?.content)) return data.content;
  if (Array.isArray(data?.chats)) return data.chats;
  if (Array.isArray(data?.messages)) return data.messages;
  if (Array.isArray(data?.data)) return data.data;
  if (Array.isArray(data)) return data;
  return [];
}

function normalizeStatus(status) {
  if (!status || typeof status !== 'object') return { connected: false };
  if (typeof status.connected !== 'undefined') return { ...status, connected: !!status.connected };
  const s = JSON.stringify(status || {}).toLowerCase();
  return { ...status, connected: s.includes('"connected":true') || s.includes('online') };
}

function resolveAvatar(obj) {
  return (
    obj?.avatarUrl ||
    obj?.profilePicUrl ||
    obj?.picture ||
    obj?.picUrl ||
    obj?.photoUrl ||
    obj?.imageUrl ||
    obj?.wa_profilePicUrl ||
    obj?.icon ||
    null
  );
}

async function refreshInstances(force = false) {
  const now = Date.now();
  if (!force && now - lastInstancesRefresh < INSTANCES_TTL_MS && instanceCache.size > 0) return;
  const data = await uaz.listInstances(UAZAPI_ADMIN_TOKEN);
  const list = Array.isArray(data?.content) ? data.content : Array.isArray(data) ? data : [];
  const newCache = new Map();
  for (const it of list) {
    newCache.set(it.id || it._id || it.instanceId || it.token, it);
  }
  instanceCache = newCache;
  lastInstancesRefresh = now;
}

function findInstanceById(id) {
  return instanceCache.get(id);
}
function resolveInstanceToken(id) {
  const inst = findInstanceById(id);
  if (!inst) return null;
  return inst.token || inst.instanceToken || inst.key || null;
}

/* ======================  Janela e cota diária  ====================== */
const DAILY_MESSAGE_COUNT = 30;
const DAILY_START_TIME = '08:00:00';
const DAILY_END_TIME   = '17:30:00';

// === Parâmetros e helpers para análise de conversas ===
// Estes parâmetros controlam a quantidade de chats e mensagens a considerar, bem como o orçamento de tokens
// para o modelo da OpenAI. Podem ser ajustados via variáveis de ambiente.
const ANALYSIS_MODEL = process.env.OPENAI_MODEL || 'gpt-3.5-turbo';
const ANALYSIS_MAX_CHATS = parseInt(process.env.ANALYSIS_MAX_CHATS || '30', 10);
const ANALYSIS_PER_CHAT_LIMIT = parseInt(process.env.ANALYSIS_PER_CHAT_LIMIT || '200', 10);
const ANALYSIS_INPUT_BUDGET = parseInt(process.env.OPENAI_INPUT_BUDGET || '12000', 10);
// Define a cota padrão de tokens de saída para o modelo.  Modelos de raciocínio como
// gpt‑5-mini podem consumir muitos tokens apenas para "pensar" e não deixar espaço
// para a resposta.  Para evitar respostas vazias, aumente o orçamento padrão.  A
// variável de ambiente OPENAI_OUTPUT_BUDGET ainda pode ser utilizada para ajustar
// este valor em produção.
const ANALYSIS_OUTPUT_BUDGET = parseInt(process.env.OPENAI_OUTPUT_BUDGET || '4096', 10);
// Prompt padrão para o papel "system". Pode ser sobrescrito via OPENAI_SYSTEM_PROMPT.
const DEFAULT_SYSTEM_PROMPT =
  'Você é um analista de conversas da assistente Luna (WhatsApp B2B). ' +
  'Analise as conversas recentes e proponha melhorias objetivas de abertura, abordagem, qualificação, follow-ups e fechamento. ' +
  'Sugira ajustes no tom, clareza, timing e conteúdo das mensagens para melhorar engajamento e taxa de resposta. ' +
  'Forneça exemplos de frases prontas e bullets acionáveis. Se houver poucas mensagens, adapte a análise.';
const SYSTEM_PROMPT_OVERRIDE = process.env.OPENAI_SYSTEM_PROMPT || '';

// Logger para registrar execuções de análise
const { appendLog } = require('./utils/logger');

// Helpers para estimar tokens e normalizar texto
function approxTokens(str) {
  if (!str) return 0;
  return Math.ceil(String(str).length / 4);
}
function normalizeLine(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

// Constrói uma linha de transcript simples a partir de uma mensagem
function toTranscriptLine(msg) {
  const ts = msg?.messageTimestamp || msg?.timestamp || msg?.wa_timestamp || msg?.createdAt || msg?.date || '';
  const fromMe =
    msg?.fromMe === true ||
    msg?.sender?.fromMe === true ||
    msg?.me === true ||
    (msg?.key && msg.key.fromMe === true);
  const who = fromMe ? 'Usuário' : 'Cliente';
  let text =
    msg?.text ||
    msg?.body ||
    msg?.message ||
    (typeof msg?.content === 'string' ? msg.content : msg?.content?.text) ||
    msg?.caption ||
    '';
  text = normalizeLine(text);
  return `[${ts}] ${who}: ${text}`;
}

function hmsToSeconds(hms) {
  const parts = String(hms || '').split(':').map((p) => parseInt(p, 10) || 0);
  const [h, m, s] = [parts[0] || 0, parts[1] || 0, parts[2] || 0];
  return h * 3600 + m * 60 + s;
}
function generateScheduleDelays(count, startStr, endStr) {
  const now = new Date();
  const nowSec   = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();
  const startSec = hmsToSeconds(startStr);
  const endSec   = hmsToSeconds(endStr);
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
    delays.push(i === 0 ? (effectiveStart - nowSec) + off : off - prev);
    prev = off;
  }
  return delays;
}

/* ======================  CORS  ====================== */
app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (CORS_ANY) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  } else if (CORS_ORIGINS.length > 0) {
    if (origin && CORS_ORIGINS.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
    } else {
      // fallback seguro: abre apenas para preflight e devolve 403 nos verbos não-OPTIONS mais abaixo
      res.setHeader('Access-Control-Allow-Origin', '*');
    }
  } else {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }

  res.setHeader('Vary', 'Origin, Access-Control-Request-Headers');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.setHeader(
    'Access-Control-Allow-Headers',
    req.headers['access-control-request-headers'] ||
      'Content-Type, Authorization, token, Range, X-Requested-With'
  );
  res.setHeader('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length');

  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }

  // Se há whitelist e a origem não está na lista, bloqueia (exceto se CORS_ANY)
  if (!CORS_ANY && CORS_ORIGINS.length && origin && !CORS_ORIGINS.includes(origin)) {
    return res.status(403).json({ error: 'Origin not allowed' });
  }

  next();
});

/* ======================  Banco de Dados  ====================== */
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
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
  // ADIÇÕES
  'leads',
  'loop-state',
  'sent-today',
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
  return /^cliente_[a-z0-9_]+$/.test(slug) || /^[a-z0-9_]+$/.test(slug);
}

/* ======================  Estado e SSE por cliente  ====================== */
const runningClients = new Set();
const progressEmitters = new Map();
const progressStates = new Map();
function getEmitter(slug) {
  if (!progressEmitters.has(slug)) progressEmitters.set(slug, new EventEmitter());
  return progressEmitters.get(slug);
}
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
  last_run_at TIMESTAMPTZ,
  daily_limit INTEGER DEFAULT 30,
  message_template TEXT
);
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS instance_token TEXT;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS instance_auth_header TEXT;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS instance_auth_scheme TEXT;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS loop_status TEXT DEFAULT 'idle';
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS last_run_at TIMESTAMPTZ;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS daily_limit INTEGER DEFAULT 30;
  -- Armazena a data/hora da última análise de conversas enviada ao ChatGPT para cada cliente.
  ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS analysis_last_msg_ts TIMESTAMPTZ;
ALTER TABLE client_settings ADD COLUMN IF NOT EXISTS message_template TEXT;
`);
}

ensureSettingsTable().catch((e) => console.error('ensureSettingsTable', e));

async function getClientSettings(slug) {
  const { rows } = await pool.query(
    `SELECT auto_run, ia_auto, instance_url, loop_status, last_run_at,
            instance_token, instance_auth_header, instance_auth_scheme,
            daily_limit, message_template, analysis_last_msg_ts
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
      daily_limit: null,
      message_template: null,
    };
  }
  return rows[0];
}


async function saveClientSettings(
  slug,
  {
    autoRun, iaAuto, instanceUrl, instanceToken,
    instanceAuthHeader, instanceAuthScheme, dailyLimit,
    messageTemplate, // << novo
  }
) {
  const safeDaily =
    Number.isFinite(Number(dailyLimit)) && Number(dailyLimit) > 0
      ? Math.min(10000, Math.floor(Number(dailyLimit)))
      : null;

  // sanitize header and scheme: if someone saved the token value into the header field, default back to "token"
  let headerName = (instanceAuthHeader && instanceAuthHeader.trim()) || 'token';
  // if header name equals the token or is extremely long (> 50 chars), assume it's a mistake and reset to "token"
  if (headerName === instanceToken || headerName.length > 50) {
    headerName = 'token';
  }
  let authScheme = instanceAuthScheme;
  if (authScheme == null) authScheme = '';

  await pool.query(
    `INSERT INTO client_settings
       (slug, auto_run, ia_auto, instance_url, instance_token, instance_auth_header, instance_auth_scheme, daily_limit, message_template)
     VALUES ($1,   $2,       $3,     $4,           $5,             $6,                   $7,             $8,           $9)
     ON CONFLICT (slug)
     DO UPDATE SET
       auto_run = EXCLUDED.auto_run,
       ia_auto = EXCLUDED.ia_auto,
       instance_url = EXCLUDED.instance_url,
       instance_token = EXCLUDED.instance_token,
       instance_auth_header = EXCLUDED.instance_auth_header,
       instance_auth_scheme = EXCLUDED.instance_auth_scheme,
       daily_limit = COALESCE(EXCLUDED.daily_limit, client_settings.daily_limit),
       message_template = EXCLUDED.message_template`,
    [
      slug,
      !!autoRun,
      !!iaAuto,
      instanceUrl || null,
      instanceToken || null,
      headerName,
      authScheme,
      safeDaily,
      messageTemplate ?? null,
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

// >>>> ACEITA {NICHO}/{NICHE}
function fillTemplate(tpl, vars) {
  // Suporta {NAME|NOME|CLIENT|CLIENTE|PHONE|TELEFONE|NICHO|NICHE} (case-insensitive)
  return String(tpl || '').replace(/\{(NAME|NOME|CLIENT|CLIENTE|PHONE|TELEFONE|NICHO|NICHE)\}/gi, (_, k) => {
    const key = k.toUpperCase();
    const map = { NOME: 'NAME', CLIENTE: 'CLIENT', TELEFONE: 'PHONE', NICHE: 'NICHO' };
    const finalKey = map[key] || key;
    return vars[finalKey] ?? '';
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
  extra: (() => { try { return JSON.parse(process.env.UAZAPI_EXTRA || '{}'); } catch { return {}; } })(),
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
    payload[UAZ.textField]  = text;
    return { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) };
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
    Object.entries(UAZ.extra || {}).forEach(([k, v]) => form.set(k, typeof v === 'object' ? JSON.stringify(v) : String(v)));
    form.set(UAZ.phoneField, phoneValue);
    form.set(UAZ.textField,  text);
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
    } catch (err) { reject(err); }
  });
}

// normaliza exibição do nicho (ex.: "DESENVOLVIMENTO DE SISTEMAS" -> "Desenvolvimento De Sistemas")
function normalizeNiche(n) {
  if (!n) return '';
  const s = String(n).trim();
  return s.toLowerCase().replace(/\b\p{L}/gu, c => c.toUpperCase());
}

async function runIAForContact({
  client, name, phone, niche,
  instanceUrl, instanceToken, instanceAuthHeader, instanceAuthScheme,
  messageTemplate, // << novo
}) {
  const SHOULD_CALL = process.env.IA_CALL === 'true';
  if (!SHOULD_CALL || !instanceUrl) return { ok: true, simulated: true };

  try {
    const e164 = normalizePhoneE164BR(phone);
    const digits = String(e164).replace(/\D/g, '');
    const prettyNiche = normalizeNiche(niche);

    // Pega o template do cliente ou cai no global (.env)
    const tpl = (typeof messageTemplate === 'string' && messageTemplate.trim())
      ? messageTemplate
      : UAZ.template;

    const text = fillTemplate(tpl, {
      NAME: name,
      CLIENT: client,
      PHONE: e164,
      NICHO: prettyNiche,
    });

    const req = buildUazRequest(instanceUrl, { e164, digits, text });

    let hdrName   = (instanceAuthHeader && instanceAuthHeader.trim()) || UAZ.authHeader || 'token';
    const hdrScheme = instanceAuthScheme !== undefined ? instanceAuthScheme : UAZ.authScheme || '';
    const tokenVal  = (instanceToken && String(instanceToken)) || UAZ.token || '';
    // Se o nome do header foi salvo erroneamente com o próprio token (ou ficou muito longo), saneie para "token"
    if (hdrName === tokenVal || hdrName.length > 50) {
      hdrName = UAZ.authHeader || 'token';
    }
    if (tokenVal) {
      req.headers = req.headers || {};
      req.headers[hdrName] = `${hdrScheme}${tokenVal}`;
    }

    if (process.env.DEBUG === 'true') {
      const maskedHeaders = Object.fromEntries(
        Object.entries(req.headers || {}).map(([k, v]) => [
          k,
          /token|authorization/i.test(k) ? '***' : v,
        ])
      );
      console.log('[UAZAPI] request', { url: req.url, method: req.method, headers: maskedHeaders, hasBody: !!req.body });
    }

    const resp = await httpSend(req);
    let body;
    try { body = await resp.json(); } catch { body = await resp.text(); }
    if (!resp.ok) console.error('UAZAPI FAIL', { status: resp.status, body });

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
  const semis  = (firstLine.match(/;/g) || []).length;
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
  const nameKeys  = new Set(['nome','name','full_name','fullname','contato','empresa','nomefantasia','razaosocial']);
  const phoneKeys = new Set(['telefone','numero','número','phone','whatsapp','celular','mobile','telemovel']);
  const nicheKeys = new Set(['nicho','niche','segmento','categoria','industry']);
  names.forEach((h, i) => {
    if (isId(h)) return;
    if (idx.name  === -1 && nameKeys.has(h))  idx.name  = i;
    if (idx.phone === -1 && phoneKeys.has(h)) idx.phone = i;
    if (idx.niche === -1 && nicheKeys.has(h)) idx.niche = i;
  });
  return idx;
}

/* ======================  ADIÇÃO: integração com buscador de leads  ====================== */
// Carrega o serviço de busca de leads a partir de services/leadsSearcher
const { searchLeads } = require('./services/leadsSearcher');

async function ensureRegionColumns(slug) {
  try { await pool.query(`ALTER TABLE "${slug}" ADD COLUMN IF NOT EXISTS region TEXT;`); }
  catch (e) { console.warn('ensureRegionColumns fila', slug, e?.message); }
  try { await pool.query(`ALTER TABLE "${slug}_totais" ADD COLUMN IF NOT EXISTS region TEXT;`); }
  catch (e) { console.warn('ensureRegionColumns totais', slug, e?.message); }
}

/* ======================  Endpoints ====================== */

// Healthcheck
app.get('/api/healthz', (_req, res) => res.json({ up: true }));

// Estado do loop / cota de hoje
app.get('/api/loop-state', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  try {
    let loop_status = 'idle', last_run_at = null, cap = DAILY_MESSAGE_COUNT;

    try {
      const r2 = await pool.query(
        `SELECT loop_status, last_run_at, COALESCE(daily_limit, $2) AS cap
           FROM client_settings WHERE slug = $1;`,
        [slug, DAILY_MESSAGE_COUNT]
      );
      if (r2.rows[0]) {
        loop_status = r2.rows[0].loop_status || 'idle';
        last_run_at = r2.rows[0].last_run_at || null;
        cap = Number(r2.rows[0].cap) || DAILY_MESSAGE_COUNT;
      }
    } catch {}

    // <<< NOVO: verdade de fato (memória do processo) >>>
    const isActuallyRunning = runningClients.has(slug);

    // Auto-heal: se o DB diz "running" mas nada está rodando, normaliza para "idle"
    if (!isActuallyRunning && loop_status === 'running') {
      loop_status = 'idle';
      try {
        await pool.query(`UPDATE client_settings SET loop_status='idle' WHERE slug=$1`, [slug]);
      } catch {}
    }

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

    const remaining_today = Math.max(0, cap - sent_today);

    res.json({
      cap,
      sent_today,
      remaining_today,
      window_start: DAILY_START_TIME,
      window_end: DAILY_END_TIME,
      loop_status,                      // já normalizado
      actually_running: isActuallyRunning, // <<< NOVO: front pode usar
      last_run_at,
      now: new Date().toISOString(),
    });
  } catch (err) {
    console.error('Erro em /api/loop-state', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});


// Busca de leads (consulta)
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

// Enviados hoje
app.get('/api/sent-today', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const table = `${slug}_totais`;
  const exists = await tableExists(table);
  if (!exists) return res.json({ items: [], total: 0 });

  const limit  = Math.min(Math.max(parseInt(req.query.limit || '100', 10) || 100, 1), 500);
  const offset = Math.max(parseInt(req.query.offset || '0', 10) || 0, 0);

  try {
    const itemsRes = await pool.query(`
      SELECT name, phone, niche, updated_at
        FROM "${table}"
       WHERE mensagem_enviada = true
         AND updated_at::date = CURRENT_DATE
       ORDER BY updated_at DESC
       LIMIT $1 OFFSET $2;`, [limit, offset]);

    const countRes = await pool.query(`
      SELECT COUNT(*)::int AS total
        FROM "${table}"
       WHERE mensagem_enviada = true
         AND updated_at::date = CURRENT_DATE;`);

    res.json({ items: itemsRes.rows, total: Number(countRes.rows?.[0]?.total || 0) });
  } catch (err) {
    console.error('Erro em /api/sent-today', err);
    res.status(500).json({ error: 'Erro interno ao consultar enviados de hoje' });
  }
});

// Lista clientes
app.get('/api/clients', async (_req, res) => {
  try {
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
            `SELECT auto_run, ia_auto, instance_url, loop_status, last_run_at, daily_limit
               FROM client_settings WHERE slug = $1;`,
            [slug]
          ),
        ]);
        clients.push({
          slug,
          queueCount: Number(countRes.rows[0].count),
          autoRun:    !!cfgRes.rows[0]?.auto_run,
          iaAuto:     !!cfgRes.rows[0]?.ia_auto,
          instanceUrl: cfgRes.rows[0]?.instance_url || null,
          loopStatus:  cfgRes.rows[0]?.loop_status || 'idle',
          lastRunAt:   cfgRes.rows[0]?.last_run_at || null,
          dailyLimit:  cfgRes.rows[0]?.daily_limit ?? DAILY_MESSAGE_COUNT,
        });
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

// Cria novo cliente
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

// KPIs
app.get('/api/stats', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const filaTable   = slug;
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
      totais   = Number(r.rows[0].totais);
      enviados = Number(r.rows[0].enviados);

      const r3 = await pool.query(
        `SELECT name, phone, updated_at
           FROM "${totaisTable}"
          WHERE mensagem_enviada = true
          ORDER BY updated_at DESC
          LIMIT 1;`
      );
      if (r3.rows[0]) {
        lastSentAt   = r3.rows[0].updated_at;
        lastSentName = r3.rows[0].name;
        lastSentPhone= r3.rows[0].phone;
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
      last_sent_at:   lastSentAt,
      last_sent_name: lastSentName,
      last_sent_phone:lastSentPhone,
    });
  } catch (err) {
    console.error('Erro ao obter estatísticas', err);
    res.status(500).json({ error: 'Erro interno ao obter estatísticas' });
  }
});

// Quota diária
app.get('/api/quota', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });
  try {
    let cap = DAILY_MESSAGE_COUNT;
    try {
      const r0 = await pool.query(
        `SELECT COALESCE(daily_limit, $2) AS cap FROM client_settings WHERE slug = $1;`,
        [slug, DAILY_MESSAGE_COUNT]
      );
      if (r0.rows[0]) cap = Number(r0.rows[0].cap) || DAILY_MESSAGE_COUNT;
    } catch {}

    const r = await pool.query(
      `SELECT COUNT(*)::int AS c
         FROM "${slug}_totais"
        WHERE mensagem_enviada = true
          AND updated_at::date = CURRENT_DATE;`
    );
    const sent_today = Number(r.rows?.[0]?.c || 0);
    const remaining  = Math.max(0, cap - sent_today);

    res.json({
      cap,
      sent_today,
      remaining,
      window_start: DAILY_START_TIME,
      window_end:   DAILY_END_TIME,
      now: new Date().toISOString()
    });
  } catch (err) {
    console.error('Erro em /api/quota', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});

// Fila (listar)
app.get('/api/queue', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const exists = await tableExists(slug);
  if (!exists) return res.json({ items: [], total: 0 });

  const page     = parseInt(req.query.page) || 1;
  const pageSize = parseInt(req.query.pageSize) || 25;
  const search   = req.query.search || '';
  const offset   = (page - 1) * pageSize;

  const values = [];
  let whereClause = '';

  if (search) {
    values.push(`%${search}%`);
    whereClause = `WHERE name ILIKE $1 OR phone ILIKE $1`;
  }

  try {
    const itemsRes = await pool.query(`
      SELECT name, phone FROM "${slug}"
      ${whereClause}
      ORDER BY name
      LIMIT $${values.length + 1} OFFSET $${values.length + 2};
    `, [...values, pageSize, offset]);

    const countRes = await pool.query(`SELECT COUNT(*) AS total FROM "${slug}" ${whereClause};`, values);

    res.json({ items: itemsRes.rows, total: Number(countRes.rows[0].total) });
  } catch (err) {
    console.error('Erro ao consultar fila', err);
    res.status(500).json({ error: 'Erro interno ao consultar fila' });
  }
});

// Remoção/Marcação manual a partir da Fila
app.delete('/api/queue', async (req, res) => {
  try {
    const client   = req.body?.client;
    const phone    = req.body?.phone;
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

// Totais (histórico)
app.get('/api/totals', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  const totaisTable = `${slug}_totais`;
  const exists = await tableExists(totaisTable);
  if (!exists) return res.json({ items: [], total: 0 });

  const page     = parseInt(req.query.page) || 1;
  const pageSize = parseInt(req.query.pageSize) || 25;
  const search   = req.query.search || '';
  const sent     = (req.query.sent || 'all').toLowerCase();
  const offset   = (page - 1) * pageSize;

  const conditions = [];
  const params = [];

  if (search) {
    params.push(`%${search}%`);
    conditions.push(`(name ILIKE $${params.length} OR phone ILIKE $${params.length} OR niche ILIKE $${params.length})`);
  }

  if (sent !== 'all') {
    if (sent === 'sim') conditions.push('mensagem_enviada = true');
    else if (sent === 'nao') conditions.push('mensagem_enviada = false');
  }

  const whereClause = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

  try {
    const itemsRes = await pool.query(`
      SELECT name, phone, niche, mensagem_enviada, updated_at
        FROM "${totaisTable}"
      ${whereClause}
      ORDER BY updated_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2};`,
      [...params, pageSize, offset]
    );

    const countRes = await pool.query(`SELECT COUNT(*) AS total FROM "${totaisTable}" ${whereClause};`, params);
    res.json({ items: itemsRes.rows, total: Number(countRes.rows[0].total) });
  } catch (err) {
    console.error('Erro ao consultar totais', err);
    res.status(500).json({ error: 'Erro interno ao consultar totais' });
  }
});

// Adiciona um contato
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

// Importa CSV
app.post('/api/import', upload.single('file'), async (req, res) => {
  try {
    const slug = req.body?.client;
    if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });
    if (!req.file || !req.file.buffer) return res.status(400).json({ error: 'Arquivo não enviado' });

    const text      = req.file.buffer.toString('utf8');
    const firstLine = text.split(/\r?\n/)[0] || '';
    const delim     = detectDelimiter(firstLine);
    const rows      = parseCSV(text, delim);

    if (!rows.length) return res.json({ inserted: 0, skipped: 0, errors: 0 });

    const header = rows[0] || [];
    const idx    = mapHeader(header);
    if (idx.name === -1 || idx.phone === -1) {
      return res.status(400).json({ error: 'Cabeçalho inválido. Precisa conter colunas de nome e telefone.' });
    }

    let inserted = 0, skipped = 0, errors = 0;

    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      if (!r || !r.length) continue;

      const name  = (r[idx.name]  || '').toString().trim();
      const phone = (r[idx.phone] || '').toString().trim();
      const niche = idx.niche !== -1 ? (r[idx.niche] || '').toString().trim() : null;

      if (!name || !phone) { skipped++; continue; }

      try {
        const q = await pool.query('SELECT client_add_contact($1, $2, $3, $4) AS status;', [slug, name, phone, niche]);
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

// Config (get)
app.get('/api/client-settings', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) return res.status(400).json({ error: 'Cliente inválido' });

  try {
    const cfg = await getClientSettings(slug);
    res.json({
      autoRun:           !!cfg.auto_run,
      iaAuto:            !!cfg.ia_auto,
      instanceUrl:        cfg.instance_url || null,
      instanceToken:      cfg.instance_token || '',
      instanceAuthHeader: cfg.instance_auth_header || 'token',
      instanceAuthScheme: cfg.instance_auth_scheme || '',
      loopStatus:         cfg.loop_status || 'idle',
      lastRunAt:          cfg.last_run_at || null,
      dailyLimit:         cfg.daily_limit ?? DAILY_MESSAGE_COUNT,
      messageTemplate:    cfg.message_template || '',   // << novo
    });
  } catch (err) {
    console.error('Erro ao obter configurações', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});

// Config (save)
app.post('/api/client-settings', async (req, res) => {
  const {
    client, autoRun, iaAuto,
    instanceUrl, instanceToken, instanceAuthHeader, instanceAuthScheme,
    dailyLimit,
    messageTemplate, // << novo
  } = req.body || {};
  if (!client || !validateSlug(client)) return res.status(400).json({ error: 'Cliente inválido' });

  try {
    if (instanceUrl) {
      try { new URL(instanceUrl); }
      catch { return res.status(400).json({ error: 'instanceUrl inválida' }); }
    }

    await saveClientSettings(client, {
      autoRun, iaAuto, instanceUrl, instanceToken, instanceAuthHeader, instanceAuthScheme, dailyLimit,
      messageTemplate: typeof messageTemplate === 'string' ? messageTemplate : null,
    });

    const cfg = await getClientSettings(client);
    res.json({ ok: true, settings: cfg });
  } catch (err) {
    console.error('Erro ao salvar configurações', err);
    res.status(500).json({ error: 'Erro interno ao salvar configurações' });
  }
});


// Apagar cliente completo
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

/* ========== Parar loop manualmente ========== */
const stopRequests = new Set();
async function sleepAbortable(ms, slug) {
  const step = 250;
  let elapsed = 0;
  while (elapsed < ms) {
    if (stopRequests.has(slug)) return 'aborted';
    await new Promise(r => setTimeout(r, Math.min(step, ms - elapsed)));
    elapsed += step;
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
  try { await pool.query(`UPDATE client_settings SET loop_status='stopping', last_run_at=NOW() WHERE slug=$1`, [client]); } catch {}
  console.log(`[STOP] Parada solicitada para ${client}`);
  return res.json({ ok: true, message: `Parada solicitada para ${client}` });
});

/* ========== Buscar & salvar LEADS ========== */
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
      const name  = (item.name && String(item.name).trim()) || String(item.phone || '').trim();
      const phone = String(item.phone || '').trim();
      const reg   = (item.region ?? region) || null;
      const nich  = (item.niche  ?? niche ) || null;

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

/* ========== Renomear cliente (slug) ========== */
app.post('/api/rename-client', async (req, res) => {
  const oldSlug = req.body?.oldSlug;
  const newSlug = req.body?.newSlug;

  if (!validateSlug(oldSlug) || !validateSlug(newSlug)) {
    return res.status(400).json({ error: 'Slugs inválidos. Use [a-z0-9_], 1..64 chars.' });
  }
  if (oldSlug === newSlug) {
    return res.status(400).json({ error: 'oldSlug e newSlug são iguais.' });
  }

  try {
    if (runningClients.has(oldSlug)) {
      return res.status(409).json({ error: `Loop em execução para ${oldSlug}. Pare antes de renomear.` });
    }

    const oldExists    = await tableExists(oldSlug);
    const oldTotExists = await tableExists(`${oldSlug}_totais`);
    if (!oldExists || !oldTotExists) return res.status(404).json({ error: `Tabelas de ${oldSlug} não encontradas.` });

    const newExists    = await tableExists(newSlug);
    const newTotExists = await tableExists(`${newSlug}_totais`);
    if (newExists || newTotExists) return res.status(409).json({ error: `Já existem tabelas para ${newSlug}.` });

    await pool.query('BEGIN');
    await pool.query(`ALTER TABLE "${oldSlug}" RENAME TO "${newSlug}";`);
    await pool.query(`ALTER TABLE "${oldSlug}_totais" RENAME TO "${newSlug}_totais";`);

    const cs = await pool.query(`SELECT 1 FROM client_settings WHERE slug = $1;`, [oldSlug]);
    if (cs.rowCount) {
      await pool.query(`UPDATE client_settings SET slug = $1 WHERE slug = $2;`, [newSlug, oldSlug]);
    } else {
      await pool.query(`INSERT INTO client_settings (slug, loop_status, last_run_at) VALUES ($1, 'idle', NOW());`, [newSlug]);
    }
    await pool.query('COMMIT');

    if (progressEmitters.has(oldSlug)) { progressEmitters.set(newSlug, progressEmitters.get(oldSlug)); progressEmitters.delete(oldSlug); }
    if (progressStates.has(oldSlug))   { progressStates.set(newSlug,   progressStates.get(oldSlug));   progressStates.delete(oldSlug); }
    stopRequests.delete(oldSlug);
    runningClients.delete(oldSlug);

    return res.json({ ok: true, oldSlug, newSlug });
  } catch (err) {
    console.error('Erro em /api/rename-client', err);
    try { await pool.query('ROLLBACK'); } catch {}
    return res.status(500).json({ error: 'Erro interno ao renomear cliente' });
  }
});

/* ========== Loop manual (envios) ========== */
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

/* ========== SSE de progresso por cliente ========== */
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
    const onProgress = (payload) => { try { res.write(`data: ${JSON.stringify(payload)}\n\n`); } catch {} };
    em.on('progress', onProgress);

    const ka = setInterval(() => { try { res.write(`event: ping\ndata: {}\n\n`); } catch {} }, 15000);

    req.on('close', () => {
      em.off('progress', onProgress);
      clearInterval(ka);
      try { res.end(); } catch {}
    });
  } catch {
    try { res.end(); } catch {}
  }
});

// Loop de processamento
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

    // start snapshot
    try {
      snapshotStart(clientSlug, totalCount);
      getEmitter(clientSlug).emit('progress', { type: 'start', total: totalCount, at: new Date().toISOString() });
    } catch {}

    const settings   = await getClientSettings(clientSlug);
    const dailyLimit = Number(settings?.daily_limit) > 0 ? Math.floor(Number(settings.daily_limit)) : DAILY_MESSAGE_COUNT;

    let processed = 0;
    let manualStop = false;
    const useIA = typeof opts.iaAutoOverride === 'boolean' ? opts.iaAutoOverride : !!settings.ia_auto;

    // enviados hoje
    let alreadySentToday = 0;
    try {
      const sentTodayRes = await pool.query(
        `SELECT COUNT(*)::int AS c
           FROM "${clientSlug}_totais"
          WHERE mensagem_enviada = true
            AND updated_at::date = CURRENT_DATE;`
      );
      alreadySentToday = Number(sentTodayRes.rows?.[0]?.c || 0);
      console.log(`[${clientSlug}] Enviadas hoje: ${alreadySentToday}/${dailyLimit}`);
    } catch (e) {
      console.warn(`[${clientSlug}] Falha ao contar envios de hoje`, e);
    }

    if (stopRequests.has(clientSlug)) manualStop = true;

    const remainingToday = Math.max(0, dailyLimit - alreadySentToday);
    if (!manualStop && remainingToday <= 0) {
      console.log(`[${clientSlug}] Cota diária (${dailyLimit}) atingida. Encerrando.`);
      try {
        snapshotEnd(clientSlug, processed, { reason: 'daily_quota' });
        getEmitter(clientSlug).emit('progress', { type: 'end', processed, at: new Date().toISOString(), reason: 'daily_quota' });
      } catch {}
      await pool.query(`UPDATE client_settings SET loop_status='idle', last_run_at=NOW() WHERE slug=$1;`, [clientSlug]);
      return { processed, status: 'quota_reached' };
    }

    const scheduleDelays = generateScheduleDelays(dailyLimit, DAILY_START_TIME, DAILY_END_TIME);
    const messageLimit   = Math.min(batchSize, scheduleDelays.length);

    const planCount = Math.min(messageLimit, remainingToday);
    if (!manualStop) {
      try {
        let acc = 0;
        const planned = [];
        for (let i = 0; i < planCount; i++) { acc += scheduleDelays[i]; planned.push(new Date(Date.now() + acc * 1000).toISOString()); }
        getEmitter(clientSlug).emit('progress', { type: 'schedule', planned, remainingToday, cap: dailyLimit });
      } catch {}
    }

    const attemptedPhones = new Set();

    for (let i = 0; i < messageLimit; i++) {
      if (stopRequests.has(clientSlug)) { manualStop = true; break; }
      if (i >= remainingToday) { console.log(`[${clientSlug}] Cota diária atingida durante o ciclo. Encerrando.`); break; }

      const delaySec = scheduleDelays[i];
      if (delaySec > 0) {
        const when = new Date(Date.now() + delaySec * 1000);
        console.log(`[${clientSlug}] Aguardando ${delaySec}s (${when.toTimeString().split(' ')[0]}) para enviar a mensagem ${i + 1}/${messageLimit}.`);
        const slept = await sleepAbortable(delaySec * 1000, clientSlug);
        if (slept === 'aborted') { manualStop = true; break; }
      }

      if (stopRequests.has(clientSlug)) { manualStop = true; break; }

      let whereNotIn = '';
      let params = [];
      if (attemptedPhones.size) {
        const arr = Array.from(attemptedPhones);
        const ph  = arr.map((_, idx) => `$${idx + 1}`).join(',');
        whereNotIn = `WHERE phone NOT IN (${ph})`;
        params = arr;
      }

      // Tenta buscar name, phone, niche da fila; se a coluna 'niche' não existir, cai no fallback.
      let name, phone, niche;
      try {
        const next = await pool.query(`SELECT name, phone, niche FROM "${clientSlug}" ${whereNotIn} ORDER BY name LIMIT 1;`, params);
        if (!next.rows.length) break;
        ({ name, phone, niche } = next.rows[0]);
      } catch {
        const next = await pool.query(`SELECT name, phone FROM "${clientSlug}" ${whereNotIn} ORDER BY name LIMIT 1;`, params);
        if (!next.rows.length) break;
        ({ name, phone } = next.rows[0]);
        try {
          const rN = await pool.query(
            `SELECT niche FROM "${clientSlug}_totais" WHERE phone = $1 ORDER BY updated_at DESC LIMIT 1;`,
            [phone]
          );
          niche = rN.rows?.[0]?.niche || null;
        } catch { niche = null; }
      }

      attemptedPhones.add(phone);

      let sendRes = null;
      let status = 'skipped';
      let shouldMark = false;

      if (!manualStop) {
        if (useIA) {
          sendRes = await runIAForContact({
            client: clientSlug, name, phone, niche,
            instanceUrl: settings.instance_url,
            instanceToken: settings.instance_token,
            instanceAuthHeader: settings.instance_auth_header,
            instanceAuthScheme: settings.instance_auth_scheme,
            messageTemplate: settings.message_template || null, // << novo
          });

          status    = sendRes && sendRes.ok ? 'success' : 'error';
          shouldMark = status === 'success';
        } else {
          status = 'skipped';
          shouldMark = false;
        }
      }

      if (stopRequests.has(clientSlug)) { manualStop = true; }

      if (shouldMark && !manualStop) {
        try { await pool.query(`DELETE FROM "${clientSlug}" WHERE phone = $1;`, [phone]); } catch (err) { console.error('Erro ao deletar da fila', clientSlug, phone, err); }
        try { await pool.query(`UPDATE "${clientSlug}_totais" SET mensagem_enviada = true, updated_at = NOW() WHERE phone = $1;`, [phone]); } catch (err) { console.error('Erro ao atualizar histórico', clientSlug, phone, err); }
        processed++;
      } else if (!manualStop) {
        console.warn(`[${clientSlug}] NÃO marcou como enviada (${status}). Mantendo na fila: ${phone}`);
      }

      try {
        const evt = { type: 'item', name, phone, ok: shouldMark && !manualStop, status: manualStop ? 'stopped' : status, at: new Date().toISOString() };
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
      getEmitter(clientSlug).emit('progress', { type: 'end', processed, at: new Date().toISOString(), ...(manualStop ? { reason: 'manual_stop' } : {}) });
    } catch {}

    if (manualStop) console.log(`[${clientSlug}] Loop encerrado manualmente.`);
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

/* =====================  Supervisão de Conversas (UAZAPI)  ===================== */

// Helper para extrair o systemName do endpoint salvo
function systemNameFromInstanceUrl(u) {
  try {
    const host = new URL(u).hostname || "";
    return (host.split(".")[0] || "").toLowerCase();
  } catch {
    return "";
  }
}

// Resolve a instância correta do cliente (prioriza token, depois systemName)
app.get('/api/instances/resolve', async (req, res) => {
  try {
    const slug = req.query.client;
    if (!slug || !validateSlug(slug)) {
      return res.status(400).json({ error: 'Cliente inválido' });
    }

    const cfg = await getClientSettings(slug);
    const wantToken = (cfg?.instance_token || "").trim();
    const wantSys   = systemNameFromInstanceUrl(cfg?.instance_url || "");

    await refreshInstances(true);
    const all = Array.from(instanceCache.values()) || [];

    // 1) match por token
    if (wantToken) {
      const byToken = all.find((it) => {
        const tok = it?.token || it?.instanceToken || it?.key || "";
        return tok && String(tok).trim() === wantToken;
      });
      if (byToken) {
        return res.json({
          id: byToken.id || byToken._id || byToken.instanceId,
          name: byToken.name || byToken.systemName || '',
          systemName: byToken.systemName || '',
          matchedBy: 'token'
        });
      }
    }

    // 2) match por systemName
    if (wantSys) {
      const want = wantSys.toLowerCase();
      const bySys = all.find((it) => String(it?.systemName || '').toLowerCase() === want)
                 || all.find((it) => String(it?.name || '').toLowerCase().includes(want));
      if (bySys) {
        return res.json({
          id: bySys.id || bySys._id || bySys.instanceId,
          name: bySys.name || bySys.systemName || '',
          systemName: bySys.systemName || '',
          matchedBy: 'system'
        });
      }
    }

    return res.status(404).json({
      error: 'Nenhuma instância compatível com a configuração do cliente',
      wantSystem: wantSys || null,
      matchedBy: null
    });
  } catch (e) {
    console.error('instances/resolve error', e);
    return res.status(500).json({ error: 'Erro interno' });
  }
});

// Lista instâncias
app.get('/api/instances', async (_req, res) => {
  try {
    await refreshInstances(false);
    const entries = Array.from(instanceCache.entries());
    const results = await Promise.all(entries.map(async ([key, inst]) => {
      let status = { connected: false };
      const token = inst.token || inst.instanceToken;
      if (token) {
        try { status = normalizeStatus(await uaz.getInstanceStatus(token)); }
        catch (e) { status = { connected: false, error: String(e.message || e) }; }
      }
      return {
        id: inst.id || key,
        name: inst.name || inst.systemName || inst.instanceName || '',
        systemName: inst.systemName || '',
        avatarUrl: resolveAvatar(inst) || null,
        status,
      };
    }));
    res.json({ instances: results });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Chats
app.get('/api/instances/:id/chats', async (req, res) => {
  try {
    const { id } = req.params;
    const { limit = 50, offset = 0, q } = req.query;
    await refreshInstances(false);
    const token = resolveInstanceToken(id);
    if (!token) return res.status(404).json({ error: 'Instância não encontrada ou sem token' });
    const body = { limit: Number(limit), offset: Number(offset) };
    if (q && String(q).trim() !== '') body.lead_name = `~${q}`;
    const data = await uaz.findChats(token, body);
    const list = pickArrayList(data);
    const chats = list.map((c) => ({ ...c, _chatId: extractChatId(c), avatarUrl: resolveAvatar(c) || null }));
    res.json({ chats, raw: data });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Mensagens
app.get('/api/instances/:id/messages', async (req, res) => {
  try {
    const { id } = req.params;
    let { chatId, limit = 100, all = '0', alts = '' } = req.query;
    if (!chatId) return res.status(400).json({ error: 'chatId é obrigatório' });
    await refreshInstances(false);
    const token = resolveInstanceToken(id);
    if (!token) return res.status(404).json({ error: 'Instância não encontrada ou sem token' });

    const candidates = Array.from(new Set([chatId].concat(String(alts || '').split(',').map(s => s.trim()).filter(Boolean))));
    const PAGE = Math.max(1, Math.min(1000, parseInt(limit, 10) || 100));
    const fetchAll = String(all) === '1' || String(all).toLowerCase() === 'true';

    async function fetchMessagesFor(chatid) {
      if (!fetchAll) {
        const data = await uaz.findMessages(token, { chatid, limit: PAGE });
        return pickArrayList(data);
      }
      const acc = [];
      let offset = 0;
      for (;;) {
        const data = await uaz.findMessages(token, { chatid, limit: PAGE, offset });
        const page = pickArrayList(data);
        if (!page.length) break;
        acc.push(...page);
        if (page.length < PAGE) break;
        offset += PAGE;
        if (offset > 50000) break;
      }
      return acc;
    }

    let final = [];
    for (const cand of candidates) {
      try {
        const msgs = await fetchMessagesFor(cand);
        if (msgs && msgs.length) { final = msgs; break; }
      } catch (_) {}
    }

    final = final.slice().sort((a, b) => {
      const ta = a?.messageTimestamp || a?.timestamp || a?.wa_timestamp || a?.createdAt || 0;
      const tb = b?.messageTimestamp || b?.timestamp || b?.wa_timestamp || b?.createdAt || 0;
      return Number(ta) - Number(tb);
    });

    res.json({ messages: final, raw: { tried: candidates, returnedMessages: final.length } });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Media proxy
app.get('/api/media/proxy', async (req, res) => {
  try {
    const { url } = req.query;
    if (!url) return res.status(400).send('Missing url');
    let target;
    try { target = new URL(url); } catch { return res.status(400).send('Invalid url'); }
    if (!['http:', 'https:'].includes(target.protocol)) return res.status(400).send('Unsupported protocol');

    const baseHost = new URL(UAZAPI_BASE_URL).host;
    const allowedHosts = new Set([baseHost, ...MEDIA_PROXY_ALLOW]);
    const hostAllowed =
      allowedHosts.has(target.host) ||
      target.host.endsWith('.fbcdn.net') ||
      target.host.endsWith('.whatsapp.net') ||
      target.host.endsWith('.whatsapp.com') ||
      target.host.includes('baserow');
    if (!hostAllowed) return res.status(403).send('Host not allowed');

    const headers = {};
    if (req.headers.range) headers.Range = req.headers.range;

    const upstream = await axios.get(target.toString(), {
      responseType: 'stream',
      timeout: 60000,
      headers,
      validateStatus: () => true,
    });
    res.status(upstream.status);
    const h = upstream.headers || {};
    if (h['content-type'])   res.setHeader('Content-Type',   h['content-type']);
    if (h['content-length']) res.setHeader('Content-Length', h['content-length']);
    if (h['accept-ranges'])  res.setHeader('Accept-Ranges',  h['accept-ranges']);
    if (h['content-range'])  res.setHeader('Content-Range',  h['content-range']);
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    upstream.data.on('error', () => res.end());
    upstream.data.pipe(res);
  } catch (e) {
    res.status(500).send('Proxy error');
  }
});

// Export TXT por instância
app.get('/api/instances/:id/export.txt', async (req, res) => {
  try {
    const { id } = req.params;
    await refreshInstances(false);
    const token = resolveInstanceToken(id);
    if (!token) return res.status(404).json({ error: 'Instância não encontrada ou sem token' });

    const pageSize = 100;
    let offset = 0;
    const allChats = [];
    for (;;) {
      const data = await uaz.findChats(token, { limit: pageSize, offset });
      const page = pickArrayList(data);
      if (!page.length) break;
      allChats.push(...page);
      if (page.length < pageSize) break;
      offset += pageSize;
    }

    const MAX_PER_CHAT = 1000;
    const nameOrJid = (chat) => chat?.lead_name || chat?.name || extractChatId(chat) || 'chat';
    const labelForMsg = (m) => (m?.fromMe === true || m?.sender?.fromMe === true || m?.me === true) ? 'Usuário' : 'Cliente';

    let output = '';
    for (const chat of allChats) {
      const chatId = extractChatId(chat);
      if (!chatId) continue;

      output += `==============================\n`;
      output += `CHAT: ${nameOrJid(chat)} (${chatId})\n`;
      output += `==============================\n`;

      const data = await uaz.findMessages(token, { chatid: chatId, limit: MAX_PER_CHAT });
      const msgs = pickArrayList(data);
      for (const m of msgs) {
        const ts   = m?.messageTimestamp || m?.timestamp || m?.wa_timestamp || m?.createdAt || m?.date || '';
        const text = m?.text || m?.body || m?.message || m?.content?.text || m?.content || JSON.stringify(m);
        const who  = labelForMsg(m);
        output += `[${ts}] (${who}): ${typeof text === 'string' ? text : JSON.stringify(text)}\n`;
      }
      output += '\n';
    }

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="export-${id}.txt"`);
    res.send(output || 'Nenhuma mensagem.');
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

/**
 * POST /api/instances/:id/export-analysis
 *
 * Envia todas as mensagens novas (desde a última análise) de uma instância UAZAPI para
 * o modelo ChatGPT via OpenAI API e retorna sugestões de melhorias para a assistente Luna.
 *
 * Este endpoint espera que o parâmetro `client` seja passado via querystring ou body,
 * indicando o slug do cliente. Ele utiliza a coluna `analysis_last_msg_ts` em
 * client_settings para evitar enviar mensagens já analisadas. Após a análise,
 * atualiza essa coluna com o timestamp da mensagem mais recente enviada.
 *
 * Para funcionar, defina a variável de ambiente `OPENAI_API_KEY` com a chave da API
 * da OpenAI. Se não houver chave configurada, retorna erro.
 */
app.post('/api/instances/:id/export-analysis', async (req, res) => {
  try {
    const { id } = req.params;
    // Aceita slug via query ou body
    const slug = (req.query?.client || req.body?.client || '').toString();
    if (!slug || !validateSlug(slug)) {
      return res.status(400).json({ error: 'Cliente inválido' });
    }
    // Log básico do início da análise para diagnosticar entradas
    console.log(`[export-analysis] Início — client=${slug}, instance=${id}`);

    // Garante que existe uma chave da OpenAI configurada
    const openaiKey = process.env.OPENAI_API_KEY;
    // Se não houver chave, retornaremos uma mensagem informativa em vez de erro 500
    const analysisEnabled = !!openaiKey;

    // Busca timestamp da última mensagem analisada para o cliente
    let lastTs = null;
    try {
      const r = await pool.query(
        `SELECT analysis_last_msg_ts FROM client_settings WHERE slug = $1`,
        [slug]
      );
      lastTs = r.rows?.[0]?.analysis_last_msg_ts || null;
    } catch {}

    console.log(`[export-analysis] client=${slug} lastTs=${lastTs}`);

    // Resolve token da instância
    await refreshInstances(false);
    const token = resolveInstanceToken(id);
    if (!token) {
      return res.status(404).json({ error: 'Instância não encontrada ou sem token' });
    }

    console.log(`[export-analysis] Token resolvido para ${id}: ${token ? 'OK' : 'NULO'}`);

    // ==================== Início da nova lógica de análise ====================
    // Registramos o início da análise no log
    appendLog(`🟢 Início da análise - Cliente: ${slug}`);
    const startTime = Date.now();

    // 1) Busca e filtra os chats recentes
    // Busca todos os chats da instância (paginação)
    const pageSize = 100;
    let offsetChats = 0;
    const chats = [];
    for (;;) {
      const data = await uaz.findChats(token, { limit: pageSize, offset: offsetChats });
      const page = pickArrayList(data);
      if (!page.length) break;
      chats.push(...page);
      if (page.length < pageSize) break;
      offsetChats += pageSize;
    }
    // Ordena chats pelo timestamp da última mensagem (desc) e limita a ANALYSIS_MAX_CHATS
    const chatLastTs = (c) =>
      c?.wa_lastTimestamp ||
      c?.lastMessageTimestamp ||
      c?.updatedAt ||
      c?.createdAt ||
      0;
    chats.sort((a, b) => {
      const at = Number(chatLastTs(a)) || 0;
      const bt = Number(chatLastTs(b)) || 0;
      return bt - at;
    });
    const selectedChats = chats.slice(0, ANALYSIS_MAX_CHATS);

    // 2) Para cada chat, coleta as últimas ANALYSIS_PER_CHAT_LIMIT mensagens e filtra as novas
    let allMessages = [];
    let maxTs = lastTs ? new Date(lastTs).getTime() : 0;
    for (const chat of selectedChats) {
      const chatId = extractChatId(chat);
      if (!chatId) continue;
      // Pega as últimas N mensagens do chat
      const data = await uaz.findMessages(token, {
        chatid: chatId,
        limit: ANALYSIS_PER_CHAT_LIMIT,
        offset: 0,
      });
      const msgs = pickArrayList(data);
      // Ordena pelas mais antigas primeiro (caso a API retorne ordem inversa)
      msgs.sort((a, b) => {
        const ta =
          a?.messageTimestamp ||
          a?.timestamp ||
          a?.wa_timestamp ||
          a?.createdAt ||
          a?.date ||
          0;
        const tb =
          b?.messageTimestamp ||
          b?.timestamp ||
          b?.wa_timestamp ||
          b?.createdAt ||
          b?.date ||
          0;
        return Number(ta) - Number(tb);
      });
      for (const msg of msgs) {
        // extrai timestamp numérico
        const rawTs =
          msg?.messageTimestamp ||
          msg?.timestamp ||
          msg?.wa_timestamp ||
          msg?.createdAt ||
          msg?.date ||
          null;
        let numTs = null;
        if (rawTs) {
          if (typeof rawTs === 'string' && /^\d+$/.test(rawTs)) {
            const n = Number(rawTs);
            numTs = n < 10 ** 12 ? n * 1000 : n;
          } else {
            const n = Number(rawTs);
            if (Number.isFinite(n)) {
              numTs = n < 10 ** 12 ? n * 1000 : n;
            } else {
              const d = new Date(rawTs);
              const ms = d.getTime();
              numTs = Number.isNaN(ms) ? null : ms;
            }
          }
        }
        if (numTs == null) continue;
        if (lastTs && numTs <= new Date(lastTs).getTime()) {
          continue; // ignora antigas
        }
        allMessages.push({ timestamp: numTs, msg });
        if (numTs > maxTs) maxTs = numTs;
      }
    }

    if (!allMessages.length) {
      appendLog('ℹ️ Nenhuma mensagem nova para analisar.');
      console.log(`[export-analysis] Nenhuma nova mensagem encontrada para ${slug}`);
      return res.json({ ok: true, suggestions: '', info: 'Nenhuma mensagem nova para analisar.' });
    }

    // Ordena todas as mensagens novas por timestamp asc
    allMessages.sort((a, b) => a.timestamp - b.timestamp);

    // 3) Constrói linhas de transcrição compactas
    const lines = allMessages.map(({ msg }) => toTranscriptLine(msg)).filter(Boolean);

    // 4) Define prompts
    // Permite sobrescrever o prompt do papel "system" via variável de ambiente OPENAI_SYSTEM_PROMPT (já lida em SYSTEM_PROMPT_OVERRIDE).
    // Caso queira personalizar o comportamento global do analista, defina OPENAI_SYSTEM_PROMPT no .env ou no Railway.
    const systemPrompt = SYSTEM_PROMPT_OVERRIDE || DEFAULT_SYSTEM_PROMPT;
    // Monta a introdução do usuário de forma dinâmica, indicando o número de mensagens e o slug do cliente.
    const userIntro = `A seguir está a transcrição (resumida) de ${lines.length} mensagens recentes do cliente ${slug}. Analise o conteúdo e proponha melhorias.`;

    // tokens fixos: system + intro + margem
    const baseTokens = approxTokens(systemPrompt) + approxTokens(userIntro) + 50;

    // 5) Chunking baseado no orçamento de tokens
    const chunks = [];
    let current = [];
    let currentTokens = baseTokens;
    for (const line of lines) {
      const t = approxTokens(line) + 1;
      if (current.length && currentTokens + t > ANALYSIS_INPUT_BUDGET) {
        chunks.push(current.join('\n'));
        current = [line];
        currentTokens = baseTokens + approxTokens(line);
      } else {
        current.push(line);
        currentTokens += t;
      }
    }
    if (current.length) {
      chunks.push(current.join('\n'));
    }

    appendLog(`→ Coletados ${selectedChats.length} chats e ${lines.length} mensagens (após filtro). Lotes: ${chunks.length}.`);

    let suggestions = '';
    let infoMessage = '';

    if (analysisEnabled) {
      const key = openaiKey;
      const results = [];
      for (let i = 0; i < chunks.length; i++) {
        const content = `${userIntro}\n\n${chunks[i]}`;
        // Constrói o payload dinamicamente, ajustando nome de parâmetro de tokens e temperatura
        const payload = {
          model: ANALYSIS_MODEL,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content },
          ],
          n: 1,
        };
        // Detecta se o modelo é de raciocínio (GPT‑5, GPT‑4o, O-series) para ajustar parâmetros suportados
        const lowerModel = String(ANALYSIS_MODEL || '').toLowerCase();
        const isReasoningModel = /gpt-5|gpt-4o|\bo[123]\b|\bomni/i.test(lowerModel);
        if (isReasoningModel) {
          // Modelos de raciocínio usam max_completion_tokens em vez de max_tokens
          payload.max_completion_tokens = ANALYSIS_OUTPUT_BUDGET;
          // Especifica o formato da resposta para texto simples e o esforço de raciocínio
          payload.response_format = { type: 'text' };
          payload.reasoning_effort = process.env.OPENAI_REASONING_EFFORT || 'low';
        } else {
          // Modelos clássicos (GPT‑3.5/4) usam max_tokens
          payload.max_tokens = ANALYSIS_OUTPUT_BUDGET;
          // Define temperatura para modelos que suportam (padrão 0.5 ou configurável)
          const tempEnv = process.env.OPENAI_TEMPERATURE;
          const parsedTemp = tempEnv !== undefined ? Number(tempEnv) : 0.5;
          if (!Number.isNaN(parsedTemp)) payload.temperature = parsedTemp;
        }
        try {
          const response = await axios.post('https://api.openai.com/v1/chat/completions', payload, {
            headers: {
              Authorization: `Bearer ${key}`,
              'Content-Type': 'application/json',
            },
            timeout: 45000,
          });
          const text = response?.data?.choices?.[0]?.message?.content || '';
          // Loga a resposta bruta (encurtada) para depuração
          const rawSnippet = text ? text.slice(0, 120).replace(/\n/g, ' ') + (text.length > 120 ? '…' : '') : '(resposta vazia)';
          appendLog(`📦 Retorno do lote ${i + 1}: ${rawSnippet}`);
          if (text) {
            results.push(`### Lote ${i + 1}\n${text.trim()}`);
            appendLog(`✅ Lote ${i + 1} concluído (tamanho aprox. chunk: ${chunks[i].length} chars).`);
          } else {
            appendLog(`⚠️ Lote ${i + 1} retornou texto vazio.`);
          }
        } catch (err) {
          const msgErr = err.response?.data?.error?.message || err.message || err.toString();
          console.error('Erro ao chamar OpenAI', msgErr);
          appendLog(`❌ Falha no lote ${i + 1}: ${msgErr}`);
        }
      }
      suggestions = results.join('\n\n---\n\n');
      if (!suggestions) {
        infoMessage = 'Sem sugestões geradas (modelo pode ter retornado vazio).';
        console.log(`[export-analysis] Modelo retornou vazio para ${slug} — nenhum texto nos lotes`);
      }
    } else {
      infoMessage = 'Análise indisponível: OPENAI_API_KEY não configurada.';
      appendLog('❌ Análise indisponível: OPENAI_API_KEY não configurada.');
    }

    // 6) Atualiza last analysis timestamp com a data mais recente processada
    try {
      await pool.query(
        `UPDATE client_settings SET analysis_last_msg_ts = $2 WHERE slug = $1`,
        [slug, new Date(maxTs).toISOString()]
      );
    } catch (e) {
      console.error('Erro ao atualizar analysis_last_msg_ts', slug, e);
      appendLog(`⚠️ Falha ao atualizar analysis_last_msg_ts para ${slug}: ${e.message || e}`);
    }

    // Loga fim da análise
    const elapsed = Date.now() - startTime;
    appendLog(`🏁 Fim da análise — ${chunks.length} lotes, tempo total ${elapsed}ms`);

    return res.json({ ok: true, suggestions, info: infoMessage });
    // ==================== Fim da nova lógica de análise ====================
  } catch (err) {
    console.error('Erro em export-analysis', err);
    return res.status(500).json({ error: String(err.message || err) });
  }
});

// Button reply (Native Flow)
app.post('/api/instances/:id/interactive/reply', async (req, res) => {
  try {
    const { id } = req.params;
    const body = req.body || {};
    const { chatid, button_id, display_text, original_message_id } = body;
    if (!chatid || !button_id) {
      return res.status(400).json({ error: 'chatid e button_id são obrigatórios' });
    }
    if (UAZAPI_INTERACTIVE_REPLY_PATH) {
      await refreshInstances(false);
      const token = resolveInstanceToken(id);
      if (!token) return res.status(404).json({ error: 'Instância não encontrada ou sem token' });
      const pay = { chatid, button_id, display_text: display_text || '', original_message_id: original_message_id || '' };
      try { await uaz.postWithToken(token, UAZAPI_INTERACTIVE_REPLY_PATH, pay); } catch (e) { console.error(e); }
    }
    return res.status(202).json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

/* =====================  Catch-all  ===================== */
app.get('*', (_req, res) => res.status(404).json({ error: 'Not found' }));

/* =====================  Boot  ===================== */
if (process.env.PORT === '5432') {
  console.warn('[CONFIG] Você definiu PORT=5432 nas variáveis. Remova essa variável no Railway; a plataforma fornece PORT automaticamente.');
}
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
