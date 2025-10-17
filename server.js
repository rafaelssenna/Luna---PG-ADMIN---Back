// server.js

// Servidor Express para a aplicação Luna

require('dotenv').config();

const express = require('express');
const { Pool } = require('pg');
const EventEmitter = require('events');
const multer = require('multer');
const path = require('path');
const app = express();

/* ======================  Disparo diário (30 mensagens)  ====================== */
// Esta seção define a lógica de agendamento para disparar até 30 mensagens por dia
// no intervalo das 08:00 às 17:30. Os horários são sorteados de maneira aleatória
// e o loop de envios aguarda os tempos sorteados antes de processar cada item da fila.

// Quantidade máxima de mensagens a enviar por dia
const DAILY_MESSAGE_COUNT = 30;
// Horário de início e fim (formato HH:MM:SS) para os disparos diários
const DAILY_START_TIME = '08:00:00';
const DAILY_END_TIME = '17:30:00';

/**
 * Converte uma string HH:MM:SS em segundos desde 00:00:00.
 *
 * @param {string} hms - hora em formato HH:MM:SS
 * @returns {number} - segundos desde 00:00:00
 */
function hmsToSeconds(hms) {
  const parts = String(hms || '').split(':').map((p) => parseInt(p, 10) || 0);
  const h = parts[0] || 0;
  const m = parts[1] || 0;
  const s = parts[2] || 0;
  return h * 3600 + m * 60 + s;
}

/**
 * Gera um array de delays (em segundos) entre cada disparo, a partir do horário
 * atual, respeitando o intervalo configurado. O primeiro delay inclui o tempo
 * até o horário de início, se necessário.
 *
 * @param {number} count - quantidade de mensagens que se deseja enviar
 * @param {string} startStr - hora de início (HH:MM:SS)
 * @param {string} endStr - hora de fim (HH:MM:SS)
 * @returns {number[]} - array de segundos a aguardar antes de cada envio
 */
function generateScheduleDelays(count, startStr, endStr) {
  const now = new Date();
  const nowSec = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();
  const startSec = hmsToSeconds(startStr);
  const endSec = hmsToSeconds(endStr);

  // Define o início efetivo: se já passou do horário de início, começa agora; caso contrário, aguarda até o horário de início
  const effectiveStart = Math.max(nowSec, startSec);

  // Se já passou do horário de término, não há tempo para enviar mensagens hoje
  if (endSec <= effectiveStart) {
    return [];
  }

  // Janela disponível em segundos
  const span = endSec - effectiveStart;
  // Limita a quantidade de mensagens ao intervalo disponível (um segundo por mensagem)
  const msgCount = Math.min(count, span);

  // Gera offsets únicos dentro do intervalo e os ordena
  const offsets = new Set();
  while (offsets.size < msgCount) {
    // Gera valor entre 0 e span (inclusive)
    const off = Math.floor(Math.random() * (span + 1));
    offsets.add(off);
  }
  const sortedOffsets = Array.from(offsets).sort((a, b) => a - b);

  // Constrói os delays: o primeiro inclui o tempo até effectiveStart, os demais são diferenças entre offsets
  const delays = [];
  let prevOffset = 0;
  for (let i = 0; i < sortedOffsets.length; i++) {
    const off = sortedOffsets[i];
    if (i === 0) {
      // Aguarda (effectiveStart - nowSec) para alinhar ao início efetivo, mais o primeiro offset
      delays.push((effectiveStart - nowSec) + off);
    } else {
      // Diferença entre este offset e o anterior
      delays.push(off - prevOffset);
    }
    prevOffset = off;
  }
  return delays;
}

/* ======================  CORS  ====================== */
// - CORS_ANY=true  => libera qualquer origem (sem credenciais)
// - CORS_ORIGINS   => lista separada por vírgulas com origens específicas
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
  // Se um dia usar cookies/sessão, habilite:
  // res.setHeader('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }
  next();
});

/* =================================================== */

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
});

app.use(express.json());
app.use(express.static(path.join(__dirname)));

const upload = multer({ storage: multer.memoryStorage() });

function validateSlug(slug) {
  return /^cliente_[a-z0-9_]+$/.test(slug);
}

/* ======================  Config por cliente (server-side)  ====================== */

const runningClients = new Set(); // trava por cliente (evita concorrer)

/**
 * Emissores por cliente (SSE).
 */
const progressEmitters = new Map();
function getEmitter(slug) {
  if (!progressEmitters.has(slug)) progressEmitters.set(slug, new EventEmitter());
  return progressEmitters.get(slug);
}

/**
 * Estado de progresso (para "replay" quando o usuário abre o SSE após o início).
 * Mantém o último "start", a cauda de até 200 "item"s e o último "end".
 * Estrutura: { lastStart, items: [...], lastEnd }
 */
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
function snapshotEnd(slug, processed) {
  const st = progressStates.get(slug);
  if (!st) return;
  st.lastEnd = { type: 'end', processed, at: new Date().toISOString() };
}

async function ensureSettingsTable() {
  await pool.query(`
CREATE TABLE IF NOT EXISTS client_settings (
  slug TEXT PRIMARY KEY,
  auto_run BOOLEAN DEFAULT false,
  ia_auto BOOLEAN DEFAULT false,
  instance_url TEXT,
  -- novos campos (podem não existir em instalações antigas)
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

/* ======================  IA (UAZAPI URL-ONLY FLEX) ====================== */

// [CHANGE] Corrigido para retornar **string** em E.164 com '+'
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
  authHeader: process.env.UAZAPI_AUTH_HEADER || 'Authorization',
  authScheme: process.env.UAZAPI_AUTH_SCHEME ?? 'Bearer ',
  phoneField: process.env.UAZAPI_PHONE_FIELD || 'number',
  textField: process.env.UAZAPI_TEXT_FIELD || 'text',
  digitsOnly: (process.env.UAZAPI_PHONE_DIGITS_ONLY || 'true') === 'true',
  payloadStyle: (process.env.UAZAPI_PAYLOAD_STYLE || 'auto').toLowerCase(), // auto|json|form|query|template
  methodPref: (process.env.UAZAPI_METHOD || 'auto').toLowerCase(), // auto|get|post
  extra: (() => {
    try {
      return JSON.parse(process.env.UAZAPI_EXTRA || '{}');
    } catch {
      return {};
    }
  })(),
  template: process.env.MESSAGE_TEMPLATE || 'Olá {NAME}, aqui é do {CLIENT}.',
};

async function httpSend({ url, method, headers, body }) {
  if (typeof fetch === 'function') {
    return fetch(url, { method, headers, body });
  }
  try {
    const nf = require('node-fetch'); // se não existir, cai no catch
    if (nf) {
      return nf(url, { method, headers, body });
    }
  } catch {}
  return new Promise((resolve, reject) => {
    try {
      const urlObj = new URL(url);
      const httpMod = urlObj.protocol === 'https:' ? require('https') : require('http');
      const req = httpMod.request(
        {
          hostname: urlObj.hostname,
          port: urlObj.port || (urlObj.protocol === 'https:' ? 443 : 80),
          path: urlObj.pathname + urlObj.search,
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
              json: async () => {
                try {
                  return JSON.parse(data);
                } catch {
                  return { raw: data };
                }
              },
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

function buildUazRequest(instanceUrl, { e164, digits, text }) {
  const hasTpl = /\{(NUMBER|PHONE_E164|TEXT)\}/.test(instanceUrl);
  const hasQueryNumber = /\?[^]*=/.test(instanceUrl); // [ADD] detecção mais ampla
  const style = UAZ.payloadStyle;
  const methodEnv = UAZ.methodPref;
  const methodAuto =
    methodEnv === 'get' ||
    (methodEnv === 'auto' && (hasTpl || hasQueryNumber))
      ? 'GET'
      : 'POST';

  if (style === 'template' || hasTpl) {
    let url = instanceUrl
      .replace(/\{NUMBER\}/g, digits)
      .replace(/\{PHONE_E164\}/g, encodeURIComponent(e164))
      .replace(/\{TEXT\}/g, encodeURIComponent(text));
    const method = methodEnv === 'post' ? 'POST' : 'GET';
    const headers = method === 'POST' ? { 'Content-Type': 'application/json' } : {};
    return { url, method, headers, body: method === 'POST' ? JSON.stringify({}) : undefined };
  }

  if (style === 'query' || hasQueryNumber) {
    const u = new URL(instanceUrl);
    u.searchParams.set(UAZ.phoneField, UAZ.digitsOnly ? digits : e164);
    u.searchParams.set(UAZ.textField, text);
    Object.entries(UAZ.extra || {}).forEach(([k, v]) => {
      if (['string', 'number', 'boolean'].includes(typeof v)) u.searchParams.set(k, String(v));
    });
    const method = methodEnv === 'post' ? 'POST' : 'GET';
    const headers = method === 'POST' ? { 'Content-Type': 'application/json' } : {};
    return {
      url: u.toString(),
      method,
      headers,
      body: method === 'POST' ? JSON.stringify({}) : undefined,
    };
  }

  if (style === 'form') {
    const form = new URLSearchParams();
    form.set(UAZ.phoneField, UAZ.digitsOnly ? digits : e164);
    form.set(UAZ.textField, text);
    Object.entries(UAZ.extra || {}).forEach(([k, v]) =>
      form.set(k, typeof v === 'object' ? JSON.stringify(v) : String(v))
    );
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
    return { url: instanceUrl, method: 'POST', headers, body: form.toString() };
  }

  const payload = { ...UAZ.extra };
  payload[UAZ.phoneField] = UAZ.digitsOnly ? digits : e164;
  payload[UAZ.textField] = text;
  const headers = { 'Content-Type': 'application/json' };
  return { url: instanceUrl, method: 'POST', headers, body: JSON.stringify(payload) };
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
    const e164 = normalizePhoneE164BR(phone);       // [CHANGE] agora é "+55..."
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
      console.log('[UAZAPI] request', {
        url: req.url,
        method: req.method,
        headers: Object.fromEntries(
          Object.entries(req.headers || {}).map(([k, v]) => [
            k,
            k.toLowerCase().includes('token') || k.toLowerCase().includes('authorization')
              ? '***'
              : v,
          ])
        ),
        hasBody: !!req.body,
      });
    }

    const resp = await httpSend(req);
    let body;
    try {
      body = await resp.json();
    } catch {
      body = await resp.text();
    }
    if (!resp.ok) console.error('UAZAPI FAIL', { status: resp.status, body });
    return { ok: resp.ok, status: resp.status, body };
  } catch (err) {
    console.error('UAZAPI ERROR', instanceUrl, err);
    return { ok: false, error: String(err) };
  }
}

/* ======================================================================= */

/**
 * Executa o loop de processamento para um cliente.
 */
async function runLoopForClient(clientSlug, opts = {}) {
  if (!validateSlug(clientSlug)) {
    throw new Error('Slug inválido');
  }
  if (runningClients.has(clientSlug)) {
    return { processed: 0, status: 'already_running' };
  }
  runningClients.add(clientSlug);
  const batchSize = parseInt(process.env.LOOP_BATCH_SIZE, 10) || opts.batchSize || DAILY_MESSAGE_COUNT;
  try {
    // Seta status para running no início da execução
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

    let totalCount = 0;
    try {
      const _cnt = await pool.query(`SELECT COUNT(*) AS count FROM "${clientSlug}";`);
      totalCount = Number(_cnt.rows?.[0]?.count || 0);
    } catch {}

    // Snapshot de início para SSE
    try {
      snapshotStart(clientSlug, totalCount);
      getEmitter(clientSlug).emit('progress', {
        type: 'start',
        total: totalCount,
        at: new Date().toISOString(),
      });
    } catch {}

    const settings = await getClientSettings(clientSlug);
    let processed = 0;
    const useIA = typeof opts.iaAutoOverride === 'boolean' ? opts.iaAutoOverride : !!settings.ia_auto;

    // [ADD] Cota diária: conta enviados hoje e calcula o restante do dia (cap = 30)
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

    const remainingToday = Math.max(0, DAILY_MESSAGE_COUNT - alreadySentToday);
    if (remainingToday <= 0) {
      console.log(`[${clientSlug}] Cota diária (${DAILY_MESSAGE_COUNT}) atingida. Encerrando.`);
      try {
        snapshotEnd(clientSlug, processed);
        getEmitter(clientSlug).emit('progress', {
          type: 'end',
          processed,
          at: new Date().toISOString(),
          reason: 'daily_quota'
        });
      } catch {}
      await pool.query(
        `UPDATE client_settings SET loop_status='idle', last_run_at=NOW() WHERE slug=$1;`,
        [clientSlug]
      );
      return { processed, status: 'quota_reached' };
    }

    // Gera os delays para os envios do dia
    const scheduleDelays = generateScheduleDelays(
      DAILY_MESSAGE_COUNT,
      DAILY_START_TIME,
      DAILY_END_TIME
    );
    const messageLimit = Math.min(batchSize, scheduleDelays.length);

    // [ADD] Anuncia via SSE a grade planejada para hoje (limitada pela cota restante)
    const planCount = Math.min(messageLimit, remainingToday);
    try {
      let acc = 0;
      const planned = [];
      for (let i = 0; i < planCount; i++) {
        acc += scheduleDelays[i];
        planned.push(new Date(Date.now() + acc * 1000).toISOString());
      }
      getEmitter(clientSlug).emit('progress', {
        type: 'schedule',
        planned,
        remainingToday,
        cap: DAILY_MESSAGE_COUNT
      });
    } catch {}

    // [ADD] Evitar re-tentar o mesmo telefone no mesmo ciclo se falhar/skipped
    const attemptedPhones = new Set();

    for (let i = 0; i < messageLimit; i++) {
      // [ADD] Proteção adicional: corta se atingir a cota restante no meio do ciclo
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
        await new Promise((resolve) => setTimeout(resolve, delaySec * 1000));
      }

      // [ADD] Seleciona ignorando os que já tentamos neste ciclo (evita loop no mesmo contato)
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

      if (next.rows.length === 0) {
        break;
      }

      const { name, phone } = next.rows[0];
      attemptedPhones.add(phone);

      let sendRes = null;
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
        if (!sendRes.ok) {
          console.warn(
            `[${clientSlug}] IA retornou erro para ${phone}. NÃO será marcado como enviado.`
          );
        }
      }

      // [ADD] Determina status e se deve marcar
      let status = 'skipped';
      if (useIA) status = sendRes && sendRes.ok ? 'success' : 'error';
      const shouldMark = status === 'success';

      // [ADD] Retira da fila e marca enviados **apenas** em caso de sucesso real
      if (shouldMark) {
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
      } else {
        // Mantém na fila para tentar depois
        console.warn(`[${clientSlug}] NÃO marcou como enviada (${status}). Mantendo na fila: ${phone}`);
      }

      processed++;
      if (!shouldMark) processed--; // [ADD] conta apenas sucessos

      try {
        const evt = {
          type: 'item',
          name,
          phone,
          ok: shouldMark,
          status,                 // [ADD] status coerente: success | error | skipped
          at: new Date().toISOString(),
        };
        snapshotPush(clientSlug, evt);
        getEmitter(clientSlug).emit('progress', evt);
      } catch {}
    }

    // Ao final, atualiza o status para idle
    await pool.query(
      `INSERT INTO client_settings (slug, loop_status, last_run_at)
       VALUES ($1, 'idle', NOW())
       ON CONFLICT (slug) DO UPDATE SET loop_status = 'idle', last_run_at = NOW()`,
      [clientSlug]
    );
    try {
      snapshotEnd(clientSlug, processed);
      getEmitter(clientSlug).emit('progress', {
        type: 'end',
        processed,
        at: new Date().toISOString(),
      });
    } catch {}
    return { processed, status: 'ok' };
  } catch (err) {
    console.error('Erro no runLoopForClient', clientSlug, err);
    return { processed: 0, status: 'error' };
  } finally {
    runningClients.delete(clientSlug);
  }
}

/* ============================================================================ */

/* ======== Helpers ======== */
async function tableExists(tableName) {
  const { rows } = await pool.query('SELECT to_regclass($1) AS reg;', [`public.${tableName}`]);
  return !!rows[0].reg;
}

/* ======== CSV Utilitários ======== */
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
  let row = [],
    val = '',
    inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') {
        val += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === '\r') continue;
    if (ch === '\n' && !inQuotes) {
      row.push(val);
      rows.push(row);
      row = [];
      val = '';
      continue;
    }
    if (ch === delim && !inQuotes) {
      row.push(val);
      val = '';
      continue;
    }
    val += ch;
  }
  if (val.length > 0 || row.length > 0) {
    row.push(val);
    rows.push(row);
  }
  return rows;
}
function mapHeader(headerCells) {
  const idx = { name: -1, phone: -1, niche: -1 };
  const names = headerCells.map((h) => norm(h));
  const isId = (h) => ['id', 'identificador', 'codigo', 'código'].includes(h);
  const nameKeys = new Set([
    'nome',
    'name',
    'full_name',
    'fullname',
    'contato',
    'empresa',
    'nomefantasia',
    'razaosocial',
  ]);
  const phoneKeys = new Set([
    'telefone',
    'numero',
    'número',
    'phone',
    'whatsapp',
    'celular',
    'mobile',
    'telemovel',
  ]);
  const nicheKeys = new Set(['nicho', 'niche', 'segmento', 'categoria', 'industry']);
  names.forEach((h, i) => {
    if (isId(h)) return;
    if (idx.name === -1 && nameKeys.has(h)) idx.name = i;
    if (idx.phone === -1 && phoneKeys.has(h)) idx.phone = i;
    if (idx.niche === -1 && nicheKeys.has(h)) idx.niche = i;
  });
  return idx;
}

/* ================================= */

/** Healthcheck */
app.get('/api/healthz', (_req, res) => {
  res.json({ up: true });
});

/** Lista clientes (slug e fila) + flags salvas */
app.get('/api/clients', async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT table_name
         FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          AND table_name LIKE 'cliente\\_%'
          AND table_name NOT LIKE '%\\_totais';`
    );
    const tables = result.rows.map((r) => r.table_name);
    const clients = [];
    for (const slug of tables) {
      try {
        const [countRes, cfgRes] = await Promise.all([
          // Conta quantos registros existem na fila deste cliente
          pool.query(`SELECT COUNT(*) AS count FROM "${slug}";`),
          // Obtém configurações adicionais, incluindo status do loop e data da última execução
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
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Slug inválido' });
  }
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
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  const filaTable = `${slug}`;
  const totaisTable = `${slug}_totais`;
  try {
    const [hasFila, hasTotais] = await Promise.all([
      tableExists(filaTable),
      tableExists(totaisTable),
    ]);

    let totais = 0,
      enviados = 0,
      fila = 0,
      lastSentAt = null,
      lastSentName = null,
      lastSentPhone = null;

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

    return res.json({
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

/** Fila (pagina/filtra) — blindado para tabela ausente */

/** Quota diária (cap, enviados hoje, restantes, janela) */
app.get('/api/quota', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
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
    return res.json({
      cap,
      sent_today,
      remaining,
      window_start: DAILY_START_TIME,
      window_end: DAILY_END_TIME,
      now: new Date().toISOString()
    });
  } catch (err) {
    console.error('Erro em /api/quota', err);
    return res.status(500).json({ error: 'Erro interno' });
  }
});

app.get('/api/queue', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  const exists = await tableExists(slug);
  if (!exists) {
    return res.json({ items: [], total: 0 });
  }

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

    // Dispara um evento de progresso "simulado" para refletir ação manual
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

    return res.json({ ok: true });
  } catch (err) {
    console.error('Erro em DELETE /api/queue', err);
    return res.status(500).json({ error: 'Erro interno' });
  }
});

/** Históricos (pagina/filtra) — blindado para tabela ausente */
app.get('/api/totals', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  const totaisTable = `${slug}_totais`;
  const exists = await tableExists(totaisTable);
  if (!exists) {
    return res.json({ items: [], total: 0 });
  }

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
  if (!client || !validateSlug(client)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  if (!name || !phone) {
    return res.status(400).json({ error: 'Nome e telefone são obrigatórios' });
  }
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
    if (!slug || !validateSlug(slug)) {
      return res.status(400).json({ error: 'Cliente inválido' });
    }
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ error: 'Arquivo não enviado' });
    }

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

    let inserted = 0,
      skipped = 0,
      errors = 0;

    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      if (!r || !r.length) continue;

      const name = (r[idx.name] || '').toString().trim();
      const phone = (r[idx.phone] || '').toString().trim();
      const niche = idx.niche !== -1 ? (r[idx.niche] || '').toString().trim() : null;

      if (!name || !phone) {
        skipped++;
        continue;
      }

      try {
        const q = await pool.query('SELECT client_add_contact($1, $2, $3, $4) AS status;', [
          slug,
          name,
          phone,
          niche,
        ]);
        const status = q.rows[0]?.status || 'inserted';
        if (status === 'inserted') inserted++;
        else skipped++;
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
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
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
  if (!client || !validateSlug(client)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  try {
    if (instanceUrl) {
      try {
        new URL(instanceUrl);
      } catch {
        return res.status(400).json({ error: 'instanceUrl inválida' });
      }
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
    if (!client || !validateSlug(client)) {
      return res.status(400).json({ error: 'Cliente inválido' });
    }

    // Bloqueia se o loop deste cliente estiver rodando
    if (runningClients.has(client)) {
      return res.status(409).json({ error: 'Loop em execução para este cliente. Tente novamente em instantes.' });
    }

    await pool.query('BEGIN');
    await pool.query(`DROP TABLE IF EXISTS "${client}" CASCADE;`);
    await pool.query(`DROP TABLE IF EXISTS "${client}_totais" CASCADE;`);
    await pool.query(`DELETE FROM client_settings WHERE slug = $1;`, [client]);
    await pool.query('COMMIT');

    // Garantia: remover eventual trava residual
    runningClients.delete(client);

    return res.json({ status: 'ok', deleted: client });
  } catch (err) {
    console.error('Erro ao apagar cliente', err);
    try {
      await pool.query('ROLLBACK');
    } catch {}
    return res.status(500).json({ error: 'Erro interno ao apagar cliente' });
  }
});

/**
 * Endpoint para iniciar manualmente o loop de processamento de um cliente.
 * Espera um body JSON com { client: 'cliente_x', iaAuto?: boolean }.
 */
app.post('/api/loop', async (req, res) => {
  const clientSlug = req.body?.client;
  const iaAutoOverride = req.body?.iaAuto;
  if (!clientSlug || !validateSlug(clientSlug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  try {
    const result = await runLoopForClient(clientSlug, { iaAutoOverride });
    return res.json({
      message: 'Loop executado',
      processed: result.processed,
      status: result.status || 'ok',
    });
  } catch (err) {
    console.error('Erro ao executar loop manual', err);
    return res.status(500).json({ error: 'Erro interno ao executar loop' });
  }
});

/** SSE de progresso por cliente (com replay do último estado) */
app.get('/api/progress', (req, res) => {
  try {
    const client = req.query?.client;
    if (!client || !validateSlug(client)) {
      return res.status(400).json({ error: 'Cliente inválido' });
    }
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    res.write(`event: ping\ndata: {}\n\n`);

    try {
      const st = progressStates.get(client);
      if (st?.lastStart) {
        res.write(`data: ${JSON.stringify(st.lastStart)}\n\n`);
      }
      if (st?.items?.length) {
        for (const it of st.items) {
          res.write(`data: ${JSON.stringify(it)}\n\n`);
        }
      }
      if (st?.lastEnd) {
        res.write(`data: ${JSON.stringify(st.lastEnd)}\n\n`);
      }
    } catch (e) {}

    const em = getEmitter(client);
    const onProgress = (payload) => {
      try {
        res.write(`data: ${JSON.stringify(payload)}\n\n`);
      } catch {}
    };
    em.on('progress', onProgress);

    const ka = setInterval(() => {
      try {
        res.write(`event: ping\ndata: {}\n\n`);
      } catch {}
    }, 15000);

    req.on('close', () => {
      em.off('progress', onProgress);
      clearInterval(ka);
      try {
        res.end();
      } catch {}
    });
  } catch (err) {
    try {
      res.end();
    } catch {}
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

/* =====================  Loop Automático (scheduler)  ===================== */
// Agenda a execução automática diária às 08:00 para todos os clientes com auto_run = true.
// A cada disparo diário, gera os delays e processa até 30 mensagens por cliente (respeitando o horário).
function scheduleDailyAutoRun() {
  const now = new Date();
  const nextRun = new Date(now);
  nextRun.setHours(8, 0, 0, 0);
  // Caso já tenhamos passado do horário de hoje, agenda para o próximo dia
  if (now >= nextRun) {
    nextRun.setDate(nextRun.getDate() + 1);
  }
  const msUntilNext = nextRun.getTime() - now.getTime();

  setTimeout(async () => {
    try {
      const { rows } = await pool.query(`SELECT slug FROM client_settings WHERE auto_run = true;`);
      for (const { slug } of rows) {
        try {
          // Não inicia se o loop já estiver rodando para este cliente
          if (runningClients.has(slug)) continue;

          const exists = await tableExists(slug);
          if (!exists) continue;

          const cnt = await pool.query(`SELECT COUNT(*) AS count FROM "${slug}";`);
          const queueCount = Number(cnt.rows[0].count);

          // Apenas inicia se houver itens na fila
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
      // Agenda a próxima execução após a conclusão
      scheduleDailyAutoRun();
    }
  }, msUntilNext);
}

// Inicializa o agendamento diário
scheduleDailyAutoRun();

/* ======================================================================== */

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
