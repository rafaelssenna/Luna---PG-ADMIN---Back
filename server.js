// server.js
// Servidor Express para a aplicação Luna

require('dotenv').config();

const express = require('express');
const { Pool } = require('pg');
const EventEmitter = require('events');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const axios = require('axios');
const crypto = require('crypto'); // adicionado para gerar IDs únicos

const app = express();

// ====== Importações de módulos reorganizados ======
// Funções de IA e UAZAPI extraídas para um serviço dedicado
const {
  runIAForContact,
  normalizePhoneE164BR,
  fillTemplate,
  normalizeNiche,
} = require('./src/services/ia');
// Utilitários de CSV (detecção de delimitador, parse e mapeamento de cabeçalhos)
const { detectDelimiter, parseCSV, mapHeader } = require('./src/utils/csv');
// Utilitários de texto (tokens, normalização de linhas e transcripts)
const { approxTokens, normalizeLine, toTranscriptLine } = require('./src/utils/text');
// Geração de PDF simples
const { generatePdfBuffer } = require('./src/utils/pdf');
// Cálculo de horários e distribuição de mensagens
const { hmsToSeconds, generateScheduleDelays } = require('./src/utils/schedule');
// Acesso a configurações de clientes no banco
const {
  ensureSettingsTable,
  getClientSettings,
  saveClientSettings,
} = require('./src/db/settings');
// Gestão de progresso de importações e loops
const {
  getEmitter,
  snapshotStart,
  snapshotPush,
  snapshotEnd,
} = require('./src/utils/progress');
// Funções SQL do banco de dados
const { ensureSQLFunctions } = require('./src/db/functions');
// Estado global e parâmetros padrão
const {
  runningClients,
  progressEmitters,
  progressStates,
  stopRequests,
  DAILY_MESSAGE_COUNT,
  DAILY_START_TIME,
  DAILY_END_TIME,
  ANALYSIS_MODEL,
  ANALYSIS_MAX_CHATS,
  ANALYSIS_PER_CHAT_LIMIT,
  ANALYSIS_INPUT_BUDGET,
  ANALYSIS_OUTPUT_BUDGET,
  DEFAULT_SYSTEM_PROMPT,
  SYSTEM_PROMPT_OVERRIDE,
  uaz,
  UAZAPI_ADMIN_TOKEN,
} = require('./src/config');

// Garante que a tabela client_settings exista (importado de src/db/settings.js)
ensureSettingsTable().catch((e) => console.error('ensureSettingsTable', e));
// Garante que as funções SQL necessárias existam (importado de src/db/functions.js)
ensureSQLFunctions().catch((e) => console.error('ensureSQLFunctions', e));

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
// A variável UAZAPI_ADMIN_TOKEN agora é importada de src/config.js.

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
// A instância 'uaz' é fornecida por src/config.js. A linha abaixo
// permanece para construir novos clientes, caso necessário.
// const uaz = buildClient(UAZAPI_BASE_URL);

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
  // Caso não encontre a instância no cache, assumimos que o próprio ID
  // pode ser o token da instância. Esse fallback é útil quando o front
  // passa o token diretamente ou quando o cache ainda não foi populado.
  if (!inst) {
    if (id && typeof id === 'string' && id.length > 4) {
      return id;
    }
    return null;
  }
  return inst.token || inst.instanceToken || inst.key || null;
}

/* ======================  Janela e cota diária  ====================== */
// As constantes DAILY_MESSAGE_COUNT, DAILY_START_TIME, DAILY_END_TIME,
// assim como ANALYSIS_MODEL, ANALYSIS_MAX_CHATS, ANALYSIS_PER_CHAT_LIMIT,
// ANALYSIS_INPUT_BUDGET, ANALYSIS_OUTPUT_BUDGET, DEFAULT_SYSTEM_PROMPT e
// SYSTEM_PROMPT_OVERRIDE agora são definidos em src/config.js e
// importados no início deste arquivo. Caso precise ajustar estes
// valores dinamicamente, faça-o em src/config.js ou via variáveis de
// ambiente.

// === Helper para gerar PDF simples com o texto das sugestões ===
// A implementação de escapePdfString e generatePdfBuffer foi movida
// para src/utils/pdf.js. Importe generatePdfBuffer no início deste
// arquivo para gerar um Buffer de PDF com o texto de análise.

// Logger para registrar execuções de análise
const { appendLog } = require('./utils/logger');

// Helpers para estimar tokens e normalizar texto
// As funções approxTokens, normalizeLine e toTranscriptLine foram movidas
// para src/utils/text.js. Importe-as no início do arquivo.

// As funções hmsToSeconds e generateScheduleDelays foram movidas para
// src/utils/schedule.js. Importe-as no início do arquivo para
// calcular atrasos de envio ao longo de uma janela de tempo.

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

// Gera um identificador único para correlacionar logs de análise.
// Tenta usar crypto.randomUUID se disponível, caso contrário usa um fallback pseudo-único.
function makeReqId() {
  try {
    return crypto.randomUUID();
  } catch {
    return Math.random().toString(36).slice(2) + '-' + Date.now().toString(36);
  }
}

/* ======================  Estado e SSE por cliente  ====================== */
// A partir desta versão, o estado e as funções de SSE foram extraídos para
// os módulos src/config.js (que fornece os conjuntos/mapas compartilhados)
// e src/utils/progress.js (que implementa getEmitter, snapshotStart,
// snapshotPush e snapshotEnd). Consulte esses arquivos para a
// implementação.

/* ======================  Tabela de settings por cliente  ====================== */
// As funções ensureSettingsTable, getClientSettings e saveClientSettings foram
// extraídas para src/db/settings.js. Chame ensureSettingsTable() após
// importar este módulo para garantir a criação da tabela.

/* ======================  IA (UAZAPI) ====================== */
/* ======================  IA (UAZAPI) ====================== */
// Todas as funções relacionadas à normalização de telefones, montagem de
// payloads e envio via UAZAPI foram extraídas para src/services/ia.js.
// Utilize runIAForContact() importada no início deste arquivo.

/* ======================  Helpers  ====================== */
async function tableExists(tableName) {
  const { rows } = await pool.query('SELECT to_regclass($1) AS reg;', [`public.${tableName}`]);
  return !!rows[0].reg;
}
// As funções norm, detectDelimiter, parseCSV e mapHeader foram movidas para
// src/utils/csv.js. Importe-as do módulo correspondente.

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
  if (!phone) return res.status(400).json({ error: 'Telefone é obrigatório' });

  // Se não tiver nome, usa o telefone como nome
  const finalName = name || phone;

  try {
    const result = await pool.query(
      'SELECT client_add_contact($1, $2, $3, $4) AS status;',
      [client, finalName, phone, niche || null]
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
    if (idx.phone === -1) {
      return res.status(400).json({ error: 'Cabeçalho inválido. Precisa conter coluna de telefone.' });
    }

    let inserted = 0, skipped = 0, errors = 0;

    console.log(`[IMPORT] Iniciando importação para cliente: ${slug}`);
    console.log(`[IMPORT] Total de linhas no CSV: ${rows.length - 1}`);

    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      if (!r || !r.length) continue;

      const name  = (r[idx.name]  || '').toString().trim();
      const phone = (r[idx.phone] || '').toString().trim();
      const niche = idx.niche !== -1 ? (r[idx.niche] || '').toString().trim() : null;

      if (!phone) { 
        console.log(`[IMPORT] Linha ${i}: Pulada (telefone vazio)`);
        skipped++; 
        continue; 
      }

      // Se não tiver nome, usa o telefone como nome
      const finalName = name || phone;

      try {
        console.log(`[IMPORT] Linha ${i}: Processando ${finalName} - ${phone}`);
        const q = await pool.query('SELECT client_add_contact($1, $2, $3, $4) AS status;', [slug, finalName, phone, niche]);
        const status = q.rows[0]?.status || 'inserted';
        console.log(`[IMPORT] Linha ${i}: Status retornado = ${status}`);
        
        if (status === 'inserted') {
          inserted++;
          console.log(`[IMPORT] Linha ${i}: ✓ Inserido com sucesso`);
        } else {
          skipped++;
          console.log(`[IMPORT] Linha ${i}: ⊘ Ignorado (motivo: ${status})`);
        }
      } catch (e) {
        console.error(`[IMPORT] Linha ${i}: ✗ ERRO:`, e.message);
        console.error(`[IMPORT] Linha ${i}: Stack:`, e.stack);
        errors++;
      }
    }

    console.log(`[IMPORT] Resultado final: ${inserted} inseridos, ${skipped} ignorados, ${errors} erros`);

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
// O conjunto stopRequests agora é gerenciado em src/config.js e importado no
// início deste arquivo. Ele é usado para sinalizar paradas de loops.
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

    console.log(`[LEADS] Buscando leads: region=${region}, niche=${niche}, limit=${limit}`);
    const raw = await searchLeads({ region, niche, limit });
    const results = Array.isArray(raw) ? raw : [];
    console.log(`[LEADS] Encontrados ${results.length} leads`);

    let inserted = 0, skipped = 0, errors = 0;

    for (const item of results) {
      const name  = (item.name && String(item.name).trim()) || String(item.phone || '').trim();
      const phone = String(item.phone || '').trim();
      const reg   = (item.region ?? region) || null;
      const nich  = (item.niche  ?? niche ) || null;

      if (!phone) { 
        console.log(`[LEADS] Lead sem telefone, pulando`);
        skipped++; 
        continue; 
      }

      try {
        console.log(`[LEADS] Processando: ${name} - ${phone} (region: ${reg}, niche: ${nich})`);
        const r = await pool.query(`SELECT client_add_lead($1,$2,$3,$4,$5) AS status;`,
          [client, name, phone, reg, nich]);
        const status = r.rows?.[0]?.status || 'inserted';
        console.log(`[LEADS] Status retornado: ${status}`);
        
        if (status === 'inserted' || status === 'queued_existing') {
          inserted++;
          console.log(`[LEADS] ✓ Lead adicionado: ${phone}`);
        } else {
          skipped++;
          console.log(`[LEADS] ⊘ Lead ignorado: ${phone} (motivo: ${status})`);
        }
      } catch (e) {
        console.error(`[LEADS] ✗ ERRO ao inserir lead ${phone}:`, e.message);
        console.error(`[LEADS] Stack:`, e.stack);
        errors++;
      }
    }

    console.log(`[LEADS] Resultado final: ${inserted} inseridos, ${skipped} ignorados, ${errors} erros`);

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
        whereNotIn = `AND f.phone NOT IN (${ph})`;
        params = arr;
      }

      // Busca da fila APENAS registros que NÃO foram marcados como enviados na tabela totais
      // Isso evita processar o mesmo número múltiplas vezes
      let name, phone, niche;
      try {
        const next = await pool.query(`
          SELECT f.name, f.phone, f.niche 
          FROM "${clientSlug}" f
          LEFT JOIN "${clientSlug}_totais" t ON f.phone = t.phone
          WHERE (t.mensagem_enviada IS NOT TRUE OR t.phone IS NULL)
            ${whereNotIn}
          ORDER BY f.name 
          LIMIT 1;
        `, params);
        if (!next.rows.length) break;
        ({ name, phone, niche } = next.rows[0]);
      } catch {
        // Fallback caso a coluna niche não exista
        const next = await pool.query(`
          SELECT f.name, f.phone 
          FROM "${clientSlug}" f
          LEFT JOIN "${clientSlug}_totais" t ON f.phone = t.phone
          WHERE (t.mensagem_enviada IS NOT TRUE OR t.phone IS NULL)
            ${whereNotIn}
          ORDER BY f.name 
          LIMIT 1;
        `, params);
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

      // SEMPRE remove da fila após processar (sucesso ou erro), para evitar loops infinitos
      // Usa transação para garantir consistência entre DELETE e UPDATE
      if (!manualStop) {
        try {
          await pool.query('BEGIN');
          
          // Remove da fila
          await pool.query(`DELETE FROM "${clientSlug}" WHERE phone = $1;`, [phone]);
          
          // Atualiza totais se envio foi bem-sucedido
          if (shouldMark) {
            await pool.query(
              `INSERT INTO "${clientSlug}_totais" (name, phone, niche, mensagem_enviada, updated_at)
               VALUES ($1, $2, $3, true, NOW())
               ON CONFLICT (phone) 
               DO UPDATE SET mensagem_enviada = true, updated_at = NOW();`,
              [name, phone, niche]
            );
            processed++;
          }
          
          await pool.query('COMMIT');
          
          if (!shouldMark) {
            console.warn(`[${clientSlug}] Envio falhou ou foi pulado (${status}). Removido da fila: ${phone}`);
          }
        } catch (err) {
          await pool.query('ROLLBACK');
          console.error(`[${clientSlug}] Erro ao processar ${phone}:`, err.message);
          // Mesmo com erro, adiciona ao attemptedPhones para não tentar novamente neste loop
        }
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
 * Esta rota agora delega toda a lógica de coleta de mensagens, geração
 * de transcrição, chamada à OpenAI e construção do PDF para o
 * serviço `generateAnalysisPdf`. O objetivo é simplificar o handler
 * original, delegando as responsabilidades pesadas para um módulo
 * dedicado. Isso facilita a manutenção, permite testes unitários e
 * reduz a complexidade do arquivo server.js.
 */
const { generateAnalysisPdf } = require('./src/services/exportAnalysis');

app.post('/api/instances/:id/export-analysis', async (req, res) => {
  try {
    const { id } = req.params;
    const slug = (req.query?.client || req.body?.client || '').toString();
    // Força por padrão, salvo se o query/body especificar explicitamente outro valor
    const forceRaw = req.query?.force ?? req.body?.force ?? '1';
    const force = ['1', 'true', 'yes', 'on'].includes(String(forceRaw).toLowerCase());
    // Gera um identificador de requisição para rastrear logs desta exportação
    const reqId = req.headers['x-request-id']?.toString() || makeReqId();
    console.log(`[ANALYSIS][${reqId}] start id=${id} slug=${slug} force=${force}`);
    const pdfBuffer = await generateAnalysisPdf(id, slug, force, { reqId });
    res.setHeader('Content-Type', 'application/pdf');
    const filenameSlug = slug && /^[a-z0-9_]+$/.test(slug) ? slug : 'client';
    res.setHeader('Content-Disposition', `attachment; filename="analysis-${id}-${filenameSlug}.pdf"`);
    console.log(`[ANALYSIS][${reqId}] end -> pdf ${pdfBuffer?.length || 0} bytes`);
    return res.end(pdfBuffer);
  } catch (err) {
    console.error('Erro em export-analysis', err);
    const fallbackPdf = generatePdfBuffer(`Erro na análise: ${String(err.message || err)}`);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="analysis-error.pdf"`);
    return res.end(fallbackPdf);
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

/**
 * GET /api/debug/ping-openai
 *
 * Endpoint para testar a comunicação com a OpenAI sem passar pela UAZAPI. Útil
 * para validar se a chave OPENAI_API_KEY está correta e se o modelo definido
 * em OPENAI_MODEL (ou ANALYSIS_MODEL) é compatível. Retorna um pequeno
 * trecho de texto gerado pela IA ou uma mensagem de erro.
 *
 * Exemplo de uso: GET /api/debug/ping-openai
 */
app.get('/api/debug/ping-openai', async (req, res) => {
  try {
    const openaiKey = process.env.OPENAI_API_KEY;
    const model = process.env.OPENAI_MODEL || ANALYSIS_MODEL;
    if (!openaiKey || !model) {
      return res.status(500).json({ ok: false, error: 'OPENAI_API_KEY ou OPENAI_MODEL/ANALYSIS_MODEL não configurado' });
    }
    const lower = String(model || '').toLowerCase();
    const isReasoning = /(gpt-5|gpt-4o|omni)/i.test(lower);
    let url;
    let payload;
    if (isReasoning) {
      // Usa o endpoint responses para modelos de raciocínio
      url = 'https://api.openai.com/v1/responses';
      payload = {
        model,
        input: [
          { role: 'system', content: 'Você é um respondedor de ping.' },
          { role: 'user', content: 'Diga "OK" se estiver funcionando.' },
        ],
        // A API de respostas requer pelo menos 16 tokens de saída. Definimos 32
        // para manter a margem e evitar erros de validação.
        max_output_tokens: 32,
        reasoning: { effort: 'low' },
      };
    } else {
      // Usa o endpoint chat/completions para modelos clássicos
      url = 'https://api.openai.com/v1/chat/completions';
      payload = {
        model,
        messages: [
          { role: 'system', content: 'Você é um respondedor de ping.' },
          { role: 'user', content: 'Diga "OK" se estiver funcionando.' },
        ],
        max_tokens: 10,
        n: 1,
      };
    }
    const response = await axios.post(url, payload, {
      headers: { Authorization: `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
      timeout: 30000,
    });
    let reply;
    if (isReasoning) {
      reply = response?.data?.output_text || response?.data?.choices?.[0]?.message?.content || '';
    } else {
      reply = response?.data?.choices?.[0]?.message?.content || '';
    }
    return res.json({ ok: true, reply: String(reply).trim() });
  } catch (err) {
    const msgErr = err.response?.data?.error?.message || err.message || err.toString();
    return res.status(500).json({ ok: false, error: msgErr });
  }
});

/**
 * GET /api/instances/:id/export-analysis.pdf
 *
 * Gera um relatório em PDF contendo apenas as sugestões da IA para as conversas recentes.
 * Diferentemente de `/export-analysis`, esta rota devolve um arquivo PDF pronto para
 * download com o texto das sugestões em vez de retornar JSON. O conteúdo do PDF
 * não inclui a transcrição das conversas, apenas as recomendações geradas pelo modelo.
 *
 * Requer as mesmas variáveis de ambiente que a rota `/export-analysis`:
 *  - OPENAI_API_KEY: chave da API da OpenAI
 *  - OPENAI_MODEL: nome do modelo (ex.: gpt-3.5-turbo ou gpt-5-mini)
 *  - OPENAI_SYSTEM_PROMPT (opcional): substitui o prompt padrão do papel system
 *  - OPENAI_TEMPERATURE (opcional): define a temperatura para modelos clássicos
 *  - OPENAI_REASONING_EFFORT (opcional): define o esforço de raciocínio para modelos de raciocínio (low, medium, high)
 *  - OPENAI_OUTPUT_BUDGET (opcional): máximo de tokens de saída por solicitação
 *
 * Parâmetros de consulta:
 *  - client: slug do cliente (obrigatório)
 */
app.get('/api/instances/:id/export-analysis.pdf', async (req, res) => {
  try {
    const { id } = req.params;
    const slug = (req.query?.client || '').toString();
    if (!slug || !validateSlug(slug)) {
      return res.status(400).json({ error: 'Cliente inválido' });
    }
    const force = ['1', 'true', 'yes', 'on'].includes(String(req.query?.force || '0').toLowerCase());
    // Identificador de requisição para rastreamento
    const reqId = req.headers['x-request-id']?.toString() || makeReqId();
    console.log(`[ANALYSIS][${reqId}] start (GET) id=${id} slug=${slug} force=${force}`);
    const pdfBuffer = await generateAnalysisPdf(id, slug, force, { reqId });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="report-${id}-${slug}.pdf"`);
    console.log(`[ANALYSIS][${reqId}] end (GET) -> pdf ${pdfBuffer?.length || 0} bytes`);
    return res.end(pdfBuffer);
  } catch (err) {
    console.error('Erro em export-analysis.pdf', err);
    const fallbackPdf = generatePdfBuffer('Erro interno na geração do relatório.');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="report-error.pdf"`);
    return res.end(fallbackPdf);
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
