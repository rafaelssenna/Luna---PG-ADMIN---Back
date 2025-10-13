// server.js
// Servidor Express para a aplicação Luna

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const path = require('path');

const app = express();

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
    req.headers['access-control-request-headers'] || 'Content-Type, Authorization'
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
// Tabela: client_settings (slug PK, auto_run, ia_auto, instance_url, loop_status, last_run_at)
const runningClients = new Set(); // trava por cliente (evita concorrer)

async function ensureSettingsTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS client_settings (
      slug TEXT PRIMARY KEY,
      auto_run BOOLEAN DEFAULT false,
      ia_auto BOOLEAN DEFAULT false,
      instance_url TEXT,
      loop_status TEXT DEFAULT 'idle',
      last_run_at TIMESTAMPTZ
    );
  `);
}
ensureSettingsTable().catch((e) => console.error('ensureSettingsTable', e));

async function getClientSettings(slug) {
  const { rows } = await pool.query(
    'SELECT auto_run, ia_auto, instance_url, loop_status, last_run_at FROM client_settings WHERE slug = $1',
    [slug]
  );
  if (!rows.length) {
    return { auto_run: false, ia_auto: false, instance_url: null, loop_status: 'idle', last_run_at: null };
  }
  return rows[0];
}

async function saveClientSettings(slug, { autoRun, iaAuto, instanceUrl }) {
  await pool.query(
    `INSERT INTO client_settings (slug, auto_run, ia_auto, instance_url)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (slug)
     DO UPDATE SET auto_run = EXCLUDED.auto_run, ia_auto = EXCLUDED.ia_auto, instance_url = EXCLUDED.instance_url`,
    [slug, !!autoRun, !!iaAuto, instanceUrl || null]
  );
}

// ===== IA (stub) =====
// Ajuste aqui caso você queira fazer chamada real para sua “instância” de IA.
// Por padrão, só simula sucesso. Para habilitar envio real, defina IA_CALL=true e
// coloque uma URL válida em client_settings.instance_url. O payload é { name, phone, client }.
async function runIAForContact({ client, name, phone, instanceUrl }) {
  const SHOULD_CALL = process.env.IA_CALL === 'true';
  if (!SHOULD_CALL || !instanceUrl) {
    // modo simulado
    return { ok: true, simulated: true };
  }
  try {
    if (typeof fetch !== 'function') {
      // Ambiente sem fetch nativo (Node < 18)
      console.warn('IA_CALL habilitado, mas fetch não está disponível. Pulando chamada.');
      return { ok: true, simulated: true };
    }
    const resp = await fetch(instanceUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client, name, phone }),
    });
    const ok = resp.ok;
    return { ok, status: resp.status };
  } catch (err) {
    console.error('Falha ao chamar IA', instanceUrl, err);
    return { ok: false, error: String(err) };
  }
}

/**
 * Executa o loop de processamento para um cliente.
 * Percorre a fila do cliente, (opcional) chama IA automática, remove da fila e
 * marca como enviada no histórico. Processa em lotes para evitar loops longos.
 *
 * @param {string} clientSlug Slug do cliente (p.ex. "cliente_x")
 * @param {object} [opts]
 * @param {number} [opts.batchSize] tamanho do lote (padrão: env LOOP_BATCH_SIZE ou 50)
 * @returns {Promise<{processed: number, status: 'ok'|'already_running'}>}
 */
async function runLoopForClient(clientSlug, opts = {}) {
  if (!validateSlug(clientSlug)) {
    throw new Error('Slug inválido');
  }

  // trava concorrência por cliente
  if (runningClients.has(clientSlug)) {
    return { processed: 0, status: 'already_running' };
  }
  runningClients.add(clientSlug);

  const batchSize = parseInt(process.env.LOOP_BATCH_SIZE, 10) || opts.batchSize || 50;

  try {
    // Atualiza status -> running
    await pool.query(
      `INSERT INTO client_settings (slug, loop_status, last_run_at)
       VALUES ($1, 'running', NOW())
       ON CONFLICT (slug) DO UPDATE SET loop_status = 'running', last_run_at = NOW()`,
      [clientSlug]
    );

    // Verifica se a tabela de fila existe
    const exists = await tableExists(clientSlug);
    if (!exists) {
      // Volta status -> idle
      await pool.query(
        `INSERT INTO client_settings (slug, loop_status, last_run_at)
         VALUES ($1, 'idle', NOW())
         ON CONFLICT (slug) DO UPDATE SET loop_status = 'idle', last_run_at = NOW()`,
        [clientSlug]
      );
      return { processed: 0, status: 'ok' };
    }

    const settings = await getClientSettings(clientSlug);
    let processed = 0;

    // Processa em lotes
    while (processed < batchSize) {
      const next = await pool.query(`SELECT name, phone FROM "${clientSlug}" ORDER BY name LIMIT 1;`);
      if (next.rows.length === 0) break; // fila vazia

      const { name, phone } = next.rows[0];

      // Chama IA (se habilitado)
      if (settings.ia_auto) {
        const r = await runIAForContact({
          client: clientSlug,
          name,
          phone,
          instanceUrl: settings.instance_url,
        });
        if (!r.ok) {
          console.warn(`[${clientSlug}] IA retornou erro para ${phone}. Prosseguindo com marcação mesmo assim.`);
        }
      }

      // Remove da fila
      try {
        await pool.query(`DELETE FROM "${clientSlug}" WHERE phone = $1;`, [phone]);
      } catch (err) {
        console.error('Erro ao deletar da fila', clientSlug, phone, err);
      }

      // Marca como enviada no histórico (se existir)
      try {
        await pool.query(
          `UPDATE "${clientSlug}_totais" SET mensagem_enviada = true, updated_at = NOW() WHERE phone = $1;`,
          [phone],
        );
      } catch (err) {
        console.error('Erro ao atualizar histórico', clientSlug, phone, err);
      }

      processed++;
    }

    // Volta status -> idle
    await pool.query(
      `INSERT INTO client_settings (slug, loop_status, last_run_at)
       VALUES ($1, 'idle', NOW())
       ON CONFLICT (slug) DO UPDATE SET loop_status = 'idle', last_run_at = NOW()`,
      [clientSlug]
    );

    return { processed, status: 'ok' };
  } catch (err) {
    // Marca erro
    await pool.query(
      `INSERT INTO client_settings (slug, loop_status, last_run_at)
       VALUES ($1, 'error', NOW())
       ON CONFLICT (slug) DO UPDATE SET loop_status = 'error', last_run_at = NOW()`,
      [clientSlug]
    );
    throw err;
  } finally {
    runningClients.delete(clientSlug);
  }
}
/* ============================================================================ */

/* ======== Helpers ======== */
async function tableExists(tableName) {
  const { rows } = await pool.query(`SELECT to_regclass($1) AS reg;`, [`public.${tableName}`]);
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
  const semis  = (firstLine.match(/;/g) || []).length;
  return semis > commas ? ';' : ',';
}
function parseCSV(text, delim) {
  const rows = [];
  let row = [], val = '', inQuotes = false;
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
  const nameKeys  = new Set(['nome','name','full_name','fullname','contato','empresa','nomefantasia','razaosocial']);
  const phoneKeys = new Set(['telefone','numero','número','phone','whatsapp','celular','mobile','telemovel']);
  const nicheKeys = new Set(['nicho','niche','segmento','categoria','industry']);

  names.forEach((h, i) => {
    if (isId(h)) return;
    if (idx.name === -1  && nameKeys.has(h))  idx.name  = i;
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
          pool.query(`SELECT COUNT(*) AS count FROM "${slug}";`),
          pool.query(`SELECT auto_run, ia_auto, instance_url FROM client_settings WHERE slug = $1;`, [slug]),
        ]);
        const queueCount = Number(countRes.rows[0].count);
        const autoRun = !!(cfgRes.rows[0]?.auto_run);
        const iaAuto = !!(cfgRes.rows[0]?.ia_auto);
        const instanceUrl = cfgRes.rows[0]?.instance_url || null;
        clients.push({ slug, queueCount, autoRun, iaAuto, instanceUrl });
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

/** KPIs (blindado) */
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

    let totais = 0, enviados = 0, fila = 0;

    if (hasTotais) {
      const r = await pool.query(
        `SELECT
           (SELECT COUNT(*) FROM "${totaisTable}") AS totais,
           (SELECT COUNT(*) FROM "${totaisTable}" WHERE mensagem_enviada = true) AS enviados;`
      );
      totais   = Number(r.rows[0].totais);
      enviados = Number(r.rows[0].enviados);
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
    });
  } catch (err) {
    console.error('Erro ao obter estatísticas', err);
    res.status(500).json({ error: 'Erro interno ao obter estatísticas' });
  }
});

/** Fila (pagina/filtra) — blindado para tabela ausente */
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

/** Remove da fila e, opcionalmente, marca como enviada nos totais */
app.delete('/api/queue', async (req, res) => {
  const { client, phone, markSent } = req.body;
  if (!client || !validateSlug(client)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  if (!phone) {
    return res.status(400).json({ error: 'Telefone é obrigatório' });
  }
  try {
    const delRes = await pool.query(`DELETE FROM "${client}" WHERE phone = $1;`, [phone]);
    const deletedCount = delRes.rowCount;
    // Marca como enviada se markSent = true, ou undefined (botão "Marcar Enviada"), ou se nem estava na fila.
    const shouldMark =
      markSent === true ||
      typeof markSent === 'undefined' ||
      deletedCount === 0;
    if (shouldMark) {
      await pool.query(
        `UPDATE "${client}_totais" SET mensagem_enviada = true, updated_at = NOW() WHERE phone = $1;`,
        [phone]
      );
    }
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Erro ao remover/atualizar contato da fila', err);
    res.status(500).json({ error: 'Erro interno ao remover contato' });
  }
});

/** Importa contatos via CSV (mapeando cabeçalhos, ignorando ID, com heurísticas) */
app.post('/api/import', upload.single('file'), async (req, res) => {
  const slug = req.body.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  if (!req.file) {
    return res.status(400).json({ error: 'Arquivo CSV não enviado' });
  }
  const text = req.file.buffer.toString('utf-8');
  if (!text.trim()) {
    return res.status(400).json({ error: 'CSV vazio' });
  }
  const firstLine = text.split(/\r?\n/)[0];
  const delim = detectDelimiter(firstLine);
  const rows = parseCSV(text, delim).filter((r) =>
    r.some((c) => (c || '').trim() !== '')
  );
  if (rows.length === 0) {
    return res.json({ inserted: 0, skipped: 0, errors: 0 });
  }
  const header = rows[0].map((c) => (c ?? '').toString().trim());
  const hasHeaderHints = header.some((h) =>
    /nome|name|telefone|n[uú]mero|phone|whats|celular|nicho|niche/i.test(h)
  );
  let startIndex = 0;
  let map = { name: -1, phone: -1, niche: -1 };
  if (hasHeaderHints) {
    map = mapHeader(header);
    startIndex = 1;
  }
  // Fallback heurístico se não detectou cabeçalhos
  if (map.phone === -1 || map.name === -1) {
    const sample = rows[Math.min(startIndex, rows.length - 1)];
    let phoneIdx = -1;
    let nameIdx = -1;
    sample.forEach((v, i) => {
      const digits = (v || '').replace(/\D/g, '');
      if (phoneIdx === -1 && digits.length >= 10) phoneIdx = i;
    });
    if (phoneIdx !== -1) {
      for (let i = 0; i < sample.length; i++) {
        if (i === phoneIdx) continue;
        const s = (sample[i] || '').trim();
        const isMostlyDigits = /^\d{1,}$/.test(s);
        if (!isMostlyDigits && !/^id$/i.test(norm(header[i] || ''))) {
          nameIdx = i;
          break;
        }
      }
    }
    if (phoneIdx !== -1 && nameIdx !== -1) {
      map.phone = phoneIdx;
      map.name = nameIdx;
    }
  }
  if (map.phone === -1 || map.name === -1) {
    return res.status(400).json({
      error:
        'Não foi possível identificar colunas de nome e telefone no CSV.',
    });
  }
  let inserted = 0;
  let skipped = 0;
  let errorsCount = 0;
  for (let r = startIndex; r < rows.length; r++) {
    const row = rows[r];
    if (!row) continue;
    const name  = (row[map.name]  ?? '').toString().trim();
    const phone = (row[map.phone] ?? '').toString().trim();
    const niche = map.niche !== -1 ? (row[map.niche] ?? '').toString().trim() : null;
    if (!name || !phone) {
      errorsCount++;
      continue;
    }
    try {
      const result = await pool.query(
        'SELECT client_add_contact($1, $2, $3, $4) AS status;',
        [slug, name, phone, niche || null]
      );
      const status = result.rows[0]?.status || 'inserted';
      if (status === 'inserted') inserted++;
      else skipped++;
    } catch (err) {
      if (err.code === '23505') skipped++;
      else {
        console.error('Erro ao importar linha', r + 1, err);
        errorsCount++;
      }
    }
  }
  res.json({ inserted, skipped, errors: errorsCount });
});

/** Lê configurações do cliente (auto_run, ia_auto, instance_url, status) */
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
      loopStatus: cfg.loop_status || 'idle',
      lastRunAt: cfg.last_run_at || null,
    });
  } catch (err) {
    console.error('Erro ao obter configurações', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});

/** Salva configurações do cliente (auto_run, ia_auto, instance_url) */
app.post('/api/client-settings', async (req, res) => {
  const { client, autoRun, iaAuto, instanceUrl } = req.body || {};
  if (!client || !validateSlug(client)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  try {
    // Validação simples da URL (opcional)
    if (instanceUrl) {
      try { new URL(instanceUrl); } catch { return res.status(400).json({ error: 'instanceUrl inválida' }); }
    }
    await saveClientSettings(client, { autoRun, iaAuto, instanceUrl });
    const cfg = await getClientSettings(client);
    res.json({
      ok: true,
      settings: {
        autoRun: !!cfg.auto_run,
        iaAuto: !!cfg.ia_auto,
        instanceUrl: cfg.instance_url || null,
      }
    });
  } catch (err) {
    console.error('Erro ao salvar configurações', err);
    res.status(500).json({ error: 'Erro interno' });
  }
});

/**
 * Endpoint para iniciar manualmente o loop de processamento de um cliente.
 * Espera um body JSON com { client: 'cliente_x' }. Retorna a quantidade de contatos processados.
 */
app.post('/api/loop', async (req, res) => {
  const clientSlug = req.body?.client;
  if (!clientSlug || !validateSlug(clientSlug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  try {
    const result = await runLoopForClient(clientSlug);
    return res.json({ message: 'Loop executado', processed: result.processed, status: result.status || 'ok' });
  } catch (err) {
    console.error('Erro ao executar loop manual', err);
    return res.status(500).json({ error: 'Erro interno ao executar loop' });
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

/* =====================  Loop Automático (scheduler)  ===================== */
// Intervalo em milissegundos para executar o loop automaticamente.
// Pode ser configurado via variável de ambiente LOOP_INTERVAL_MS (padrão: 60000ms = 1 minuto).
const LOOP_INTERVAL_MS = parseInt(process.env.LOOP_INTERVAL_MS, 10) || 60000;

setInterval(async () => {
  try {
    // Apenas clientes com auto_run = true
    const { rows } = await pool.query(`SELECT slug FROM client_settings WHERE auto_run = true;`);
    for (const { slug } of rows) {
      try {
        if (runningClients.has(slug)) continue; // já em execução
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
  }
}, LOOP_INTERVAL_MS);
/* ======================================================================== */

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
