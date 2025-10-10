// server.js
// Servidor Express para a aplicação Luna

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const path = require('path');

// Cria aplicação Express
const app = express();

/* ======================  CORS  ====================== */
// Configuração de CORS:
// - CORS_ANY=true  -> permite qualquer origem (sem credenciais)
// - CORS_ORIGINS   -> lista separada por vírgula de origens permitidas
const CORS_ANY = process.env.CORS_ANY === 'true';
const CORS_ORIGINS = (process.env.CORS_ORIGINS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (CORS_ANY) {
    // Reflete a origem quando conhecida; fallback para * (sem credenciais)
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  } else if (CORS_ORIGINS.length > 0) {
    if (origin && CORS_ORIGINS.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
    }
    // Se quiser forçar bloqueio quando origem não está na lista, remova o else abaixo.
    else {
      res.setHeader('Access-Control-Allow-Origin', '*');
    }
  } else {
    // Padrão: liberar geral (útil quando front e back estão separados e não há cookies)
    res.setHeader('Access-Control-Allow-Origin', '*');
  }

  res.setHeader('Vary', 'Origin, Access-Control-Request-Headers');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  // Reutiliza o cabeçalho solicitado no preflight para evitar falhas
  res.setHeader(
    'Access-Control-Allow-Headers',
    req.headers['access-control-request-headers'] || 'Content-Type, Authorization'
  );
  // Se um dia usar cookies/sessão: habilite a linha abaixo e NÃO use '*'
  // res.setHeader('Access-Control-Allow-Credentials', 'true');

  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }
  next();
});
/* =================================================== */

// Configura pool de conexões com PostgreSQL
// A string de conexão deve ser fornecida via variável de ambiente DATABASE_URL
// Em plataformas como Railway, a variável PORT também é definida automaticamente.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Muitos serviços em nuvem exigem SSL; se a variável DATABASE_SSL for 'true'
  // ativamos SSL com verificação de certificado desativada.
  ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
});

// Middleware para interpretar JSON de requisições
app.use(express.json());

// Servir arquivos estáticos (HTML, CSS e JS da interface)
// Os arquivos index.html, styles.css e app.js estão na raiz do projeto.
app.use(express.static(path.join(__dirname)));

// Configura o upload de arquivos em memória (para importação CSV)
const upload = multer({ storage: multer.memoryStorage() });

/**
 * Valida o slug (identificador) de um cliente.
 * Apenas valores no formato 'cliente_<alphanum_underscore>' são aceitos.
 * Isso previne SQL injection quando o slug é interpolado como nome de tabela.
 *
 * @param {string} slug
 * @returns {boolean}
 */
function validateSlug(slug) {
  return /^cliente_[a-z0-9_]+$/.test(slug);
}

/**
 * Healthcheck simples
 */
app.get('/api/healthz', (_req, res) => {
  res.json({ up: true });
});

/**
 * Lista os clientes existentes.
 * Percorre as tabelas do schema público que começam com "cliente_" e não terminam em "_totais".
 * Para cada cliente, conta quantos registros há na fila (tabela principal) e devolve `queueCount`.
 */
app.get('/api/clients', async (req, res) => {
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
        const countRes = await pool.query(`SELECT COUNT(*) AS count FROM "${slug}";`);
        const queueCount = Number(countRes.rows[0].count);
        clients.push({ slug, queueCount });
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

/**
 * Cria um novo cliente.
 * Espera body JSON { slug: 'cliente_nome' }.
 * Invoca a função stored procedure create_full_client_structure para criar as tabelas e triggers.
 */
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

/**
 * Retorna estatísticas resumidas para um cliente.
 * Query params: client=<slug>
 * Responde com { totais, enviados, pendentes, fila }
 */
app.get('/api/stats', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  try {
    const result = await pool.query(
      `SELECT
        (SELECT COUNT(*) FROM "${slug}_totais") AS totais,
        (SELECT COUNT(*) FROM "${slug}_totais" WHERE mensagem_enviada = true) AS enviados,
        (SELECT COUNT(*) FROM "${slug}") AS fila;`
    );
    const { totais, enviados, fila } = result.rows[0];
    const pendentes = Number(totais) - Number(enviados);
    res.json({
      totais: Number(totais),
      enviados: Number(enviados),
      pendentes,
      fila: Number(fila),
    });
  } catch (err) {
    console.error('Erro ao obter estatísticas', err);
    res.status(500).json({ error: 'Erro interno ao obter estatísticas' });
  }
});

/**
 * Lista itens da fila de um cliente, com paginação e busca.
 * Query params:
 *  - client=<slug>
 *  - page=<número>
 *  - pageSize=<tamanho>
 *  - search=<termo de busca>
 */
app.get('/api/queue', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
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
    const itemsParams = [...values, pageSize, offset];
    const itemsRes = await pool.query(itemsSql, itemsParams);
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

/**
 * Lista itens do histórico total de um cliente, com paginação, busca e filtro de status.
 * Query params:
 *  - client=<slug>
 *  - page=<número>
 *  - pageSize=<tamanho>
 *  - search=<termo de busca>
 *  - sent=all|sim|nao (enviados ou não)
 */
app.get('/api/totals', async (req, res) => {
  const slug = req.query.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
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
    conditions.push(`(name ILIKE $${params.length} OR phone ILIKE $${params.length} OR niche ILIKE $${params.length})`);
  }
  if (sent !== 'all') {
    if (sent === 'sim') {
      conditions.push('mensagem_enviada = true');
    } else if (sent === 'nao') {
      conditions.push('mensagem_enviada = false');
    }
  }
  const whereClause = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';
  try {
    const itemsSql = `
      SELECT name, phone, niche, mensagem_enviada, updated_at
        FROM "${slug}_totais"
      ${whereClause}
      ORDER BY updated_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2};
    `;
    const itemsParams = [...params, pageSize, offset];
    const itemsRes = await pool.query(itemsSql, itemsParams);
    const items = itemsRes.rows;

    const countSql = `SELECT COUNT(*) AS total FROM "${slug}_totais" ${whereClause};`;
    const countRes = await pool.query(countSql, params);
    const total = Number(countRes.rows[0].total);

    res.json({ items, total });
  } catch (err) {
    console.error('Erro ao consultar totais', err);
    res.status(500).json({ error: 'Erro interno ao consultar totais' });
  }
});

/**
 * Adiciona um contato individual ao cliente.
 * Body JSON: { client: <slug>, name: <string>, phone: <string>, niche?: <string|null> }
 * Retorna { status: 'inserted'|'skipped_conflict'|'skipped_already_known' }
 */
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
      [client, name, phone, niche || null],
    );
    const status = result.rows[0]?.status || 'inserted';
    res.json({ status });
  } catch (err) {
    if (err.code === '23505') {
      return res.json({ status: 'skipped_conflict' });
    }
    console.error('Erro ao adicionar contato', err);
    res.status(500).json({ error: 'Erro interno ao adicionar contato' });
  }
});

/**
 * Remove um contato da fila e opcionalmente marca como enviado no histórico.
 * Body JSON: { client: <slug>, phone: <string>, markSent?: boolean }
 */
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
    // Se markSent = true, ou se veio undefined (botão "Marcar Enviada"),
    // ou se não havia na fila (deletedCount=0), marca como enviada.
    const shouldMark = markSent === true || typeof markSent === 'undefined' || deletedCount === 0;
    if (shouldMark) {
      await pool.query(
        `UPDATE "${client}_totais" SET mensagem_enviada = true, updated_at = NOW() WHERE phone = $1;`,
        [phone],
      );
    }
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Erro ao remover/atualizar contato da fila', err);
    res.status(500).json({ error: 'Erro interno ao remover contato' });
  }
});

/**
 * Importa contatos em lote a partir de arquivo CSV.
 * FormData com campos:
 *  - file: arquivo CSV (campo "file")
 *  - client: slug do cliente
 * Responde com { inserted: n, skipped: m, errors: k }
 */
app.post('/api/import', upload.single('file'), async (req, res) => {
  const slug = req.body.client;
  if (!slug || !validateSlug(slug)) {
    return res.status(400).json({ error: 'Cliente inválido' });
  }
  if (!req.file) {
    return res.status(400).json({ error: 'Arquivo CSV não enviado' });
  }
  const text = req.file.buffer.toString('utf-8');
  const lines = text.split(/\r?\n/);
  let inserted = 0;
  let skipped = 0;
  let errorsCount = 0;

  // Se a primeira linha parece ser cabeçalho (contém "name" e "phone"), ignore-a
  let startIndex = 0;
  if (lines.length > 0) {
    const h = lines[0].toLowerCase();
    if (h.includes('name') && h.includes('phone')) {
      startIndex = 1;
    }
  }
  for (let i = startIndex; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const parts = line.split(',');
    if (parts.length < 2) {
      errorsCount++;
      continue;
    }
    const name = parts[0]?.trim();
    const phone = parts[1]?.trim();
    const niche = parts[2] ? parts[2].trim() : null;
    if (!name || !phone) {
      errorsCount++;
      continue;
    }
    try {
      const result = await pool.query(
        'SELECT client_add_contact($1, $2, $3, $4) AS status;',
        [slug, name, phone, niche || null],
      );
      const status = result.rows[0]?.status || 'inserted';
      if (status === 'inserted') inserted++;
      else skipped++;
    } catch (err) {
      if (err.code === '23505') {
        skipped++;
      } else {
        console.error('Erro ao importar linha', i + 1, err);
        errorsCount++;
      }
    }
  }
  res.json({ inserted, skipped, errors: errorsCount });
});

// Rota final: envia index.html para qualquer rota não reconhecida (SPA fallback)
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Inicia o servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
