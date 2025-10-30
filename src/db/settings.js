/*
 * src/db/settings.js
 *
 * Este módulo contém funções de acesso ao banco de dados relacionadas à
 * configuração de clientes. Ao extrair a criação de tabela e as
 * operações de leitura/gravação para este local, mantemos server.js
 * organizado e separamos preocupações de persistência.
 */

const { pool } = require('../config');

/**
 * Garante que a tabela client_settings exista e contenha todas as
 * colunas necessárias. As colunas existentes são criadas apenas se
 * estiverem ausentes. Este método é idempotente e pode ser chamado
 * repetidamente sem efeitos adversos.
 *
 * @returns {Promise<void>}
 */
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

/**
 * Recupera as configurações de um cliente específico. Se o cliente ainda
 * não tiver registro na tabela, retorna um objeto com valores
 * padrão. Esse comportamento facilita o uso no código, evitando
 * verificações de null.
 *
 * @param {string} slug
 * @returns {Promise<Object>}
 */
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

/**
 * Insere ou atualiza as configurações de um cliente. Se já houver um
 * registro com o slug fornecido, apenas os campos listados são
 * atualizados; campos ausentes permanecem com seus valores
 * anteriores. A função também saneia o cabeçalho de autenticação e
 * aplica um limite superior à cota diária.
 *
 * @param {string} slug
 * @param {Object} param1
 * @param {boolean} param1.autoRun
 * @param {boolean} param1.iaAuto
 * @param {string} param1.instanceUrl
 * @param {string} param1.instanceToken
 * @param {string} param1.instanceAuthHeader
 * @param {string} param1.instanceAuthScheme
 * @param {number} param1.dailyLimit
 * @param {string} param1.messageTemplate
 * @returns {Promise<void>}
 */
async function saveClientSettings(
  slug,
  {
    autoRun,
    iaAuto,
    instanceUrl,
    instanceToken,
    instanceAuthHeader,
    instanceAuthScheme,
    dailyLimit,
    messageTemplate,
  }
) {
  const safeDaily =
    Number.isFinite(Number(dailyLimit)) && Number(dailyLimit) > 0
      ? Math.min(10000, Math.floor(Number(dailyLimit)))
      : null;

  let headerName = (instanceAuthHeader && instanceAuthHeader.trim()) || 'token';
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

module.exports = { ensureSettingsTable, getClientSettings, saveClientSettings };