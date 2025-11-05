/*
 * src/db/functions.js
 *
 * Este módulo garante que todas as funções SQL necessárias para o funcionamento
 * do sistema estejam criadas no banco de dados PostgreSQL.
 */

const { pool } = require('../config');

/**
 * Garante que todas as tabelas de clientes existentes tenham as colunas necessárias
 */
async function ensureClientColumns() {
  try {
    // Busca todas as tabelas de clientes (exceto _totais)
    const { rows } = await pool.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%\\_totais'
        AND table_name != 'client_settings'
      ORDER BY table_name;
    `);

    for (const { table_name } of rows) {
      try {
        // Adiciona coluna niche se não existir
        await pool.query(`ALTER TABLE "${table_name}" ADD COLUMN IF NOT EXISTS niche TEXT;`);
        
        // Adiciona coluna region se não existir  
        await pool.query(`ALTER TABLE "${table_name}" ADD COLUMN IF NOT EXISTS region TEXT;`);
        
        // Adiciona coluna created_at se não existir
        await pool.query(`ALTER TABLE "${table_name}" ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();`);

        // Faz o mesmo para tabela _totais se existir
        const totaisTable = `${table_name}_totais`;
        const { rows: totaisExists } = await pool.query(
          `SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = $1
          );`,
          [totaisTable]
        );

        if (totaisExists[0]?.exists) {
          await pool.query(`ALTER TABLE "${totaisTable}" ADD COLUMN IF NOT EXISTS niche TEXT;`);
          await pool.query(`ALTER TABLE "${totaisTable}" ADD COLUMN IF NOT EXISTS region TEXT;`);
          await pool.query(`ALTER TABLE "${totaisTable}" ADD COLUMN IF NOT EXISTS mensagem_enviada BOOLEAN DEFAULT false;`);
          await pool.query(`ALTER TABLE "${totaisTable}" ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();`);
        }

        console.log(`[DB] Colunas verificadas/criadas para: ${table_name}`);
      } catch (e) {
        console.warn(`[DB] Aviso ao verificar colunas de ${table_name}:`, e.message);
      }
    }
  } catch (error) {
    console.error('[DB] Erro ao garantir colunas:', error.message);
  }
}

/**
 * Cria ou atualiza as funções SQL necessárias no banco de dados.
 * Esta função é idempotente e pode ser executada múltiplas vezes sem problemas.
 */
async function ensureSQLFunctions() {
  try {
    // Primeiro garante que todas as tabelas existentes tenham as colunas necessárias
    await ensureClientColumns();

    // Função para criar estrutura completa de um cliente
    await pool.query(`
      CREATE OR REPLACE FUNCTION create_full_client_structure(client_slug TEXT)
      RETURNS void AS $$
      BEGIN
        -- Cria tabela de fila
        EXECUTE format('
          CREATE TABLE IF NOT EXISTS %I (
            name TEXT NOT NULL,
            phone TEXT PRIMARY KEY,
            niche TEXT,
            region TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
          )', client_slug);

        -- Cria tabela de totais
        EXECUTE format('
          CREATE TABLE IF NOT EXISTS %I (
            name TEXT NOT NULL,
            phone TEXT PRIMARY KEY,
            niche TEXT,
            region TEXT,
            mensagem_enviada BOOLEAN DEFAULT false,
            updated_at TIMESTAMPTZ DEFAULT NOW()
          )', client_slug || '_totais');

        -- Cria índices
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_phone ON %I(phone)', client_slug, client_slug);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_totais_phone ON %I(phone)', client_slug, client_slug || '_totais');
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_totais_enviada ON %I(mensagem_enviada)', client_slug, client_slug || '_totais');

        -- Insere registro inicial em client_settings
        INSERT INTO client_settings (slug, loop_status, last_run_at)
        VALUES (client_slug, 'idle', NOW())
        ON CONFLICT (slug) DO NOTHING;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // Função para adicionar contato
    await pool.query(`
      CREATE OR REPLACE FUNCTION client_add_contact(
        client_slug TEXT,
        contact_name TEXT,
        contact_phone TEXT,
        contact_niche TEXT DEFAULT NULL
      )
      RETURNS TEXT AS $$
      DECLARE
        already_sent BOOLEAN;
      BEGIN
        -- Verifica se já foi enviado
        EXECUTE format('SELECT mensagem_enviada FROM %I WHERE phone = $1', client_slug || '_totais')
        INTO already_sent
        USING contact_phone;

        -- Se já foi enviado, retorna
        IF already_sent = true THEN
          RETURN 'skipped_already_sent';
        END IF;

        -- Insere na tabela de totais
        EXECUTE format('
          INSERT INTO %I (name, phone, niche, mensagem_enviada, updated_at)
          VALUES ($1, $2, $3, false, NOW())
          ON CONFLICT (phone) DO NOTHING',
          client_slug || '_totais')
        USING contact_name, contact_phone, contact_niche;

        -- Insere na fila
        EXECUTE format('
          INSERT INTO %I (name, phone, niche)
          VALUES ($1, $2, $3)
          ON CONFLICT (phone) DO NOTHING',
          client_slug)
        USING contact_name, contact_phone, contact_niche;

        RETURN 'inserted';
      EXCEPTION
        WHEN unique_violation THEN
          RETURN 'skipped_conflict';
      END;
      $$ LANGUAGE plpgsql;
    `);

    // Função para adicionar lead (com região)
    await pool.query(`
      CREATE OR REPLACE FUNCTION client_add_lead(
        client_slug TEXT,
        lead_name TEXT,
        lead_phone TEXT,
        lead_region TEXT DEFAULT NULL,
        lead_niche TEXT DEFAULT NULL
      )
      RETURNS TEXT AS $$
      DECLARE
        already_sent BOOLEAN;
        queue_exists BOOLEAN;
      BEGIN
        -- Verifica se já foi enviado
        EXECUTE format('SELECT mensagem_enviada FROM %I WHERE phone = $1', client_slug || '_totais')
        INTO already_sent
        USING lead_phone;

        -- Se já foi enviado, retorna
        IF already_sent = true THEN
          RETURN 'skipped_already_sent';
        END IF;

        -- Insere na tabela de totais
        EXECUTE format('
          INSERT INTO %I (name, phone, region, niche, mensagem_enviada, updated_at)
          VALUES ($1, $2, $3, $4, false, NOW())
          ON CONFLICT (phone) 
          DO UPDATE SET region = COALESCE(EXCLUDED.region, %I.region), 
                        niche = COALESCE(EXCLUDED.niche, %I.niche)',
          client_slug || '_totais', client_slug || '_totais', client_slug || '_totais')
        USING lead_name, lead_phone, lead_region, lead_niche;

        -- Verifica se já está na fila
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE phone = $1)', client_slug)
        INTO queue_exists
        USING lead_phone;

        IF queue_exists THEN
          -- Atualiza região e nicho se fornecidos
          EXECUTE format('
            UPDATE %I 
            SET region = COALESCE($1, region), 
                niche = COALESCE($2, niche)
            WHERE phone = $3',
            client_slug)
          USING lead_region, lead_niche, lead_phone;
          RETURN 'queued_existing';
        ELSE
          -- Insere na fila
          EXECUTE format('
            INSERT INTO %I (name, phone, region, niche)
            VALUES ($1, $2, $3, $4)',
            client_slug)
          USING lead_name, lead_phone, lead_region, lead_niche;
          RETURN 'inserted';
        END IF;

      EXCEPTION
        WHEN unique_violation THEN
          RETURN 'skipped_conflict';
        WHEN others THEN
          RAISE WARNING 'Erro ao adicionar lead: %', SQLERRM;
          RETURN 'error';
      END;
      $$ LANGUAGE plpgsql;
    `);

    console.log('[DB] Funções SQL criadas/atualizadas com sucesso');
  } catch (error) {
    console.error('[DB] Erro ao criar funções SQL:', error.message);
    throw error;
  }
}

module.exports = { ensureSQLFunctions };
