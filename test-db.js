// Script de teste para verificar conexão com banco e funções SQL
require('dotenv').config();

const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
});

async function testDatabase() {
  try {
    console.log('Testando conexão com banco de dados...');
    
    // Testa conexão
    const result = await pool.query('SELECT NOW() as now;');
    console.log('✓ Conexão OK:', result.rows[0].now);

    // Verifica se as funções existem
    const functions = await pool.query(`
      SELECT routine_name 
      FROM information_schema.routines 
      WHERE routine_schema = 'public' 
        AND routine_type = 'FUNCTION'
        AND routine_name IN ('create_full_client_structure', 'client_add_contact', 'client_add_lead')
      ORDER BY routine_name;
    `);

    console.log('\nFunções SQL encontradas:');
    if (functions.rows.length === 0) {
      console.log('⚠ Nenhuma função encontrada. Execute o servidor para criar as funções.');
    } else {
      functions.rows.forEach(row => {
        console.log('✓', row.routine_name);
      });
    }

    // Verifica tabela client_settings
    const settingsTable = await pool.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'client_settings'
      ) as exists;
    `);

    if (settingsTable.rows[0].exists) {
      console.log('\n✓ Tabela client_settings existe');
    } else {
      console.log('\n⚠ Tabela client_settings não existe');
    }

    await pool.end();
    console.log('\nTeste concluído com sucesso!');
  } catch (error) {
    console.error('\n✗ Erro ao testar banco de dados:');
    console.error('  Mensagem:', error.message);
    console.error('  Código:', error.code);
    
    if (error.code === 'ENOTFOUND') {
      console.error('\n  Verifique se a variável DATABASE_URL está correta no arquivo .env');
    } else if (error.code === '28P01') {
      console.error('\n  Erro de autenticação. Verifique usuário e senha no DATABASE_URL');
    } else if (error.code === '3D000') {
      console.error('\n  Banco de dados não encontrado. Verifique o nome do banco no DATABASE_URL');
    }
    
    process.exit(1);
  }
}

testDatabase();
