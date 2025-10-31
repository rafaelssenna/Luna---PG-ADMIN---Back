/*
 * src/config.js
 *
 * Este módulo centraliza a configuração e os singletons utilizados em toda a
 * aplicação. Ao extrair a criação de clientes, pools e objetos de estado
 * compartilhado para este arquivo, podemos importar a mesma instância em
 * diferentes partes do código sem recriar conexões ou duplicar lógica de
 * inicialização. Também consolida variáveis de ambiente e valores padrão.
 */

require('dotenv').config();

const { Pool } = require('pg');
const multer = require('multer');
// Importações de módulos externos ao diretório src.
// Como este arquivo reside em src/, precisamos voltar um nível para acessar
// os módulos localizados na raiz do projeto (services e utils). Usar paths
// relativos baseados em './' (que aponta para src/) resultava em erros de
// resolução e causava falhas em tempo de execução. O correto é '../' para
// alcançar a raiz do projeto.
const { buildClient } = require('../services/uazapi');
const helpers = require('../utils/helpers');
const { searchLeads } = require('../services/leadsSearcher');
const { appendLog } = require('../utils/logger');

// ========= Conexão com o banco de dados =========
// Define um pool global para reutilizar conexões com o PostgreSQL. A configuração
// SSL é opcional e baseada na variável de ambiente DATABASE_SSL. Se for
// configurado como 'true', aceita conexões TLS sem rejeitar certificados
// autoassinados. Caso contrário, nenhuma configuração SSL é aplicada.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
});

// ========= Cliente UAZAPI =========
// Cria um cliente para comunicação com a UAZAPI a partir da URL base
// configurada em UAZAPI_BASE_URL. Este cliente é usado para consultar
// instâncias, chats e mensagens.
const UAZAPI_BASE_URL = process.env.UAZAPI_BASE_URL;
const UAZAPI_ADMIN_TOKEN = process.env.UAZAPI_ADMIN_TOKEN;
const uaz = buildClient(UAZAPI_BASE_URL);

// ========= Uploads =========
// Instância global do multer para upload de arquivos na memória. Isso evita
// recriar a configuração em cada endpoint que precisa lidar com uploads.
const upload = multer({ storage: multer.memoryStorage() });

// ========= Estado global =========
// Conjuntos e mapas para rastrear o estado de execução de loops, progresso
// incremental e solicitações de parada. Esses objetos são compartilhados entre
// controladores e módulos para coordenar operações assíncronas.
const runningClients = new Set();
const progressEmitters = new Map();
const progressStates = new Map();
const stopRequests = new Set();

// ========= Parâmetros de janela e cota diária =========
// Valores padrão para a quantidade de mensagens diárias e janela de envio.
// Estes podem ser ajustados diretamente aqui ou via variáveis de ambiente
// nas rotinas que consomem estes valores.
const DAILY_MESSAGE_COUNT = 30;
const DAILY_START_TIME = '08:00:00';
const DAILY_END_TIME = '17:30:00';

// ========= Parâmetros de análise de conversas =========
// Definem limites e comportamentos para a função de exportação/análise de
// conversas. Permitem configurar o modelo da OpenAI, número máximo de
// conversas e mensagens, além dos orçamentos de tokens de entrada e saída.
const ANALYSIS_MODEL = process.env.OPENAI_MODEL || 'gpt-3.5-turbo';
const ANALYSIS_MAX_CHATS = parseInt(process.env.ANALYSIS_MAX_CHATS || '30', 10);
const ANALYSIS_PER_CHAT_LIMIT = parseInt(process.env.ANALYSIS_PER_CHAT_LIMIT || '200', 10);
const ANALYSIS_INPUT_BUDGET = parseInt(process.env.OPENAI_INPUT_BUDGET || '12000', 10);
const ANALYSIS_OUTPUT_BUDGET = parseInt(process.env.OPENAI_OUTPUT_BUDGET || '4096', 10);

// Prompt padrão para o papel "system" na conversa com o modelo. Pode ser
// sobrescrito através da variável de ambiente OPENAI_SYSTEM_PROMPT. Se desejar
// personalizar o comportamento global do analista, defina essa variável no
// .env ou nos ambientes de implantação (Railway, Vercel etc.).
const DEFAULT_SYSTEM_PROMPT =
  'Você é um analista de conversas da assistente Luna (WhatsApp B2B). ' +
  'Analise as conversas recentes e proponha melhorias objetivas de abertura, abordagem, qualificação, follow-ups e fechamento. ' +
  'Sugira ajustes no tom, clareza, timing e conteúdo das mensagens para melhorar engajamento e taxa de resposta. ' +
  'Forneça exemplos de frases prontas e bullets acionáveis. Se houver poucas mensagens, adapte a análise.';
const SYSTEM_PROMPT_OVERRIDE = process.env.OPENAI_SYSTEM_PROMPT || '';

module.exports = {
  pool,
  uaz,
  helpers,
  searchLeads,
  appendLog,
  upload,
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
  UAZAPI_ADMIN_TOKEN,
};
