/*
 * src/services/exportAnalysis.js
 *
 * Este módulo encapsula toda a lógica de exportação e análise de conversas
 * da aplicação Luna. A rotina originalmente estava implementada dentro
 * do arquivo server.js, o que tornava o código extenso e difícil de
 * testar. Ao extrair para um serviço separado conseguimos organizar
 * melhor as responsabilidades, reutilizar funções em diferentes rotas e
 * facilitar futuras evoluções como novos modelos de IA ou fontes de dados.
 *
 * A função principal, `generateAnalysisPdf`, coleta mensagens recentes de
 * uma instância UAZ, compila uma transcrição resumida, invoca a API da
 * OpenAI com o modelo configurado e finalmente retorna um Buffer com um
 * relatório em PDF contendo as sugestões produzidas pela IA. Além disso
 * atualiza o campo `analysis_last_msg_ts` no banco de dados para evitar
 * reprocessar conversas já analisadas, quando o gate estiver ativo.
 */

const axios = require('axios');
const {
  pool,
  uaz,
  appendLog,
  ANALYSIS_MODEL,
  ANALYSIS_MAX_CHATS,
  ANALYSIS_PER_CHAT_LIMIT,
  ANALYSIS_INPUT_BUDGET,
  ANALYSIS_OUTPUT_BUDGET,
  DEFAULT_SYSTEM_PROMPT,
  SYSTEM_PROMPT_OVERRIDE,
  UAZAPI_ADMIN_TOKEN,
} = require('../config');
const { approxTokens, toTranscriptLine } = require('../utils/text');
const { generatePdfBuffer } = require('../utils/pdf');
const helpers = require('../../utils/helpers');

// ======= Logging helpers =======
// Ativa logs detalhados quando ANALYSIS_DEBUG ou DEBUG estiverem definidos.
const ANALYSIS_DEBUG = String(process.env.ANALYSIS_DEBUG || process.env.DEBUG || 'false').toLowerCase() === 'true';

/**
 * Emite logs padronizados da análise, prefixando com o ID da requisição.
 * @param {string} reqId Identificador de correlação
 * @param  {...any} args Mensagens a serem logadas
 */
function log(reqId, ...args) {
  try {
    console.log(`[ANALYSIS][${reqId}]`, ...args);
  } catch (e) {
    // ignora falhas de log
  }
}

/**
 * Mascara tokens longos para que não vazem em logs.
 * @param {string} tok Token a ser mascarado
 * @returns {string}
 */
function maskToken(tok) {
  if (!tok || typeof tok !== 'string') return '';
  if (tok.length <= 8) return '***';
  return tok.slice(0, 4) + '…' + tok.slice(-4);
}

/**
 * Resolve o token da instância desejada consultando a UAZAPI com o
 * token de administrador. Isso substitui o cache global de instâncias
 * presente em server.js e garante que sempre temos o token mais
 * recente. Retorna null caso a instância não seja encontrada.
 *
 * @param {string} instanceId ID da instância a ser resolvida
 * @returns {Promise<string|null>} Token da instância ou null
 */
async function resolveInstanceToken(instanceId) {
  try {
    const data = await uaz.listInstances(UAZAPI_ADMIN_TOKEN);
    const list = Array.isArray(data?.content)
      ? data.content
      : Array.isArray(data)
        ? data
        : [];
    const it = list.find((x) => {
      const id = x.id || x._id || x.instanceId || x.token;
      return String(id) === String(instanceId);
    });
    if (!it) {
      // Fallback: caso o ID fornecido já seja o próprio token da instância.
      // Isso é útil quando o front passa o token diretamente em vez do identificador numérico.
      if (instanceId && typeof instanceId === 'string' && instanceId.length > 4) {
        // evita retornar strings vazias ou muito curtas; aceita tokens longos
        return instanceId;
      }
      return null;
    }
    return it.token || it.instanceToken || it.key || null;
  } catch (err) {
    console.error('Erro ao resolver token da instância', err);
    return null;
  }
}

/**
 * Recupera a marca de tempo da última análise registrada para o cliente.
 * Quando `useGate` é falso ou não houver registro, retorna null.
 *
 * @param {string} slug Slug do cliente
 * @param {boolean} useGate Ativa o gate para ignorar mensagens antigas
 * @returns {Promise<Date|null>} Timestamp como objeto Date ou null
 */
async function getLastAnalysisTs(slug, useGate) {
  if (!useGate) return null;
  try {
    const r = await pool.query(
      `SELECT analysis_last_msg_ts FROM client_settings WHERE slug = $1`,
      [slug]
    );
    const ts = r.rows?.[0]?.analysis_last_msg_ts;
    return ts ? new Date(ts) : null;
  } catch (err) {
    console.warn('Falha ao obter analysis_last_msg_ts para', slug, err?.message);
    return null;
  }
}

/**
 * Atualiza a marca de tempo da última análise para o cliente se o gate
 * estiver ativo. Qualquer erro de atualização é logado mas não causa
 * falha no processo de geração de relatório.
 *
 * @param {string} slug Slug do cliente
 * @param {Date|number} ts Novo timestamp (em milissegundos ou objeto Date)
 * @param {boolean} useGate Define se o gate estava ativado para atualizar
 */
async function updateLastAnalysisTs(slug, ts, useGate) {
  if (!useGate || !ts) return;
  try {
    const iso = typeof ts === 'number' ? new Date(ts).toISOString() : new Date(ts).toISOString();
    await pool.query(
      `UPDATE client_settings SET analysis_last_msg_ts = $2 WHERE slug = $1`,
      [slug, iso]
    );
  } catch (err) {
    console.error('Erro ao atualizar analysis_last_msg_ts', slug, err?.message);
  }
}

/**
 * Coleta e ordena os chats de uma instância UAZ. A busca é paginada
 * conforme o tamanho máximo configurado pelo modelo de análise. A lista
 * retornada é ordenada descendentemente pelo último timestamp de
 * atualização e limitada em `ANALYSIS_MAX_CHATS`.
 *
 * @param {string} token Token de acesso da instância
 * @returns {Promise<Array>} Lista de chats selecionados
 */
async function collectChats(token) {
  const pageSize = 100;
  let offset = 0;
  const chats = [];
  for (;;) {
    const data = await uaz.findChats(token, { limit: pageSize, offset });
    const page = helpers.pickArrayList(data);
    if (!page.length) break;
    chats.push(...page);
    if (page.length < pageSize) break;
    offset += pageSize;
    if (chats.length >= ANALYSIS_MAX_CHATS * 2) break; // evita loops longos
  }
  const chatLastTs = (c) => c?.wa_lastTimestamp || c?.lastMessageTimestamp || c?.updatedAt || c?.createdAt || 0;
  chats.sort((a, b) => Number(chatLastTs(b)) - Number(chatLastTs(a)));
  return chats.slice(0, ANALYSIS_MAX_CHATS);
}

/**
 * Coleta mensagens recentes de uma lista de chats. As mensagens são
 * filtradas de acordo com o gate de tempo (última análise) e limitadas
 * a `ANALYSIS_PER_CHAT_LIMIT` por chat. Retorna um array de objetos
 * contendo o timestamp numérico e a mensagem.
 *
 * @param {string} token Token de acesso da instância
 * @param {Array} chats Lista de chats já ordenados
 * @param {Date|null} lastTs Data da última análise ou null
 * @param {boolean} useGate Define se o gate deve ser aplicado
 * @returns {Promise<Array<{timestamp:number,msg:object}>>}
 */
async function collectMessages(token, chats, lastTs, useGate) {
  const results = [];
  let maxTs = lastTs ? lastTs.getTime() : 0;
  for (const chat of chats) {
    const chatId = helpers.extractChatId(chat);
    if (!chatId) continue;
    const data = await uaz.findMessages(token, {
      chatid: chatId,
      limit: ANALYSIS_PER_CHAT_LIMIT,
      offset: 0,
    });
    const msgs = helpers.pickArrayList(data);
    // Ordena ascendentemente por timestamp
    msgs.sort((a, b) => {
      const ta = a?.messageTimestamp || a?.timestamp || a?.wa_timestamp || a?.createdAt || a?.date || 0;
      const tb = b?.messageTimestamp || b?.timestamp || b?.wa_timestamp || b?.createdAt || b?.date || 0;
      return Number(ta) - Number(tb);
    });
    for (const msg of msgs) {
      const rawTs = msg?.messageTimestamp || msg?.timestamp || msg?.wa_timestamp || msg?.createdAt || msg?.date || null;
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
      if (useGate && lastTs && numTs <= lastTs.getTime()) continue;
      results.push({ timestamp: numTs, msg });
      if (numTs > maxTs) maxTs = numTs;
    }
  }
  return { messages: results, maxTs };
}

/**
 * Constrói a transcrição compacta das mensagens coletadas. As mensagens
 * precisam estar ordenadas. A transcrição consiste em linhas no formato
 * definido por `toTranscriptLine`, removendo itens vazios.
 *
 * @param {Array<{timestamp:number,msg:object}>} items Mensagens ordenadas
 * @returns {Array<string>} Lista de linhas de transcrição
 */
function buildTranscript(items) {
  return items.map(({ msg }) => toTranscriptLine(msg)).filter(Boolean);
}

/**
 * Agrupa linhas de transcrição em blocos respeitando o orçamento de
 * tokens configurado. Cada bloco inclui o texto introdutório e o
 * prompt do sistema. Retorna uma lista de strings, uma por bloco.
 *
 * @param {Array<string>} lines Linhas da transcrição
 * @param {string} systemPrompt Prompt de sistema
 * @param {string} userIntro Texto de introdução do usuário
 * @returns {Array<string>}
 */
function chunkTranscript(lines, systemPrompt, userIntro) {
  const chunks = [];
  const baseTokens = approxTokens(systemPrompt) + approxTokens(userIntro) + 50;
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
  if (current.length) chunks.push(current.join('\n'));
  return chunks;
}

/**
 * Invoca a API de chat da OpenAI para cada bloco de transcrição. Ajusta
 * automaticamente os parâmetros de requisição de acordo com o modelo em
 * uso (modelos de raciocínio versus modelos clássicos). Se ocorrer um
 * erro em algum bloco, apenas loga e continua com os demais. Retorna
 * uma lista de respostas (strings) já aparadas.
 *
 * @param {Array<string>} chunks Blocos de transcrição
 * @param {string} systemPrompt Prompt de sistema
 * @param {string} userIntro Introdução do usuário
 * @param {string} openaiKey Chave da API da OpenAI
 * @returns {Promise<Array<string>>}
 */
/**
 * Helper para detectar se um nome de modelo pertence à família de
 * raciocínio da OpenAI (ex.: gpt-5*, gpt-4o*, omni*). Esses modelos
 * utilizam a API de "responses" em vez da API de chat.
 *
 * @param {string} name Nome do modelo
 * @returns {boolean}
 */
function isReasoningModelName(name) {
  const n = String(name || '').toLowerCase();
  return /(gpt-5|gpt-4o|omni)/i.test(n);
}

/**
 * Invoca a API da OpenAI para cada bloco de transcrição. Para modelos
 * clássicos, usa o endpoint chat/completions; para modelos de
 * raciocínio (gpt-5*, gpt-4o*, omni*), usa o endpoint responses. Em
 * caso de erro registra a mensagem e continua. Retorna um objeto
 * contendo as respostas obtidas e a lista de erros coletados.
 *
 * @param {Array<string>} chunks Blocos de transcrição
 * @param {string} systemPrompt Prompt de sistema
 * @param {string} userIntro Introdução do usuário
 * @param {string} openaiKey Chave da API da OpenAI
 * @returns {Promise<{responses:string[],errors:string[]}>}
 */
async function callOpenAI(chunks, systemPrompt, userIntro, openaiKey, reqId = undefined) {
  const responses = [];
  const errorsCollected = [];
  const model = process.env.OPENAI_MODEL || ANALYSIS_MODEL;
  const reasoning = isReasoningModelName(model);
  for (const contentBody of chunks) {
    const content = `${userIntro}\n\n${contentBody}`;
    try {
      let text = '';
      if (reasoning) {
        const payload = {
          model,
          input: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content },
          ],
          max_output_tokens: Number(process.env.OPENAI_OUTPUT_BUDGET || ANALYSIS_OUTPUT_BUDGET) || 1024,
          reasoning: { effort: process.env.OPENAI_REASONING_EFFORT || 'low' },
        };
        const tEnv = process.env.OPENAI_TEMPERATURE;
        const parsedTemp = tEnv !== undefined ? Number(tEnv) : undefined;
        if (parsedTemp !== undefined && !Number.isNaN(parsedTemp)) payload.temperature = parsedTemp;
        const startTs = Date.now();
        const resp = await axios.post('https://api.openai.com/v1/responses', payload, {
          headers: { Authorization: `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
          timeout: 60000,
          validateStatus: () => true,
        });
        if (ANALYSIS_DEBUG && reqId) {
          const usage = resp?.data?.usage || {};
          log(reqId, `openai responses status=${resp.status} dt=${Date.now() - startTs}ms usage=${JSON.stringify(usage)}`);
        }
        if (resp.status >= 400) {
          const errMsg = resp?.data?.error?.message || `OpenAI HTTP ${resp.status}`;
          errorsCollected.push(errMsg);
        } else {
          text = resp?.data?.output_text || resp?.data?.choices?.[0]?.message?.content || '';
        }
      } else {
        const payload = {
          model,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content },
          ],
          n: 1,
          max_tokens: Number(process.env.OPENAI_OUTPUT_BUDGET || ANALYSIS_OUTPUT_BUDGET) || 1024,
        };
        const tEnv = process.env.OPENAI_TEMPERATURE;
        const parsedTemp = tEnv !== undefined ? Number(tEnv) : 0.5;
        if (!Number.isNaN(parsedTemp)) payload.temperature = parsedTemp;
        const startTs = Date.now();
        const resp = await axios.post('https://api.openai.com/v1/chat/completions', payload, {
          headers: { Authorization: `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
          timeout: 60000,
          validateStatus: () => true,
        });
        if (ANALYSIS_DEBUG && reqId) {
          const usage = resp?.data?.usage || {};
          log(reqId, `openai chat status=${resp.status} dt=${Date.now() - startTs}ms usage=${JSON.stringify(usage)}`);
        }
        if (resp.status >= 400) {
          const errMsg = resp?.data?.error?.message || `OpenAI HTTP ${resp.status}`;
          errorsCollected.push(errMsg);
        } else {
          text = resp?.data?.choices?.[0]?.message?.content || '';
        }
      }
      if (text && text.trim()) {
        const trimmed = text.trim();
        if (ANALYSIS_DEBUG && reqId) {
          log(reqId, `chunk ok text[0..120]=${JSON.stringify(trimmed.slice(0, 120))}`);
        }
        responses.push(trimmed);
      }
    } catch (err) {
      const msgErr = err.response?.data?.error?.message || err.message || err.toString();
      errorsCollected.push(msgErr);
      console.error('[ANALYSIS] Erro ao chamar OpenAI', msgErr);
    }
  }
  return { results: responses, errorsCollected };
}

/**
 * Gera um relatório em PDF a partir das conversas recentes de uma
 * instância UAZ. Retorna um Buffer com o conteúdo do PDF. Em caso de
 * erro ou ausência de mensagens novas, gera um PDF contendo uma
 * mensagem apropriada. A lógica de fallback (por exemplo, falta de
 * chave OpenAI) também está tratada aqui.
 *
 * @param {string} instanceId ID da instância UAZ
 * @param {string} slug Slug do cliente (prefixo da tabela)
 * @param {boolean} force Ignora o gate de última análise se true
 * @returns {Promise<Buffer>} PDF pronto para ser enviado ao cliente
 */
async function generateAnalysisPdf(instanceId, slug, force = true, opts = {}) {
  // Valida o slug (formato cliente_nome)
  if (!slug || !/^([a-z0-9_]+)$/.test(slug)) {
    return generatePdfBuffer('Cliente inválido.');
  }

  // Gera identificador de requisição (reqId) para logs; usa opts.reqId se fornecido
  const reqId = opts && opts.reqId ? opts.reqId : ('local-' + Date.now().toString(36));
  // Determina se o gate está habilitado
  const useGate = !force && (process.env.ANALYSIS_USE_LAST_GATE === 'true');

  // Checa a chave e o modelo da OpenAI
  const openaiKey = process.env.OPENAI_API_KEY;
  const model = process.env.OPENAI_MODEL || ANALYSIS_MODEL;
  if (!openaiKey || !model) {
    return generatePdfBuffer('Análise indisponível: OPENAI_API_KEY/OPENAI_MODEL não configurado.');
  }

  // Resolve token da instância
  const token = await resolveInstanceToken(instanceId);
  if (ANALYSIS_DEBUG) log(reqId, `instance ${instanceId} -> token=${maskToken(token)}`);
  if (!token) {
    return generatePdfBuffer('Instância não encontrada ou sem token.');
  }

  appendLog(`🟢 Início da análise - Cliente: ${slug}`);
  const startTime = Date.now();

  // Obtém última análise
  const lastTs = await getLastAnalysisTs(slug, useGate);

  // 1) Coleta e ordena chats
  const chats = await collectChats(token);

  // 2) Coleta mensagens
  const { messages: allMessages, maxTs } = await collectMessages(token, chats, lastTs, useGate);

  // Registra número total de chats e mensagens para fins de depuração
  const totalMsgs = allMessages.length;
  if (ANALYSIS_DEBUG) log(reqId, `chats=${chats.length} totalMsgs=${totalMsgs}`);

  if (!allMessages.length) {
    return generatePdfBuffer('Nenhuma mensagem nova para analisar.');
  }

  // Ordena em ordem crescente
  allMessages.sort((a, b) => a.timestamp - b.timestamp);
  const lines = buildTranscript(allMessages);
  // Prompt de sistema: permite override via variável de ambiente e prompt de override
  const systemPrompt = (process.env.OPENAI_SYSTEM_PROMPT || SYSTEM_PROMPT_OVERRIDE || DEFAULT_SYSTEM_PROMPT || '').toString().trim() ||
    'Você é um analista de desempenho conversacional. Gere sugestões práticas e diretas.';
  // Texto de introdução para o usuário
  const userIntro =
    `Contexto: A seguir estão amostras de conversas entre a assistente Luna e leads B2B do cliente "${slug}".` +
    `\nGere um relatório curto em português com tópicos práticos de melhoria, exemplos reescritos e um resumo executivo em até 3 linhas.`;
  const chunks = chunkTranscript(lines, systemPrompt, userIntro);
  appendLog(`→ Coletados ${chats.length} chats e ${lines.length} mensagens. Lotes: ${chunks.length}.`);

  let suggestions = '';
  let errors = [];
  {
    const { results, errorsCollected } = await callOpenAI(chunks, systemPrompt, userIntro, openaiKey, reqId);
    suggestions = results.join('\n\n---\n\n');
    errors = errorsCollected;
  }

  // Atualiza lastTs somente se gate ativo
  if (useGate && maxTs) {
    await updateLastAnalysisTs(slug, maxTs, useGate);
  }

  const elapsed = Date.now() - startTime;
  appendLog(`🏁 Fim da análise — ${chunks.length} lotes, tempo total ${elapsed}ms`);
  let finalText = suggestions || 'Nenhuma sugestão gerada.';
  if (!suggestions && errors && errors.length) {
    finalText = `Falha ao gerar sugestões.\nPrimeiro erro: ${errors[0]}`;
    if (ANALYSIS_DEBUG) log(reqId, `no suggestions. errors=${errors[0]}`);
  } else if (suggestions && ANALYSIS_DEBUG) {
    log(reqId, `final length=${suggestions.length}`);
  }
  return generatePdfBuffer(finalText);
}

module.exports = {
  generateAnalysisPdf,
};
