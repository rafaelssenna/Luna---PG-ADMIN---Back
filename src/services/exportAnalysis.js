/*
 * src/services/exportAnalysis.js
 *
 * Gera relatório em PDF com sugestões da IA a partir das conversas recentes.
 * Compatível com a API /v1/responses (modelos reasoning) e com chat/completions.
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

// ======= Debug / Logging =======
const ANALYSIS_DEBUG = String(process.env.ANALYSIS_DEBUG || process.env.DEBUG || 'false')
  .toLowerCase() === 'true';

function log(reqId, ...args) { try { console.log(`[ANALYSIS][${reqId}]`, ...args); } catch {} }
function maskToken(tok) { if (!tok || typeof tok !== 'string') return ''; return tok.length <= 8 ? '***' : tok.slice(0,4)+'…'+tok.slice(-4); }

// ======= UAZ / DB helpers =======
async function resolveInstanceToken(instanceId) {
  try {
    const data = await uaz.listInstances(UAZAPI_ADMIN_TOKEN);
    const list = Array.isArray(data?.content) ? data.content : Array.isArray(data) ? data : [];
    const it = list.find(x => String(x.id || x._id || x.instanceId || x.token) === String(instanceId));
    if (!it) {
      if (instanceId && typeof instanceId === 'string' && instanceId.length > 4) return instanceId; // fallback: ID é o próprio token
      return null;
    }
    return it.token || it.instanceToken || it.key || null;
  } catch (err) {
    console.error('Erro ao resolver token da instância', err);
    return null;
  }
}

async function getLastAnalysisTs(slug, useGate) {
  if (!useGate) return null;
  try {
    const r = await pool.query(`SELECT analysis_last_msg_ts FROM client_settings WHERE slug=$1`, [slug]);
    const ts = r.rows?.[0]?.analysis_last_msg_ts;
    return ts ? new Date(ts) : null;
  } catch (err) {
    console.warn('Falha ao obter analysis_last_msg_ts', slug, err?.message);
    return null;
  }
}

async function updateLastAnalysisTs(slug, ts, useGate) {
  if (!useGate || !ts) return;
  try {
    const iso = new Date(typeof ts === 'number' ? ts : ts.valueOf()).toISOString();
    await pool.query(`UPDATE client_settings SET analysis_last_msg_ts=$2 WHERE slug=$1`, [slug, iso]);
  } catch (err) {
    console.error('Erro ao atualizar analysis_last_msg_ts', slug, err?.message);
  }
}

// ======= Coleta UAZ =======
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
    if (chats.length >= ANALYSIS_MAX_CHATS * 2) break; // trava de segurança
  }
  const lastTs = c => c?.wa_lastTimestamp || c?.lastMessageTimestamp || c?.updatedAt || c?.createdAt || 0;
  chats.sort((a, b) => Number(lastTs(b)) - Number(lastTs(a)));
  return chats.slice(0, ANALYSIS_MAX_CHATS);
}

async function collectMessages(token, chats, lastTs, useGate) {
  const results = [];
  let maxTs = lastTs ? lastTs.getTime() : 0;

  for (const chat of chats) {
    const chatId = helpers.extractChatId(chat);
    if (!chatId) continue;

    const data = await uaz.findMessages(token, { chatid: chatId, limit: ANALYSIS_PER_CHAT_LIMIT, offset: 0 });
    const msgs = helpers.pickArrayList(data);

    msgs.sort((a, b) => {
      const ta = a?.messageTimestamp || a?.timestamp || a?.wa_timestamp || a?.createdAt || a?.date || 0;
      const tb = b?.messageTimestamp || b?.timestamp || b?.wa_timestamp || b?.createdAt || b?.date || 0;
      return Number(ta) - Number(tb);
    });

    for (const msg of msgs) {
      const rawTs = msg?.messageTimestamp || msg?.timestamp || msg?.wa_timestamp || msg?.createdAt || msg?.date || null;
      if (!rawTs) continue;
      let n = Number(rawTs);
      if (!Number.isFinite(n)) { const d = new Date(rawTs); n = d.valueOf(); }
      if (n && n < 10 ** 12) n *= 1000; // segundos -> ms

      if (!n) continue;
      if (useGate && lastTs && n <= lastTs.getTime()) continue;

      results.push({ timestamp: n, msg });
      if (n > maxTs) maxTs = n;
    }
  }

  return { messages: results, maxTs };
}

function buildTranscript(items) {
  return items.map(({ msg }) => toTranscriptLine(msg)).filter(Boolean);
}

function chunkTranscript(lines, systemPrompt, userIntro) {
  const chunks = [];
  const baseTokens = approxTokens(systemPrompt) + approxTokens(userIntro) + 50;
  let cur = [];
  let curTok = baseTokens;

  for (const line of lines) {
    const t = approxTokens(line) + 1;
    if (cur.length && curTok + t > ANALYSIS_INPUT_BUDGET) {
      chunks.push(cur.join('\n'));
      cur = [line];
      curTok = baseTokens + approxTokens(line);
    } else {
      cur.push(line);
      curTok += t;
    }
  }
  if (cur.length) chunks.push(cur.join('\n'));
  return chunks;
}

function isReasoningModelName(name) {
  const n = String(name || '').toLowerCase();
  return /(gpt-5|gpt-4o|omni)/i.test(n);
}

/**
 * Chama OpenAI (Responses API / Chat) e SEMPRE retorna { responses, errors }.
 * Sem 'response_format' para evitar 400 em contas/versões onde ele não é aceito.
 */
async function callOpenAI(chunks, systemPrompt, userIntro, openaiKey, reqId) {
  const responses = [];
  const errors = [];
  const model = process.env.OPENAI_MODEL || ANALYSIS_MODEL;
  const reasoning = isReasoningModelName(model);

  for (const contentBody of chunks) {
    const content = `${userIntro}\n\n${contentBody}`;

    try {
      let text = '';

      if (reasoning) {
        // Responses API — sem 'response_format'
        const payload = {
          model,
          input: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content },
          ],
          max_output_tokens: Number(process.env.OPENAI_OUTPUT_BUDGET || ANALYSIS_OUTPUT_BUDGET) || 1024,
          reasoning: { effort: process.env.OPENAI_REASONING_EFFORT || 'low' },
        };
        const temp = process.env.OPENAI_TEMPERATURE;
        if (temp !== undefined && !Number.isNaN(Number(temp))) payload.temperature = Number(temp);

        const t0 = Date.now();
        const resp = await axios.post('https://api.openai.com/v1/responses', payload, {
          headers: { Authorization: `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
          timeout: 60000,
          validateStatus: () => true,
        });
        if (ANALYSIS_DEBUG && reqId) {
          log(reqId, `responses status=${resp.status} dt=${Date.now()-t0}ms usage=${JSON.stringify(resp?.data?.usage || {})}`);
        }

        if (resp.status >= 400) {
          errors.push(resp?.data?.error?.message || `OpenAI HTTP ${resp.status}`);
        } else {
          // 1) texto direto
          if (typeof resp?.data?.output_text === 'string' && resp.data.output_text.trim()) {
            text = resp.data.output_text.trim();
          }
          // 2) output[].content[].text
          if (!text && Array.isArray(resp?.data?.output)) {
            const parts = [];
            for (const out of resp.data.output) {
              const arr = Array.isArray(out?.content) ? out.content : [];
              for (const c of arr) {
                if (typeof c?.text === 'string') parts.push(c.text);
                else if (typeof c === 'string') parts.push(c);
              }
            }
            text = parts.join('\n').trim();
          }
          // 3) choices (alguns backends ainda retornam)
          if (!text && Array.isArray(resp?.data?.choices)) {
            text = resp.data.choices
              .map(c => c?.message?.content || c?.text || '')
              .filter(Boolean)
              .join('\n')
              .trim();
          }
          // 4) fallback final: chat/completions
          if (!text) {
            const chatPayload = {
              model,
              messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content },
              ],
              max_tokens: Number(process.env.OPENAI_OUTPUT_BUDGET || ANALYSIS_OUTPUT_BUDGET) || 1024,
              n: 1,
            };
            const chat = await axios.post('https://api.openai.com/v1/chat/completions', chatPayload, {
              headers: { Authorization: `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
              timeout: 60000,
            });
            text = chat?.data?.choices?.[0]?.message?.content || '';
          }
        }
      } else {
        // Modelos clássicos: chat/completions
        const payload = {
          model,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content },
          ],
          max_tokens: Number(process.env.OPENAI_OUTPUT_BUDGET || ANALYSIS_OUTPUT_BUDGET) || 1024,
          n: 1,
        };
        const temp = process.env.OPENAI_TEMPERATURE;
        if (temp !== undefined && !Number.isNaN(Number(temp))) payload.temperature = Number(temp);

        const t0 = Date.now();
        const resp = await axios.post('https://api.openai.com/v1/chat/completions', payload, {
          headers: { Authorization: `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
          timeout: 60000,
          validateStatus: () => true,
        });
        if (ANALYSIS_DEBUG && reqId) {
          log(reqId, `chat status=${resp.status} dt=${Date.now()-t0}ms usage=${JSON.stringify(resp?.data?.usage || {})}`);
        }

        if (resp.status >= 400) {
          errors.push(resp?.data?.error?.message || `OpenAI HTTP ${resp.status}`);
        } else {
          text = resp?.data?.choices?.[0]?.message?.content || '';
        }
      }

      if (typeof text === 'string' && text.trim()) {
        const clean = text.trim();
        if (ANALYSIS_DEBUG && reqId) log(reqId, `chunk -> ${clean.length} chars`);
        responses.push(clean);
      } else {
        errors.push('Resposta vazia do modelo');
      }
    } catch (err) {
      const msg = err?.response?.data?.error?.message || err?.message || String(err);
      console.error('[ANALYSIS] OpenAI error:', msg);
      errors.push(msg);
    }
  }

  return { responses, errors };
}

// ======= Orquestração / PDF =======
async function generateAnalysisPdf(instanceId, slug, force = true, opts = {}) {
  if (!slug || !/^([a-z0-9_]+)$/.test(slug)) return generatePdfBuffer('Cliente inválido.');

  const reqId = opts?.reqId || ('local-' + Date.now().toString(36));
  const useGate = !force && (process.env.ANALYSIS_USE_LAST_GATE === 'true');

  const openaiKey = process.env.OPENAI_API_KEY;
  const model = process.env.OPENAI_MODEL || ANALYSIS_MODEL;
  if (!openaiKey || !model) {
    return generatePdfBuffer('Análise indisponível: OPENAI_API_KEY/OPENAI_MODEL não configurado.');
  }

  const token = await resolveInstanceToken(instanceId);
  if (ANALYSIS_DEBUG) log(reqId, `instance ${instanceId} -> token=${maskToken(token)}`);
  if (!token) return generatePdfBuffer('Instância não encontrada ou sem token.');

  appendLog(`🟢 Início da análise - Cliente: ${slug}`);
  const t0 = Date.now();

  const lastTs = await getLastAnalysisTs(slug, useGate);

  const chats = await collectChats(token);
  const { messages: allMessages, maxTs } = await collectMessages(token, chats, lastTs, useGate);
  if (ANALYSIS_DEBUG) log(reqId, `chats=${chats.length} totalMsgs=${allMessages.length}`);
  if (!allMessages.length) return generatePdfBuffer('Nenhuma mensagem nova para analisar.');

  allMessages.sort((a, b) => a.timestamp - b.timestamp);
  const lines = buildTranscript(allMessages);

  const systemPrompt =
    (process.env.OPENAI_SYSTEM_PROMPT || SYSTEM_PROMPT_OVERRIDE || DEFAULT_SYSTEM_PROMPT || '')
      .toString()
      .trim() ||
    'Você é um analista de desempenho conversacional. Gere sugestões práticas e diretas.';

  const userIntro =
    `Contexto: A seguir estão amostras de conversas entre a assistente Luna e leads B2B do cliente "${slug}".` +
    `\nGere um relatório curto em português com tópicos práticos de melhoria, exemplos reescritos e um resumo executivo em até 3 linhas.`;

  const chunks = chunkTranscript(lines, systemPrompt, userIntro);
  appendLog(`→ Coletados ${chats.length} chats e ${lines.length} mensagens. Lotes: ${chunks.length}.`);

  const { responses, errors: callErrors } = await callOpenAI(chunks, systemPrompt, userIntro, openaiKey, reqId);
  const suggestions = Array.isArray(responses) ? responses.join('\n\n---\n\n') : '';

  if (useGate && maxTs) await updateLastAnalysisTs(slug, maxTs, useGate);

  appendLog(`🏁 Fim da análise — ${chunks.length} lotes, tempo total ${Date.now()-t0}ms`);

  let finalText = suggestions || 'Nenhuma sugestão gerada.';
  if (!suggestions && callErrors && callErrors.length) {
    finalText = `Falha ao gerar sugestões.\nPrimeiro erro: ${callErrors[0]}`;
    if (ANALYSIS_DEBUG) log(reqId, `no suggestions: ${callErrors[0]}`);
  }

  return generatePdfBuffer(finalText);
}

module.exports = { generateAnalysisPdf };
