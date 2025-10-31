/*
 * src/utils/text.js
 *
 * Este módulo oferece utilidades para manipulação e análise de texto. São
 * funções pequenas, porém frequentemente utilizadas pelo servidor para
 * estimar a contagem de tokens, normalizar espaçamentos e construir
 * linhas de transcript a partir de mensagens de chat. Centralizar
 * essas funções aqui melhora a clareza do server.js.
 */

/**
 * Estima aproximadamente o número de tokens que uma string ocuparia
 * utilizando a heurística de que cada token ocupa em média 4 caracteres.
 *
 * @param {string} str
 * @returns {number}
 */
function approxTokens(str) {
  if (!str) return 0;
  return Math.ceil(String(str).length / 4);
}

/**
 * Remove espaços em excesso e normaliza quebras de linha de uma string,
 * retornando uma única linha com espaços simples entre as palavras.
 *
 * @param {string} s
 * @returns {string}
 */
function normalizeLine(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

/**
 * Constrói uma representação textual de uma mensagem para análise ou
 * geração de transcript. Inclui o timestamp e marca se a mensagem
 * veio do usuário ou do cliente.
 *
 * @param {Object} msg
 * @returns {string}
 */
function toTranscriptLine(msg) {
  const ts = msg?.messageTimestamp || msg?.timestamp || msg?.wa_timestamp || msg?.createdAt || msg?.date || '';
  const fromMe =
    msg?.fromMe === true ||
    msg?.sender?.fromMe === true ||
    msg?.me === true ||
    (msg?.key && msg.key.fromMe === true);
  const who = fromMe ? 'Usuário' : 'Cliente';
  let text =
    msg?.text ||
    msg?.body ||
    msg?.message ||
    (typeof msg?.content === 'string' ? msg.content : msg?.content?.text) ||
    msg?.caption ||
    '';
  text = normalizeLine(text);
  return `[${ts}] ${who}: ${text}`;
}

module.exports = { approxTokens, normalizeLine, toTranscriptLine };