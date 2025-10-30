/**
 * helpers.js
 *
 * Este módulo contém funções utilitárias compartilhadas por várias partes
 * da aplicação. Extraí-las para cá ajuda a manter o código do servidor
 * organizado e facilita a reutilização e testes isolados.
 */

/**
 * Extrai o identificador de chat a partir de um objeto de chat. A UAZAPI
 * retorna múltiplos campos possíveis para o ID; esta função centraliza a
 * lógica de fallback.
 *
 * @param {Object} chat
 * @returns {string|null}
 */
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

/**
 * Converte diferentes formatos de retorno de lista da UAZAPI em um array
 * consistente. Algumas chamadas retornam `content`, outras `chats` ou
 * `messages`. Esta função escolhe o primeiro campo de array válido.
 *
 * @param {any} data
 * @returns {Array}
 */
function pickArrayList(data) {
  if (Array.isArray(data?.content)) return data.content;
  if (Array.isArray(data?.chats)) return data.chats;
  if (Array.isArray(data?.messages)) return data.messages;
  if (Array.isArray(data?.data)) return data.data;
  if (Array.isArray(data)) return data;
  return [];
}

/**
 * Normaliza um status de instância retornado pela UAZAPI. Se o status
 * contiver a propriedade `connected`, apenas garante que seja booleano;
 * caso contrário, verifica se a string JSON contém "connected":true ou
 * "online".
 *
 * @param {any} status
 * @returns {Object}
 */
function normalizeStatus(status) {
  if (!status || typeof status !== 'object') return { connected: false };
  if (typeof status.connected !== 'undefined') return { ...status, connected: !!status.connected };
  const s = JSON.stringify(status || {}).toLowerCase();
  return { ...status, connected: s.includes('"connected":true') || s.includes('online') };
}

/**
 * Resolve a URL de avatar a partir de um objeto retornado pela UAZAPI.
 * Diferentes campos podem conter a imagem de perfil; esta função tenta
 * todos os campos conhecidos.
 *
 * @param {any} obj
 * @returns {string|null}
 */
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

/**
 * Converte uma string HH:MM:SS em segundos inteiros.
 *
 * @param {string} hms
 * @returns {number}
 */
function hmsToSeconds(hms) {
  const parts = String(hms || '').split(':').map((p) => parseInt(p, 10) || 0);
  const [h, m, s] = [parts[0] || 0, parts[1] || 0, parts[2] || 0];
  return h * 3600 + m * 60 + s;
}

/**
 * Gera atrasos aleatórios para agendamento de envios dentro de uma janela
 * diária. Retorna um array de segundos a esperar entre cada envio.
 *
 * @param {number} count        Número total de mensagens a agendar
 * @param {string} startStr     Hora de início (HH:MM:SS)
 * @param {string} endStr       Hora de fim (HH:MM:SS)
 * @returns {Array<number>}
 */
function generateScheduleDelays(count, startStr, endStr) {
  const now = new Date();
  const nowSec = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();
  const startSec = hmsToSeconds(startStr);
  const endSec = hmsToSeconds(endStr);
  const effectiveStart = Math.max(nowSec, startSec);
  if (endSec <= effectiveStart) return [];
  const span = endSec - effectiveStart;
  const msgCount = Math.min(count, span);
  const offsets = new Set();
  while (offsets.size < msgCount) offsets.add(Math.floor(Math.random() * (span + 1)));
  const sortedOffsets = Array.from(offsets).sort((a, b) => a - b);
  const delays = [];
  let prev = 0;
  for (let i = 0; i < sortedOffsets.length; i++) {
    const off = sortedOffsets[i];
    delays.push(i === 0 ? effectiveStart - nowSec + off : off - prev);
    prev = off;
  }
  return delays;
}

/**
 * Valida o slug do cliente. Um slug deve ser composto por letras minúsculas,
 * números e underscores, podendo opcionalmente começar com "cliente_".
 *
 * @param {string} slug
 * @returns {boolean}
 */
function validateSlug(slug) {
  return /^cliente_[a-z0-9_]+$/.test(slug) || /^[a-z0-9_]+$/.test(slug);
}

module.exports = {
  extractChatId,
  pickArrayList,
  normalizeStatus,
  resolveAvatar,
  hmsToSeconds,
  generateScheduleDelays,
  validateSlug,
};