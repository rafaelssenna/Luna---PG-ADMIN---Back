/*
 * src/utils/progress.js
 *
 * Centraliza funções auxiliares relacionadas ao rastreamento do
 * progresso de importações e loops por cliente. As estruturas de
 * estado (progressEmitters, progressStates) são fornecidas pelo
 * módulo de configuração global. Separar essa lógica facilita a
 * reutilização em outros módulos e reduz o tamanho do server.js.
 */

const { progressEmitters, progressStates } = require('../config');

/**
 * Obtém (ou cria, se necessário) um EventEmitter para um slug de
 * cliente. Emitters são usados para enviar eventos de progresso por
 * SSE ao frontend.
 *
 * @param {string} slug
 * @returns {EventEmitter}
 */
function getEmitter(slug) {
  if (!progressEmitters.has(slug)) progressEmitters.set(slug, new (require('events'))());
  return progressEmitters.get(slug);
}

/**
 * Inicializa o estado de progresso para uma nova operação, definindo
 * o total de itens esperados e limpando eventos anteriores.
 *
 * @param {string} slug
 * @param {number} total
 */
function snapshotStart(slug, total) {
  progressStates.set(slug, {
    lastStart: { type: 'start', total, at: new Date().toISOString() },
    items: [],
    lastEnd: null,
  });
}

/**
 * Adiciona um evento intermediário ao snapshot de progresso. Mantém
 * apenas os últimos 200 eventos para evitar crescimento ilimitado.
 *
 * @param {string} slug
 * @param {Object} evt
 */
function snapshotPush(slug, evt) {
  const st = progressStates.get(slug);
  if (!st) return;
  st.items.push(evt);
  if (st.items.length > 200) st.items.shift();
}

/**
 * Finaliza o snapshot de progresso indicando quantos itens foram
 * processados e quaisquer campos extras fornecidos.
 *
 * @param {string} slug
 * @param {number} processed
 * @param {Object} [extra]
 */
function snapshotEnd(slug, processed, extra = {}) {
  const st = progressStates.get(slug);
  if (!st) return;
  st.lastEnd = { type: 'end', processed, ...extra, at: new Date().toISOString() };
}

module.exports = { getEmitter, snapshotStart, snapshotPush, snapshotEnd };