/*
 * src/utils/schedule.js
 *
 * Funções de apoio para cálculo de horários e distribuição de mensagens
 * ao longo de uma janela de tempo. Separar esse código em um módulo
 * próprio permite reutilizar a lógica de forma isolada e reduz o
 * tamanho de server.js.
 */

/**
 * Converte uma string no formato HH:MM:SS para segundos desde o início
 * do dia. Valores ausentes ou inválidos resultam em 0.
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
 * Gera um array de atrasos (em segundos) para distribuir N mensagens
 * uniformemente aleatórias dentro do intervalo definido pelos horários
 * de início e fim. Caso a janela já tenha passado, retorna um array
 * vazio. O primeiro atraso é relativo ao horário atual.
 *
 * @param {number} count
 * @param {string} startStr
 * @param {string} endStr
 * @returns {number[]}
 */
function generateScheduleDelays(count, startStr, endStr) {
  const now = new Date();
  const nowSec   = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();
  const startSec = hmsToSeconds(startStr);
  const endSec   = hmsToSeconds(endStr);
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
    delays.push(i === 0 ? (effectiveStart - nowSec) + off : off - prev);
    prev = off;
  }
  return delays;
}

module.exports = { hmsToSeconds, generateScheduleDelays };