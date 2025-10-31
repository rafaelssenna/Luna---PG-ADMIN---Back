/**
 * logger.js
 *
 * Pequeno utilitário para centralizar logs de análise. Escreve mensagens com carimbo
 * de data e hora no arquivo logs/analysis.log. Se o diretório não existir, cria-o.
 */

const fs = require('fs');
const path = require('path');

/**
 * Escreve uma linha no arquivo de log de análise. Cada linha é prefixada com a
 * data e hora no formato ISO (sem fuso).
 *
 * @param {string} message Mensagem a registrar no log.
 */
function appendLog(message) {
  try {
    const logDir = path.join(__dirname, '..', 'logs');
    // Assegura que o diretório exista
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
    const filePath = path.join(logDir, 'analysis.log');
    const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
    const line = `[${timestamp}] ${message}\n`;
    fs.appendFileSync(filePath, line, { encoding: 'utf-8' });
  } catch (err) {
    // Em caso de erro ao gravar no log, apenas exibe no console para não
    // comprometer a requisição principal.
    console.error('Falha ao escrever no log de análise', err);
  }
}

module.exports = { appendLog };