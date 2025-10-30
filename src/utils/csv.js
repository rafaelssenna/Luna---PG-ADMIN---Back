/*
 * src/utils/csv.js
 *
 * Este módulo reúne funções auxiliares relacionadas ao processamento de
 * arquivos CSV. Ao deslocar a lógica de detecção de delimitador,
 * normalização de cabeçalhos e parsing de linhas para este arquivo,
 * podemos reutilizar o código em diferentes partes da aplicação e
 * manter o servidor principal mais enxuto. Todas as funções aqui
 * exportadas são puras e não dependem de estado externo.
 */

/**
 * Normaliza uma string para facilitar comparações de cabeçalhos. Faz
 * minúsculas, remove acentos e recorta espaços extras. Útil para
 * identificar campos como "nome", "telefone" ou "nicho" em CSVs.
 *
 * @param {string} s
 * @returns {string}
 */
function norm(s) {
  return (s ?? '')
    .toString()
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

/**
 * Detecta se a primeira linha de um CSV usa vírgulas ou ponto e vírgula
 * como delimitador. Conta a ocorrência de cada caractere e escolhe o
 * delimitador mais frequente. Caso ambos sejam iguais, assume vírgula.
 *
 * @param {string} firstLine
 * @returns {';' | ','}
 */
function detectDelimiter(firstLine) {
  const commas = (firstLine.match(/,/g) || []).length;
  const semis  = (firstLine.match(/;/g) || []).length;
  return semis > commas ? ';' : ',';
}

/**
 * Realiza o parse de um texto CSV para um array de linhas, cada uma
 * representada por um array de células. Suporta valores entre aspas
 * contendo delimitadores ou quebras de linha. Não depende de
 * bibliotecas externas.
 *
 * @param {string} text
 * @param {string} delim
 * @returns {string[][]}
 */
function parseCSV(text, delim) {
  const rows = [];
  let row = [], val = '', inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') { val += '"'; i++; }
      else { inQuotes = !inQuotes; }
      continue;
    }
    if (ch === '\r') continue;
    if (ch === '\n' && !inQuotes) {
      row.push(val);
      rows.push(row);
      row = [];
      val = '';
      continue;
    }
    if (ch === delim && !inQuotes) {
      row.push(val);
      val = '';
      continue;
    }
    val += ch;
  }
  if (val.length > 0 || row.length > 0) {
    row.push(val);
    rows.push(row);
  }
  return rows;
}

/**
 * Mapeia as posições dos campos de interesse em um array de cabeçalhos
 * normalizados. Procura por variações de "nome", "telefone" e
 * "nicho" (incluindo inglês e abreviações) e retorna seus índices.
 * Se um campo não for encontrado, permanece -1.
 *
 * @param {string[]} headerCells
 * @returns {{ name: number, phone: number, niche: number }}
 */
function mapHeader(headerCells) {
  const idx = { name: -1, phone: -1, niche: -1 };
  const names = headerCells.map((h) => norm(h));
  const isId = (h) => ['id', 'identificador', 'codigo', 'código'].includes(h);
  const nameKeys  = new Set(['nome','name','full_name','fullname','contato','empresa','nomefantasia','razaosocial']);
  const phoneKeys = new Set(['telefone','numero','número','phone','whatsapp','celular','mobile','telemovel']);
  const nicheKeys = new Set(['nicho','niche','segmento','categoria','industry']);
  names.forEach((h, i) => {
    if (isId(h)) return;
    if (idx.name  === -1 && nameKeys.has(h))  idx.name  = i;
    if (idx.phone === -1 && phoneKeys.has(h)) idx.phone = i;
    if (idx.niche === -1 && nicheKeys.has(h)) idx.niche = i;
  });
  return idx;
}

module.exports = { norm, detectDelimiter, parseCSV, mapHeader };