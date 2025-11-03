/*
 * src/utils/pdf.js
 *
 * Geração de PDF A4 simples, com:
 *   - escape seguro de texto para sintaxe PDF;
 *   - quebra automática de linha (word-wrap) por coluna;
 *   - paginação (margens superior/inferior);
 *   - fonte Helvetica com /Encoding /WinAnsiEncoding para acentuação correta;
 *   - múltiplas páginas usando objetos /Pages e /Page.
 *
 * Sem dependências externas.
 */

/**
 * Escapa parênteses e barras invertidas para literais de texto do PDF.
 * @param {string} str
 * @returns {string}
 */
function escapePdfString(str) {
  return String(str)
    .replace(/\\/g, '\\\\')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)');
}

/**
 * Quebra uma string em linhas com limite de caracteres por coluna.
 * Aproximação por coluna (caracteres), suficiente para Helvetica 12pt em ~500pt de largura útil.
 * Palavras maiores que o limite são quebradas (fallback).
 * @param {string} text
 * @param {number} maxChars
 * @returns {string[]}
 */
function wrapTextByColumns(text, maxChars) {
  const out = [];
  const rawLines = String(text || '').split(/\r?\n/);

  for (const raw of rawLines) {
    const line = String(raw || '').trimEnd();
    if (line.length === 0) {
      out.push('');
      continue;
    }
    const words = line.split(/\s+/);
    let cur = '';

    const pushLine = () => {
      out.push(cur);
      cur = '';
    };

    for (let w of words) {
      // palavra isolada maior que o limite -> quebra forçada
      while (w.length > maxChars) {
        const head = w.slice(0, maxChars);
        const tail = w.slice(maxChars);
        if (cur) pushLine();
        out.push(head);
        w = tail;
      }
      const test = cur ? cur + ' ' + w : w;
      if (test.length > maxChars) {
        pushLine();
        cur = w;
      } else {
        cur = test;
      }
    }
    if (cur) pushLine();
  }

  return out;
}

/**
 * Gera um Buffer contendo um PDF A4 com o texto fornecido.
 * Implementa quebra automática de linha e paginação,
 * usando Helvetica 12pt com /Encoding /WinAnsiEncoding (acentos corretos).
 *
 * @param {string} text
 * @returns {Buffer}
 */
function generatePdfBuffer(text) {
  // Configurações de layout
  const PAGE_WIDTH = 612;   // A4 em pontos (72dpi)
  const PAGE_HEIGHT = 792;  // A4
  const MARGIN_LEFT = 50;
  const MARGIN_TOP = 750;
  const MARGIN_BOTTOM = 50;
  const LINE_HEIGHT = 14;
  const MAX_COL_CHARS = 92; // largura aproximada para Helvetica 12pt em ~512pt úteis

  // 1) Quebra de linhas por colunas
  const wrappedLines = wrapTextByColumns(text, MAX_COL_CHARS);

  // 2) Monta streams de conteúdo por página
  const pageStreams = [];
  let y = MARGIN_TOP;
  let curStream = '/F1 12 Tf\n';

  const emitLine = (ln) => {
    const escaped = escapePdfString(ln);
    curStream += `BT ${MARGIN_LEFT} ${y} Td (${escaped}) Tj ET\n`;
    y -= LINE_HEIGHT;
  };

  for (const ln of wrappedLines) {
    // linha em branco = quebra visual
    if (ln === '') {
      emitLine(' ');
    } else {
      emitLine(ln);
    }

    // nova página se necessário
    if (y < MARGIN_BOTTOM) {
      pageStreams.push(curStream);
      curStream = '/F1 12 Tf\n';
      y = MARGIN_TOP;
    }
  }
  // fecha a última página
  pageStreams.push(curStream);

  // 3) Constrói objetos PDF
  // IDs:
  // 1: Catalog
  // 2: Pages
  // 3: Font (Helvetica + WinAnsiEncoding)
  // A partir de 4: para cada página, par de objetos [Page, Contents]
  const objects = [];

  // obj1: Catalog
  const obj1 = '1 0 obj<< /Type /Catalog /Pages 2 0 R >>\nendobj\n';
  objects.push(obj1);

  // Placeholder de /Pages (preenche Kids depois)
  // Será atualizado após conhecermos os IDs das páginas
  let obj2 = null;

  // obj3: Font com WinAnsiEncoding
  const obj3 =
    '3 0 obj<< /Type /Font /Subtype /Type1 /Name /F1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>\nendobj\n';
  objects.push(obj3);

  // Para montar /Kids precisaremos dos IDs das páginas (que serão 4,6,8,...)
  const kidsIds = [];
  const pageObjs = [];
  const contentObjs = [];

  // Próximo ID disponível
  let nextId = 4;

  for (let i = 0; i < pageStreams.length; i++) {
    const pageId = nextId++;
    const contentId = nextId++;

    kidsIds.push(`${pageId} 0 R`);

    // Página
    const pageObj =
      `${pageId} 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${PAGE_WIDTH} ${PAGE_HEIGHT}] ` +
      `/Contents ${contentId} 0 R /Resources << /Font << /F1 3 0 R >> >> >>\nendobj\n`;
    pageObjs.push(pageObj);

    // Stream de conteúdo (calcular length em latin1 para bater com WinAnsi)
    const stream = pageStreams[i];
    const streamLength = Buffer.byteLength(stream, 'latin1');
    const contentObj =
      `${contentId} 0 obj<< /Length ${streamLength} >>\nstream\n${stream}\nendstream\nendobj\n`;
    contentObjs.push(contentObj);
  }

  // Agora podemos montar o objeto /Pages com a lista de filhos
  obj2 =
    `2 0 obj<< /Type /Pages /Kids [${kidsIds.join(' ')}] /Count ${kidsIds.length} >>\nendobj\n`;
  objects.splice(1, 0, obj2); // garantir ordem 1,2,3,...

  // Empilha páginas e conteúdos
  for (let i = 0; i < pageObjs.length; i++) {
    objects.push(pageObjs[i]);
    objects.push(contentObjs[i]);
  }

  // 4) Monta xref e trailer
  const header = '%PDF-1.4\n';
  let pdf = header;
  const offsets = [0]; // obj 0 é especial (free)
  let pos = Buffer.byteLength(header, 'latin1');

  for (const obj of objects) {
    offsets.push(pos);
    pos += Buffer.byteLength(obj, 'latin1');
    pdf += obj;
  }

  const xrefPos = pos;
  let xref = `xref\n0 ${offsets.length}\n0000000000 65535 f \n`;
  for (let i = 1; i < offsets.length; i++) {
    const padded = String(offsets[i]).padStart(10, '0');
    xref += `${padded} 00000 n \n`;
  }
  const trailer =
    `trailer<< /Size ${offsets.length} /Root 1 0 R >>\nstartxref\n${xrefPos}\n%%EOF`;

  pdf += xref + trailer;

  // 5) Retorna buffer em latin1 (compatível com WinAnsiEncoding)
  return Buffer.from(pdf, 'latin1');
}

module.exports = { escapePdfString, generatePdfBuffer };
