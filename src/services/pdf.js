/*
 * src/utils/pdf.js
 *
 * Este módulo fornece funções utilitárias para geração de PDFs simples.
 * Ele contém um gerador de PDF mínimo que constrói um Buffer contendo
 * um arquivo PDF A4 com texto básico e uma função para escapar
 * caracteres especiais no texto. Ao mover essa lógica para fora do
 * server.js, reduzimos o tamanho do arquivo principal e facilitamos
 * testes e reutilização.
 */

/**
 * Escapa parênteses e barras invertidas de uma string para uso em
 * literais de texto de PDFs. A especificação PDF requer que
 * caracteres ( ) e \ sejam escapados com uma barra invertida.
 *
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
 * Gera um Buffer contendo um PDF A4 com o texto fornecido. O PDF
 * produzido é extremamente simples: todas as linhas são desenhadas
 * usando a fonte Helvetica tamanho 12, começando na coordenada (50,750)
 * e descendo 14 pontos por linha. Caso as linhas excedam o espaço
 * vertical, as linhas restantes são descartadas (não há novas páginas).
 *
 * Esta versão especifica explicitamente a codificação WinAnsiEncoding
 * para a fonte embutida. Isso garante que caracteres acentuados
 * (como ç, ã, õ, á, é etc.) sejam mapeados corretamente para os
 * glifos disponíveis na fonte Helvetica padrão. Sem essa especificação,
 * o PDF viewer usaria StandardEncoding, que não contempla boa parte
 * dos caracteres de línguas latinas.
 *
 * @param {string} text
 * @returns {Buffer}
 */
function generatePdfBuffer(text) {
  const lines = String(text || '').split(/\r?\n/);
  let y = 750;
  let textCommands = '';
  for (const line of lines) {
    const escaped = escapePdfString(line);
    textCommands += `BT 50 ${y} Td (${escaped}) Tj ET\n`;
    y -= 14;
    if (y < 50) {
      break;
    }
  }
  const contentStream = `/F1 12 Tf\n` + textCommands;
  const streamLength = Buffer.byteLength(contentStream, 'utf8');
  const obj1 = '1 0 obj<< /Type /Catalog /Pages 2 0 R >>\nendobj\n';
  const obj2 = '2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n';
  const obj3 = '3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 5 0 R /Resources << /Font << /F1 4 0 R >> >> >>\nendobj\n';
  // Utilize WinAnsiEncoding para mapear caracteres estendidos corretamente.  Sem especificar
  // essa codificação, a fonte padrão usa StandardEncoding, que tem suporte
  // limitado a caracteres acentuados.
  const obj4 = '4 0 obj<< /Type /Font /Subtype /Type1 /Name /F1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>\nendobj\n';
  const obj5 = `5 0 obj<< /Length ${streamLength} >>\nstream\n${contentStream}\nendstream\nendobj\n`;
  const header = '%PDF-1.4\n';
  const objects = [obj1, obj2, obj3, obj4, obj5];
  let pos = header.length;
  const offsets = [0];
  for (const obj of objects) {
    offsets.push(pos);
    pos += Buffer.byteLength(obj, 'latin1');
  }
  const xrefPos = pos;
  let xref = 'xref\n0 6\n0000000000 65535 f \n';
  for (const off of offsets.slice(1)) {
    const padded = String(off).padStart(10, '0');
    xref += `${padded} 00000 n \n`;
  }
  const trailer = `trailer<< /Size 6 /Root 1 0 R >>\nstartxref\n${xrefPos}\n%%EOF`;
  const pdfString = header + obj1 + obj2 + obj3 + obj4 + obj5 + xref + trailer;
  return Buffer.from(pdfString, 'utf8');
}

module.exports = { escapePdfString, generatePdfBuffer };
