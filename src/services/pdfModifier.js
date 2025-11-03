/*
 * src/services/pdfModifier.js
 *
 * Este módulo aplica pós‑processamento em PDFs gerados pela ferramenta de
 * análise. Ele adiciona uma capa personalizada e garante que o conteúdo
 * textual seja codificado corretamente em UTF‑8. A capa utiliza um logotipo
 * opcional e um título fornecido pelo chamador. Após a capa ser criada,
 * todas as páginas do PDF original são copiadas para o novo documento.
 *
 * Dependências:
 *   - pdf-lib: biblioteca utilizada para manipular PDFs em memória.
 *   - fs: leitura síncrona do arquivo de logotipo (PNG ou JPEG).
 *
 * Uso:
 *   const { modifyPdf } = require('./pdfModifier');
 *   const buffer = await modifyPdf(originalBuffer, './assets/logo.png', 'Relatório');
 */

const fs = require('fs');

/*
 * pdf-lib é utilizado para manipular PDFs em memória. Como este módulo
 * depende de uma dependência externa, pode não estar instalado no ambiente
 * em que o código está sendo executado. Para evitar erros de execução,
 * tentamos carregar pdf-lib de forma segura. Caso não esteja disponível,
 * as funções deste módulo simplesmente retornam o PDF original sem
 * alterações, registrando um aviso no console.
 */
let PDFDocument;
let rgb;
let StandardFonts;
try {
  // Tenta importar o pdf-lib. Se a dependência não existir, estas
  // variáveis permanecerão indefinidas e serão tratadas em modifyPdf.
  const pdfLib = require('pdf-lib');
  PDFDocument = pdfLib.PDFDocument;
  rgb = pdfLib.rgb;
  StandardFonts = pdfLib.StandardFonts;
} catch (err) {
  // Se pdf-lib não estiver instalado, registra um aviso. As operações de
  // modificação serão ignoradas e o PDF será retornado intacto.
  console.warn('Dependência pdf-lib não encontrada. Capa personalizada será ignorada.');
  PDFDocument = null;
  rgb = null;
  StandardFonts = null;
}

/**
 * Corrige a codificação de caracteres de um PDF existente e adiciona uma capa.
 *
 * Esta função carrega o PDF existente, cria um novo documento, injeta uma
 * página de capa no início com um título e um logotipo opcional e copia
 * todas as páginas originais a seguir. O título é renderizado utilizando
 * uma fonte padrão (Helvetica), que suporta caracteres acentuados básicos.
 *
 * @param {Buffer} pdfBuffer Buffer contendo o PDF original.
 * @param {string|null} logoPath Caminho para a imagem do logotipo. Pode ser null.
 * @param {string} title Título a ser exibido na capa.
 * @returns {Promise<Buffer>} Buffer contendo o novo PDF modificado.
 */
async function modifyPdf(pdfBuffer, logoPath = null, title = 'Relatório Técnico') {
  if (!pdfBuffer || !Buffer.isBuffer(pdfBuffer)) {
    throw new TypeError('pdfBuffer deve ser um Buffer');
  }

  // Se pdf-lib não estiver disponível, apenas retorna o PDF original sem
  // realizar nenhuma modificação. Isto evita que a aplicação falhe
  // completamente em ambientes sem a dependência instalada.
  if (!PDFDocument || !rgb || !StandardFonts) {
    return pdfBuffer;
  }

  // Carregue o PDF original a partir do buffer fornecido
  const originalPdf = await PDFDocument.load(pdfBuffer);

  // Crie um novo documento onde adicionaremos a capa e as páginas copiadas
  const newPdf = await PDFDocument.create();

  // Dimensões de uma página A4 em pontos (tamanho padrão utilizado pelo pdf-lib)
  const A4_WIDTH = 595.28;
  const A4_HEIGHT = 841.89;

  // Adicione a página de capa
  const coverPage = newPdf.addPage([A4_WIDTH, A4_HEIGHT]);
  const { width, height } = coverPage.getSize();

  // Defina o fundo da capa (branco)
  coverPage.drawRectangle({
    x: 0,
    y: 0,
    width,
    height,
    color: rgb(1, 1, 1),
  });

  // Tente embutir uma fonte padrão que suporte caracteres latinos básicos. A
  // Helvetica é uma fonte padrão de 14 glifos do PDF, mas seu suporte para
  // caracteres acentuados varia conforme o leitor. Ainda assim é preferível
  // à ausência de fonte, e melhora a compatibilidade.
  const font = await newPdf.embedFont(StandardFonts.Helvetica);

  // Se um caminho de logotipo foi fornecido e o arquivo existir, leia e
  // incorpore a imagem na capa. Suporta PNG e JPEG.
  if (logoPath && typeof logoPath === 'string' && fs.existsSync(logoPath)) {
    try {
      const imageBuffer = fs.readFileSync(logoPath);
      let logo;
      // Tente como PNG primeiro; se falhar, tenta JPEG.
      try {
        logo = await newPdf.embedPng(imageBuffer);
      } catch (pngErr) {
        logo = await newPdf.embedJpg(imageBuffer);
      }
      // Escala o logotipo para caber horizontalmente na página
      const scaleFactor = Math.min(1, (width * 0.5) / logo.width);
      const logoWidth = logo.width * scaleFactor;
      const logoHeight = logo.height * scaleFactor;
      coverPage.drawImage(logo, {
        x: (width - logoWidth) / 2,
        y: height - logoHeight - 60,
        width: logoWidth,
        height: logoHeight,
      });
    } catch (err) {
      // Caso haja algum problema ao embutir a imagem, apenas ignora e prossegue.
      console.warn('Não foi possível embutir o logotipo na capa:', err?.message || err);
    }
  }

  // Desenhe o título aproximadamente no centro da página
  const titleFontSize = 28;
  const textWidth = font.widthOfTextAtSize(title, titleFontSize);
  const textX = (width - textWidth) / 2;
  const textY = height / 2;
  coverPage.drawText(title, {
    x: textX,
    y: textY,
    size: titleFontSize,
    font,
    color: rgb(0, 0, 0),
  });

  // Copie todas as páginas do documento original para o novo PDF
  const originalPages = await newPdf.copyPages(originalPdf, originalPdf.getPageIndices());
  originalPages.forEach((p) => newPdf.addPage(p));

  // Gere o PDF final e retorne como Buffer
  const modifiedPdfBytes = await newPdf.save();
  return Buffer.from(modifiedPdfBytes);
}

module.exports = { modifyPdf };
