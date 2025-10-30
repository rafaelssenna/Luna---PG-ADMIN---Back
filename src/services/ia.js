/*
 * src/services/ia.js
 *
 * Este módulo isola toda a lógica necessária para enviar mensagens via UAZAPI
 * utilizando inteligência artificial. Ao extrair essas funções para um
 * serviço dedicado, o arquivo principal do servidor fica mais enxuto e mais
 * fácil de manter. As funções aqui exportadas não dependem de objetos
 * Express ou de estado global específico, recebendo os parâmetros
 * necessários diretamente.
 */

const { URLSearchParams } = require('url');

// Conjunto de variáveis de configuração relacionadas à UAZAPI. É lido das
// variáveis de ambiente no momento da importação. Este objeto agrupa
// parâmetros que determinam como a requisição ao serviço será montada.
const UAZ = {
  token: process.env.UAZAPI_TOKEN || '',
  authHeader: process.env.UAZAPI_AUTH_HEADER || 'token',
  authScheme: process.env.UAZAPI_AUTH_SCHEME ?? '',
  phoneField: process.env.UAZAPI_PHONE_FIELD || 'phone',
  textField: process.env.UAZAPI_TEXT_FIELD || 'message',
  digitsOnly: (process.env.UAZAPI_PHONE_DIGITS_ONLY || 'true') === 'true',
  payloadStyle: (process.env.UAZAPI_PAYLOAD_STYLE || 'json').toLowerCase(),
  methodPref: (process.env.UAZAPI_METHOD || 'post').toLowerCase(),
  extra: (() => {
    try { return JSON.parse(process.env.UAZAPI_EXTRA || '{}'); }
    catch { return {}; }
  })(),
  template: process.env.MESSAGE_TEMPLATE || 'Olá {NAME}, aqui é do {CLIENT}.',
};

/**
 * Normaliza um número de telefone brasileiro para o formato E.164. Aceita
 * entradas com ou sem código do país e remove todos os caracteres não
 * numéricos. Se o número possuir 11 dígitos e não começar com 55, assume
 * código de país 55 (Brasil).
 *
 * @param {string} phone
 * @returns {string}
 */
function normalizePhoneE164BR(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (!digits) return '';
  if (digits.startsWith('55')) return `+${digits}`;
  if (digits.length === 11) return `+55${digits}`;
  return `+${digits}`;
}

/**
 * Substitui campos do template por variáveis fornecidas. Aceita chaves em
 * português e inglês, converte para maiúsculas e mapeia variantes para
 * chaves padrão (NAME, CLIENT, PHONE, NICHO).
 *
 * @param {string} tpl
 * @param {Object} vars
 * @returns {string}
 */
function fillTemplate(tpl, vars) {
  // Gera saudação automática conforme horário atual
  const now = new Date();
  const hour = now.getHours();
  let saudacao;
  if (hour >= 5 && hour < 12) saudacao = "Bom dia ";
  else if (hour >= 12 && hour < 18) saudacao = "Boa tarde ";
  else saudacao = "Boa noite ";

  // Adiciona ao mapa de variáveis
  vars = { ...vars, SAUDACAO: saudacao };

  // Substitui campos do template por variáveis fornecidas
  return String(tpl || "").replace(
    /\{(NAME|NOME|CLIENT|CLIENTE|PHONE|TELEFONE|NICHO|NICHE|SAUDACAO|GREETING)\}/gi,
    (_, k) => {
      const key = k.toUpperCase();
      const map = {
        NOME: "NAME",
        CLIENTE: "CLIENT",
        TELEFONE: "PHONE",
        NICHE: "NICHO",
        GREETING: "SAUDACAO",
      };
      const finalKey = map[key] || key;
      return vars[finalKey] ?? "";
    }
  );
}

/**
 * Constrói a requisição para a UAZAPI de acordo com o estilo de payload
 * configurado. Suporta estilos JSON, querystring, form e template. Também
 * decide o método HTTP (GET/POST) com base na configuração e no endpoint.
 *
 * @param {string} instanceUrl
 * @param {Object} param0
 * @param {string} param0.e164
 * @param {string} param0.digits
 * @param {string} param0.text
 * @returns {Object}
 */
function buildUazRequest(instanceUrl, { e164, digits, text }) {
  const hasTpl = /\{(NUMBER|PHONE_E164|TEXT)\}/.test(instanceUrl);
  const hasQueryParams = /\?[^#]*=/.test(instanceUrl);
  const style = UAZ.payloadStyle;
  const methodEnv = UAZ.methodPref;

  const decideMethod = () => {
    if (methodEnv === 'get') return 'GET';
    if (methodEnv === 'post') return 'POST';
    return (hasTpl || hasQueryParams) ? 'GET' : 'POST';
  };

  const method = decideMethod();
  const phoneValue = UAZ.digitsOnly ? digits : e164;

  const makeJson = () => {
    const payload = { ...UAZ.extra };
    payload[UAZ.phoneField] = phoneValue;
    payload[UAZ.textField]  = text;
    return { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) };
  };

  if (style === 'template' || hasTpl) {
    if (method === 'GET') {
      const url = instanceUrl
        .replace(/\{NUMBER\}/g, digits)
        .replace(/\{PHONE_E164\}/g, encodeURIComponent(e164))
        .replace(/\{TEXT\}/g, encodeURIComponent(text));
      return { url, method: 'GET' };
    }
    let cleanUrl;
    try {
      const u = new URL(instanceUrl);
      cleanUrl = u.origin + u.pathname;
    } catch {
      cleanUrl = instanceUrl.split('?')[0];
    }
    const j = makeJson();
    return { url: cleanUrl, method: 'POST', headers: j.headers, body: j.body };
  }

  if (style === 'query' || (hasQueryParams && style === 'auto')) {
    const u = new URL(instanceUrl);
    if (method === 'GET') {
      u.searchParams.set(UAZ.phoneField, phoneValue);
      u.searchParams.set(UAZ.textField, text);
      Object.entries(UAZ.extra || {}).forEach(([k, v]) => {
        if (['string', 'number', 'boolean'].includes(typeof v)) u.searchParams.set(k, String(v));
      });
      return { url: u.toString(), method: 'GET' };
    }
    const cleanUrl = u.origin + u.pathname;
    const j = makeJson();
    return { url: cleanUrl, method: 'POST', headers: j.headers, body: j.body };
  }

  if (style === 'form') {
    const form = new URLSearchParams();
    Object.entries(UAZ.extra || {}).forEach(([k, v]) => form.set(k, typeof v === 'object' ? JSON.stringify(v) : String(v)));
    form.set(UAZ.phoneField, phoneValue);
    form.set(UAZ.textField, text);
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
    return { url: instanceUrl, method: 'POST', headers, body: form.toString() };
  }

  const j = makeJson();
  return { url: instanceUrl, method: 'POST', headers: j.headers, body: j.body };
}

/**
 * Envia uma requisição HTTP genérica, utilizando fetch se estiver disponível
 * globalmente ou node-fetch como fallback. Também oferece uma implementação
 * manual baseada em http/https para o caso de nenhuma das duas opções estar
 * disponível. Retorna um objeto com métodos compatíveis com fetch.
 *
 * @param {Object} param0
 * @param {string} param0.url
 * @param {string} [param0.method]
 * @param {Object} [param0.headers]
 * @param {any} [param0.body]
 */
async function httpSend({ url, method, headers, body }) {
  if (typeof fetch === 'function') {
    return fetch(url, { method, headers, body });
  }
  try {
    const nf = require('node-fetch');
    if (nf) return nf(url, { method, headers, body });
  } catch {}
  return new Promise((resolve, reject) => {
    try {
      const URLmod = new URL(url);
      const httpMod = URLmod.protocol === 'https:' ? require('https') : require('http');
      const req = httpMod.request(
        {
          hostname: URLmod.hostname,
          port: URLmod.port || (URLmod.protocol === 'https:' ? 443 : 80),
          path: URLmod.pathname + URLmod.search,
          method: method || 'GET',
          headers: headers || {},
        },
        (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => {
            resolve({
              ok: res.statusCode >= 200 && res.statusCode < 300,
              status: res.statusCode,
              json: async () => {
                try { return JSON.parse(data); }
                catch { return { raw: data }; }
              },
              text: async () => data,
            });
          });
        }
      );
      req.on('error', reject);
      if (body) req.write(body);
      req.end();
    } catch (err) { reject(err); }
  });
}

/**
 * Normaliza a exibição de um nicho, transformando cada primeira letra de
 * palavra em maiúscula e o restante em minúsculo. Útil para padronizar
 * nomes de segmentos antes de enviá-los ao template de mensagem.
 *
 * @param {string} n
 * @returns {string}
 */
function normalizeNiche(n) {
  if (!n) return '';
  const s = String(n).trim();
  return s.toLowerCase().replace(/\b\p{L}/gu, (c) => c.toUpperCase());
}

/**
 * Função principal para enviar uma mensagem via UAZAPI usando IA. Ela recebe
 * os dados do contato e as credenciais da instância e constrói uma
 * requisição conforme as configurações do projeto. Caso IA_CALL não esteja
 * habilitado ou não haja URL de instância, simula um envio bem-sucedido.
 *
 * @param {Object} param0
 * @param {string} param0.client
 * @param {string} param0.name
 * @param {string} param0.phone
 * @param {string} [param0.niche]
 * @param {string} [param0.instanceUrl]
 * @param {string} [param0.instanceToken]
 * @param {string} [param0.instanceAuthHeader]
 * @param {string} [param0.instanceAuthScheme]
 * @param {string} [param0.messageTemplate]
 */
async function runIAForContact({
  client,
  name,
  phone,
  niche,
  instanceUrl,
  instanceToken,
  instanceAuthHeader,
  instanceAuthScheme,
  messageTemplate,
}) {
  const SHOULD_CALL = process.env.IA_CALL === 'true';
  if (!SHOULD_CALL || !instanceUrl) return { ok: true, simulated: true };

  try {
    const e164 = normalizePhoneE164BR(phone);
    const digits = String(e164).replace(/\D/g, '');
    const prettyNiche = normalizeNiche(niche);

    // Pega o template do cliente ou cai no global (.env)
    const tpl = typeof messageTemplate === 'string' && messageTemplate.trim()
      ? messageTemplate
      : UAZ.template;

    const text = fillTemplate(tpl, {
      NAME: name,
      CLIENT: client,
      PHONE: e164,
      NICHO: prettyNiche,
    });

    const req = buildUazRequest(instanceUrl, { e164, digits, text });

    let hdrName   = (instanceAuthHeader && instanceAuthHeader.trim()) || UAZ.authHeader || 'token';
    const hdrScheme = instanceAuthScheme !== undefined ? instanceAuthScheme : UAZ.authScheme || '';
    const tokenVal  = (instanceToken && String(instanceToken)) || UAZ.token || '';
    // Se o nome do header foi salvo erroneamente com o próprio token (ou ficou muito longo), saneie para "token"
    if (hdrName === tokenVal || hdrName.length > 50) {
      hdrName = UAZ.authHeader || 'token';
    }
    if (tokenVal) {
      req.headers = req.headers || {};
      req.headers[hdrName] = `${hdrScheme}${tokenVal}`;
    }

    if (process.env.DEBUG === 'true') {
      const maskedHeaders = Object.fromEntries(
        Object.entries(req.headers || {}).map(([k, v]) => [
          k,
          /token|authorization/i.test(k) ? '***' : v,
        ])
      );
      console.log('[UAZAPI] request', { url: req.url, method: req.method, headers: maskedHeaders, hasBody: !!req.body });
    }

    const resp = await httpSend(req);
    let body;
    try { body = await resp.json(); }
    catch { body = await resp.text(); }
    if (!resp.ok) console.error('UAZAPI FAIL', { status: resp.status, body });

    return { ok: resp.ok, status: resp.status, body };
  } catch (err) {
    console.error('UAZAPI ERROR', instanceUrl, err);
    return { ok: false, error: String(err) };
  }
}

module.exports = {
  runIAForContact,
  normalizePhoneE164BR,
  fillTemplate,
  normalizeNiche,
};
