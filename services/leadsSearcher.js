// leadsSearcher.js — robusto (SSE/chunk + idle-timeout + JSON + normalizeRegion)

/**
 * Este serviço encapsula a lógica de integração com o SmartLeads. Ele expõe
 * uma única função `searchLeads` que aceita parâmetros de região, nicho e
 * limite e retorna uma lista normalizada de leads com nome, telefone,
 * região e nicho. Ele respeita os limites de tempo configurados e lida
 * tanto com resposta JSON quanto com streams SSE.
 */

require('dotenv').config();

/* ENVs usadas */
const BASE    = process.env.SMARTLEADS_URL || '';
const PATH    = process.env.SMARTLEADS_SEARCH_PATH || '/leads/stream';
const METHOD  = String(process.env.SMARTLEADS_METHOD || 'GET').toUpperCase();

const QS_REGION = process.env.SMARTLEADS_QS_REGION || 'local';
const QS_NICHE  = process.env.SMARTLEADS_QS_NICHE  || 'nicho';
const QS_LIMIT  = process.env.SMARTLEADS_QS_LIMIT  || 'n';
const QS_VERIFY = process.env.SMARTLEADS_QS_VERIFY || 'verify';

const EXTRA_QUERY = safeJson(process.env.SMARTLEADS_EXTRA_QUERY) || { sid: 'shared', session_id: 'shared' };

const TOKEN         = process.env.SMARTLEADS_TOKEN || '';
const TOKEN_QS_KEYS = (process.env.SMARTLEADS_TOKEN_QS_KEYS || 'access,token,authorization').split(',');

const TIMEOUT_MS = Math.max(15000, parseInt(process.env.SMARTLEADS_TIMEOUT_MS || '180000', 10));
const DEBUG = String(process.env.DEBUG || '').toLowerCase() === 'true';

/* ---------- utils ---------- */
function safeJson(s) {
  if (!s) return null;
  let t = String(s).trim();
  if ((t[0] === '"' && t[t.length - 1] === '"') || (t[0] === "'" && t[t.length - 1] === "'")) t = t.slice(1, -1);
  try {
    t = t.replace(/\\"/g, '"').replace(/'/g, '"');
  } catch {}
  try {
    return JSON.parse(t);
  } catch {
    return null;
  }
}

let _fetch = null;
async function doFetch(url, opts) {
  if (_fetch) return _fetch(url, opts);
  if (typeof fetch === 'function') {
    _fetch = fetch;
    return _fetch(url, opts);
  }
  try {
    _fetch = require('node-fetch');
    return _fetch(url, opts);
  } catch {
    const mod = await import('node-fetch');
    _fetch = mod.default;
    return _fetch(url, opts);
  }
}

function genDevice(prefix) {
  prefix = prefix || 'WEB';
  return prefix + '-' + Math.random().toString(36).slice(2, 12);
}
function normalizeDigits(s) {
  return String(s || '').replace(/\D/g, '');
}

/* normaliza região digitada (ex.: "bh" -> "Belo Horizonte") */
function normalizeRegionInput(s) {
  if (!s) return s;
  const m = String(s).trim().toLowerCase();
  if (m === 'bh' || m === 'bh/mg' || m === 'b.h.' || m === 'b h' || m === 'bh mg') return 'Belo Horizonte';
  if (m === 'belo horizonte mg' || m === 'belo horizonte, mg') return 'Belo Horizonte';
  return s;
}

function pushCandidate(out, obj, region, niche) {
  const phone = normalizeDigits((obj && (obj.phone || obj.telefone || obj.number || obj.whatsapp)) || '');
  if (!phone) return;
  out.push({
    name: (obj && (obj.name || obj.nome)) || '',
    phone,
    region: (obj && obj.region) || region || null,
    niche: (obj && (obj.niche || obj.segment)) || niche || null,
  });
}

function parsePhonesInText(out, text, region, niche) {
  const matches = String(text || '').match(/\+?\d[\d\-\s\(\)]{8,}\d/g) || [];
  for (let i = 0; i < matches.length; i++) {
    const phone = normalizeDigits(matches[i]);
    if (phone.length >= 10 && phone.length <= 14) {
      out.push({ name: '', phone, region: region || null, niche: niche || null });
    }
  }
}

/* leitura de stream */
async function readStream(resp, ctx, onActivity) {
  const { region, niche } = ctx;
  const out = [];
  const body = resp.body;

  if (body && typeof body.getReader === 'function') {
    const reader = body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buf = '';

    while (true) {
      const r = await reader.read();
      if (r.done) break;
      onActivity();

      buf += decoder.decode(r.value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 1);
        if (!line) continue;

        if (line.indexOf('data:') === 0) {
          const raw = line.slice(5).trim();
          try {
            pushCandidate(out, JSON.parse(raw), region, niche);
          } catch {
            parsePhonesInText(out, raw, region, niche);
          }
        } else {
          parsePhonesInText(out, line, region, niche);
        }
        onActivity();
      }
    }
    if (buf.trim().length) {
      parsePhonesInText(out, buf.trim(), region, niche);
      onActivity();
    }
  } else {
    const text = await resp.text();
    const lines = String(text).split(/\r?\n/);
    for (let j = 0; j < lines.length; j++) {
      const ln = lines[j];
      if (ln.indexOf('data:') === 0) {
        const raw = ln.slice(5).trim();
        try {
          pushCandidate(out, JSON.parse(raw), region, niche);
        } catch {
          parsePhonesInText(out, raw, region, niche);
        }
      } else {
        parsePhonesInText(out, ln, region, niche);
      }
      onActivity();
    }
  }

  const seen = Object.create(null);
  const out2 = [];
  for (let k = 0; k < out.length; k++) {
    const it = out[k];
    if (!it.phone || seen[it.phone]) continue;
    seen[it.phone] = 1;
    out2.push(it);
  }
  return out2;
}

function makeIdleController(timeoutMs) {
  const ctrl = new AbortController();
  let t = null;
  function arm() {
    clearTimeout(t);
    t = setTimeout(() => {
      ctrl.abort(new Error('idle-timeout'));
    }, timeoutMs);
  }
  arm();
  return {
    signal: ctrl.signal,
    activity: arm,
    dispose: function () {
      clearTimeout(t);
    },
  };
}

/* JSON puro (não-SSE) */
function mapJsonPayload(data, region, niche) {
  const out = [];
  if (Array.isArray(data)) {
    for (let i = 0; i < data.length; i++) pushCandidate(out, data[i], region, niche);
    return out;
  }
  if (data && typeof data === 'object') {
    const keys = ['items', 'results', 'leads', 'data'];
    for (let j = 0; j < keys.length; j++) {
      const arr = data[keys[j]];
      if (Array.isArray(arr)) {
        for (let i2 = 0; i2 < arr.length; i2++) pushCandidate(out, arr[i2], region, niche);
        return out;
      }
    }
    parsePhonesInText(out, JSON.stringify(data), region, niche);
  }
  return out;
}

/**
 * searchLeads
 *
 * Chama a API do SmartLeads buscando leads a partir de região (local) e nicho.
 * Pode retornar tanto via Streaming (SSE) quanto JSON puro. No modo SSE, ele
 * mantém uma janela de tempo de inatividade (idle) e aborta a requisição se
 * não houver novas mensagens dentro do prazo. O token é passado via
 * querystring conforme as chaves configuradas em SMARTLEADS_TOKEN_QS_KEYS.
 *
 * @param {Object} opts
 * @param {string} opts.region
 * @param {string} opts.niche
 * @param {number} opts.limit
 */
async function searchLeads(opts = {}) {
  const region = normalizeRegionInput(opts.region);
  const niche = opts.niche;
  const limit = opts.limit || 100;

  if (!BASE) {
    console.warn('[leadsSearcher] SMARTLEADS_URL não configurada.');
    return [];
  }
  if (METHOD !== 'GET') {
    console.warn('[leadsSearcher] Use SMARTLEADS_METHOD=GET para stream.');
  }

  const u = new URL(PATH, BASE);
  if (region) u.searchParams.set(QS_REGION, String(region));
  if (niche) u.searchParams.set(QS_NICHE, String(niche));
  if (limit) u.searchParams.set(QS_LIMIT, String(limit));
  if (QS_VERIFY) u.searchParams.set(QS_VERIFY, '1');

  if (TOKEN) {
    for (let i = 0; i < TOKEN_QS_KEYS.length; i++) {
      const key = (TOKEN_QS_KEYS[i] || '').trim();
      if (key) u.searchParams.set(key, TOKEN);
    }
  }

  // extras + device; se vier "shared" gera sid único por chamada
  const extra = EXTRA_QUERY || {};
  const device = extra.device || genDevice('WEB');
  const deviceId = extra.device_id || device;

  const unique = Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 7);
  if (!extra.sid || String(extra.sid).toLowerCase() === 'shared') extra.sid = 'sid-' + unique;
  if (!extra.session_id || String(extra.session_id).toLowerCase() === 'shared') extra.session_id = 'sid-' + unique;

  const all = Object.assign({}, extra, { device, device_id: deviceId });
  for (const k in all) {
    if (Object.prototype.hasOwnProperty.call(all, k) && all[k] != null) {
      u.searchParams.set(String(k), String(all[k]));
    }
  }

  if (DEBUG) console.log('[LEADS] URL:', u.toString());

  const idle = makeIdleController(TIMEOUT_MS);
  try {
    const resp = await doFetch(u.toString(), {
      method: 'GET',
      headers: { Accept: 'application/json, text/event-stream, text/plain' },
      signal: idle.signal,
    });

    const contentType = resp.headers.get('content-type') || '';
    const onActivity = idle.activity;
    if (/text\/event-stream/i.test(contentType)) {
      return await readStream(resp, { region, niche }, onActivity);
    }
    // JSON puro
    const text = await resp.text();
    let data = null;
    try {
      data = JSON.parse(text);
    } catch {
      data = null;
    }
    onActivity();
    return mapJsonPayload(data, region, niche);
  } catch (err) {
    if (err.name === 'AbortError') {
      console.warn('[leadsSearcher] Abortado por idle-timeout.');
      return [];
    }
    console.error('[leadsSearcher] Erro ao buscar leads:', err.message || err);
    return [];
  } finally {
    idle.dispose();
  }
}

module.exports = { searchLeads };