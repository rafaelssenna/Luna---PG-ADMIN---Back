// leadsSearcher.js
require('dotenv').config();

/**
 * Integração com o seu back pronto (GET /leads/stream ? params...)
 *
 * ENVs suportadas:
 *  SMARTLEADS_URL="https://web-production-e49bb.up.railway.app"
 *  SMARTLEADS_SEARCH_PATH="/leads/stream"        // rota
 *  SMARTLEADS_METHOD="GET"                       // GET (stream)
 *
 *  SMARTLEADS_QS_REGION="local"                  // nome do campo "região" na query
 *  SMARTLEADS_QS_NICHE="nicho"                   // nome do campo "nicho"
 *  SMARTLEADS_QS_LIMIT="n"                       // nome do campo "limite"
 *  SMARTLEADS_QS_VERIFY="verify"                 // nome do campo de verificação (opcional)
 *
 *  SMARTLEADS_TOKEN="helsenia_admin_key"         // token da sua API (se houver)
 *  SMARTLEADS_TOKEN_QS_KEYS="access,token,authorization" // nomes de query que recebem o mesmo token
 *
 *  SMARTLEADS_EXTRA_QUERY='{"sid":"shared","session_id":"shared"}' // JSON com extras fixos
 *
 *  SMARTLEADS_TIMEOUT_MS="45000"
 */

const BASE    = process.env.SMARTLEADS_URL || "";
const PATH    = process.env.SMARTLEADS_SEARCH_PATH || "/leads/stream";
const METHOD  = (process.env.SMARTLEADS_METHOD || "GET").toUpperCase();

const QS_REGION = process.env.SMARTLEADS_QS_REGION || "local";
const QS_NICHE  = process.env.SMARTLEADS_QS_NICHE  || "nicho";
const QS_LIMIT  = process.env.SMARTLEADS_QS_LIMIT  || "n";
const QS_VERIFY = process.env.SMARTLEADS_QS_VERIFY || "verify";

const EXTRA_QUERY = safeJson(process.env.SMARTLEADS_EXTRA_QUERY) || { sid: "shared", session_id: "shared" };

const TOKEN          = process.env.SMARTLEADS_TOKEN || "";
const TOKEN_QS_KEYS  = (process.env.SMARTLEADS_TOKEN_QS_KEYS || "access,token,authorization")
  .split(",").map(s => s.trim()).filter(Boolean);

const TIMEOUT_MS = parseInt(process.env.SMARTLEADS_TIMEOUT_MS || "45000", 10);

/* util: parse JSON seguro */
function safeJson(s) {
  if (!s) return null;
  try { return JSON.parse(s); } catch { return null; }
}

/* fetch compat */
async function doFetch(url, opts) {
  if (typeof fetch === "function") return fetch(url, opts);
  const nf = require("node-fetch");
  return nf(url, opts);
}

/* gera um device aleatório se não vier por EXTRA_QUERY */
function genDevice(prefix = "WEB") {
  const rand = Math.random().toString(36).slice(2, 12);
  return `${prefix}-${rand}`;
}

/**
 * Lê SSE/stream linha a linha.
 * Aceita payloads "data: {...}" e também blocos plain-text contendo números.
 */
async function readStream(resp, { region, niche }) {
  const out = [];
  const ct = (resp.headers.get?.("content-type") || "").toLowerCase();

  // Node 18+ fetch: Response.body -> ReadableStream (web)
  if (resp.body && typeof resp.body.getReader === "function") {
    const reader = resp.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buf = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });

      // divide por linhas
      let idx;
      while ((idx = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 1);

        if (!line) continue;
        // SSE: linhas com "data: {...}"
        if (line.startsWith("data:")) {
          const raw = line.slice(5).trim();
          try {
            const obj = JSON.parse(raw);
            pushCandidate(out, obj, region, niche);
          } catch {
            // fallback: regex direto na linha
            parsePhonesInText(out, raw, region, niche);
          }
        } else {
          // conteúdo plain no stream
          parsePhonesInText(out, line, region, niche);
        }
      }
    }
    // resto do buffer
    if (buf.trim().length) parsePhonesInText(out, buf.trim(), region, niche);
  } else {
    // fallback: carrega tudo (se o servidor bufferizar)
    const text = await resp.text();
    const lines = text.split(/\r?\n/);
    for (const ln of lines) {
      if (ln.startsWith("data:")) {
        const raw = ln.slice(5).trim();
        try { pushCandidate(out, JSON.parse(raw), region, niche); }
        catch { parsePhonesInText(out, raw, region, niche); }
      } else {
        parsePhonesInText(out, ln, region, niche);
      }
    }
  }

  // dedup por phone (apenas dígitos)
  const seen = new Set();
  return out.filter(it => {
    const key = it.phone;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function normalizeDigits(s) {
  return String(s || "").replace(/\D/g, "");
}

function pushCandidate(out, obj, region, niche) {
  const phone = normalizeDigits(obj.phone || obj.telefone || obj.number || obj.whatsapp || "");
  if (!phone) return;
  out.push({
    name: obj.name || obj.nome || "",
    phone,
    region: obj.region || region || null,
    niche:  obj.niche  || obj.segment || niche || null,
  });
}

function parsePhonesInText(out, text, region, niche) {
  // captura números longos (10 a 14 dígitos) com ou sem +
  const matches = String(text || "").match(/\+?\d[\d\-\s\(\)]{8,}\d/g) || [];
  for (const m of matches) {
    const phone = normalizeDigits(m);
    if (phone.length >= 10 && phone.length <= 14) {
      out.push({ name: "", phone, region: region || null, niche: niche || null });
    }
  }
}

/**
 * searchLeads({ region, niche, limit })
 * Retorna Array<{ name?, phone, region?, niche? }>
 */
async function searchLeads({ region, niche, limit = 100 } = {}) {
  if (!BASE) {
    console.warn("[leadsSearcher] SMARTLEADS_URL não configurada. Retornando lista vazia.");
    return [];
  }
  if (METHOD !== "GET") {
    console.warn("[leadsSearcher] Para stream use SMARTLEADS_METHOD=GET");
  }

  // monta URL GET
  const u = new URL(PATH, BASE);
  if (region) u.searchParams.set(QS_REGION, region);
  if (niche)  u.searchParams.set(QS_NICHE, niche);
  if (limit)  u.searchParams.set(QS_LIMIT, String(limit));

  // verify=1 (se configurado)
  if (QS_VERIFY) u.searchParams.set(QS_VERIFY, "1");

  // tokens iguais em chaves diferentes
  if (TOKEN && TOKEN_QS_KEYS.length) {
    for (const k of TOKEN_QS_KEYS) u.searchParams.set(k, TOKEN);
  }

  // extras fixos
  const extra = EXTRA_QUERY || {};
  const device = extra.device || genDevice("WEB");
  const deviceId = extra.device_id || device;

  Object.entries({
    ...extra,
    device,
    device_id: deviceId,
  }).forEach(([k, v]) => {
    if (v != null) u.searchParams.set(String(k), String(v));
  });

  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), TIMEOUT_MS);

  const resp = await doFetch(u.toString(), {
    method: "GET",
    headers: { "Accept": "text/event-stream, text/plain, application/json" },
    signal: ctrl.signal,
  }).finally(() => clearTimeout(id));

  if (!resp.ok) {
    const body = await safeText(resp);
    console.warn("[leadsSearcher] HTTP", resp.status, body?.slice?.(0, 200) || "");
    return [];
  }

  return readStream(resp, { region, niche });
}

async function safeText(r) {
  try { return await r.text(); } catch { return ""; }
}

module.exports = { searchLeads };
