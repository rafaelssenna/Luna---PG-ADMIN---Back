// leadsSearcher.js — v2 (SSE/chunk + idle-timeout, JSON-aware)
require('dotenv').config();

/**
 * ENVs:
 *  SMARTLEADS_URL="https://web-production-e49bb.up.railway.app"
 *  SMARTLEADS_SEARCH_PATH="/leads/stream"
 *  SMARTLEADS_METHOD="GET"
 *
 *  SMARTLEADS_QS_REGION="local"
 *  SMARTLEADS_QS_NICHE="nicho"
 *  SMARTLEADS_QS_LIMIT="n"
 *  SMARTLEADS_QS_VERIFY="verify"
 *
 *  SMARTLEADS_TOKEN=""                         // se não precisar, deixe vazio
 *  SMARTLEADS_TOKEN_QS_KEYS="access,token,authorization"
 *
 *  SMARTLEADS_EXTRA_QUERY='{"sid":"shared","session_id":"shared"}'
 *  SMARTLEADS_TIMEOUT_MS="180000"              // idle-timeout (ms) – reinicia a cada chunk
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

const TIMEOUT_MS = Math.max(15000, parseInt(process.env.SMARTLEADS_TIMEOUT_MS || "180000", 10));
const DEBUG = String(process.env.DEBUG || "").toLowerCase() === "true";

/* ---------- utils ---------- */
function safeJson(s){
  if (!s) return null;
  let t = String(s).trim();
  // remove aspas externas
  if ((t.startsWith('"') && t.endsWith('"')) || (t.startsWith("'") && t.endsWith("'"))) {
    t = t.slice(1, -1);
  }
  // desescapa \" e normaliza aspas simples -> duplas
  try { t = t.replace(/\\"/g, '"').replace(/'/g, '"'); } catch {}
  try { return JSON.parse(t); } catch { return null; }
}

async function doFetch(url, opts){
  if (typeof fetch === "function") return fetch(url, opts);      // nativo (Node 18+ / undici)
  try {
    // eslint-disable-next-line global-require
    const nf = require("node-fetch");                            // commonjs
    return nf(url, opts);
  } catch {
    const mod = await import("node-fetch");                      // dynamic import
    return mod.default(url, opts);
  }
}

function genDevice(prefix="WEB"){ return `${prefix}-${Math.random().toString(36).slice(2,12)}`; }
function normalizeDigits(s){ return String(s||"").replace(/\D/g,""); }

function pushCandidate(out, obj, region, niche){
  const phone = normalizeDigits(obj?.phone ?? obj?.telefone ?? obj?.number ?? obj?.whatsapp ?? "");
  if (!phone) return;
  out.push({
    name: obj?.name ?? obj?.nome ?? "",
    phone,
    region: obj?.region ?? region ?? null,
    niche:  obj?.niche  ?? obj?.segment ?? niche ?? null,
  });
}

function parsePhonesInText(out, text, region, niche){
  const matches = String(text||"").match(/\+?\d[\d\-\s\(\)]{8,}\d/g) || [];
  for(const m of matches){
    const phone = normalizeDigits(m);
    if (phone.length >= 10 && phone.length <= 14) {
      out.push({ name:"", phone, region: region || null, niche: niche || null });
    }
  }
}

/* ---------- leitura de stream ---------- */
async function readStream(resp, { region, niche }, onActivity){
  const out = [];
  const body = resp.body;

  if (body && typeof body.getReader === "function"){
    const reader = body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buf = "";

    while (true){
      const { value, done } = await reader.read();
      if (done) break;
      onActivity();

      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf("\n")) >= 0){
        const line = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 1);
        if (!line) continue;

        if (line.startsWith("data:")){
          const raw = line.slice(5).trim();
          try { pushCandidate(out, JSON.parse(raw), region, niche); }
          catch { parsePhonesInText(out, raw, region, niche); }
        } else {
          parsePhonesInText(out, line, region, niche);
        }
        onActivity();
      }
    }
    if (buf.trim().length){ parsePhonesInText(out, buf.trim(), region, niche); onActivity(); }
  } else {
    const text = await resp.text();
    const lines = String(text).split(/\r?\n/);
    for(const ln of lines){
      if (ln.startsWith("data:")){
        const raw = ln.slice(5).trim();
        try { pushCandidate(out, JSON.parse(raw), region, niche); }
        catch { parsePhonesInText(out, raw, region, niche); }
      } else {
        parsePhonesInText(out, ln, region, niche);
      }
      onActivity();
    }
  }

  const seen = new Set();
  return out.filter(it => { if (!it.phone || seen.has(it.phone)) return false; seen.add(it.phone); return true; });
}

function makeIdleController(timeoutMs){
  const ctrl = new AbortController();
  let t = null;
  const arm = ()=>{ clearTimeout(t); t = setTimeout(()=> ctrl.abort(new Error("idle-timeout")), timeoutMs); };
  arm();
  return { signal: ctrl.signal, activity: arm, dispose: ()=> clearTimeout(t) };
}

/* ---------- mapeamento JSON "puro" (não-SSE) ---------- */
function mapJsonPayload(data, region, niche){
  const out = [];
  if (Array.isArray(data)) {
    data.forEach(it => pushCandidate(out, it, region, niche));
    return out;
  }
  if (data && typeof data === "object") {
    const keys = ["items", "results", "leads", "data"];
    for (const k of keys) {
      if (Array.isArray(data[k])) {
        data[k].forEach(it => pushCandidate(out, it, region, niche));
        return out;
      }
    }
    // fallback: extrair números do JSON serializado
    parsePhonesInText(out, JSON.stringify(data), region, niche);
  }
  return out;
}

/* ---------- API ---------- */
/**
 * searchLeads({ region, niche, limit }) -> Promise<Array<{name?, phone, region?, niche?}>>
 */
async function searchLeads({ region, niche, limit=100 } = {}){
  if (!BASE){ console.warn("[leadsSearcher] SMARTLEADS_URL não configurada."); return []; }
  if (METHOD !== "GET"){ console.warn("[leadsSearcher] Use SMARTLEADS_METHOD=GET para stream."); }

  // monta URL GET
  const u = new URL(PATH, BASE);
  if (region) u.searchParams.set(QS_REGION, region);
  if (niche)  u.searchParams.set(QS_NICHE,  niche);
  if (limit)  u.searchParams.set(QS_LIMIT,  String(limit));
  if (QS_VERIFY) u.searchParams.set(QS_VERIFY, "1");

  if (TOKEN && TOKEN_QS_KEYS.length){
    for (const k of TOKEN_QS_KEYS) u.searchParams.set(k, TOKEN);
  }

  const extra = EXTRA_QUERY || {};
  const device = extra.device || genDevice("WEB");
  const deviceId = extra.device_id || device;
  Object.entries({ ...extra, device, device_id: deviceId }).forEach(([k,v])=>{
    if (v != null) u.searchParams.set(String(k), String(v));
  });

  if (DEBUG) console.log("[LEADS] URL:", u.toString());

  const idle = makeIdleController(TIMEOUT_MS);
  try {
    const resp = await doFetch(u.toString(), {
      method: "GET",
      headers: { "Accept": "application/json, text/event-stream, text/plain" },
      signal: idle.signal,
    });

    const ct = String(resp.headers?.get?.("content-type") || "").toLowerCase();
    if (DEBUG) console.log("[LEADS] HTTP:", resp.status, ct);

    if (!resp.ok){
      const body = await safeText(resp);
      console.warn("[leadsSearcher] HTTP", resp.status, body?.slice?.(0,200) || "");
      return [];
    }

    // JSON puro
    if (ct.includes("application/json")) {
      const data = await resp.json();
      return mapJsonPayload(data, region, niche);
    }

    // SSE / texto
    const results = await readStream(resp, { region, niche }, idle.activity);
    return results;
  } catch (err){
    if (String(err?.messag
