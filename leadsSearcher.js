// leadsSearcher.js — robusto (SSE/chunk + idle-timeout + JSON + normalizeRegion)
require('dotenv').config();

/* ENVs usadas */
var BASE    = process.env.SMARTLEADS_URL || "";
var PATH    = process.env.SMARTLEADS_SEARCH_PATH || "/leads/stream";
var METHOD  = String(process.env.SMARTLEADS_METHOD || "GET").toUpperCase();

var QS_REGION = process.env.SMARTLEADS_QS_REGION || "local";
var QS_NICHE  = process.env.SMARTLEADS_QS_NICHE  || "nicho";
var QS_LIMIT  = process.env.SMARTLEADS_QS_LIMIT  || "n";
var QS_VERIFY = process.env.SMARTLEADS_QS_VERIFY || "verify";

var EXTRA_QUERY = safeJson(process.env.SMARTLEADS_EXTRA_QUERY) || { sid: "shared", session_id: "shared" };

var TOKEN         = process.env.SMARTLEADS_TOKEN || "";
var TOKEN_QS_KEYS = (process.env.SMARTLEADS_TOKEN_QS_KEYS || "access,token,authorization").split(",");

var TIMEOUT_MS = Math.max(15000, parseInt(process.env.SMARTLEADS_TIMEOUT_MS || "180000", 10));
var DEBUG = String(process.env.DEBUG || "").toLowerCase() === "true";

/* ---------- utils ---------- */
function safeJson(s){
  if (!s) return null;
  var t = String(s).trim();
  if ((t[0] === '"' && t[t.length-1] === '"') || (t[0] === "'" && t[t.length-1] === "'")) t = t.slice(1, -1);
  try { t = t.replace(/\\"/g, '"').replace(/'/g, '"'); } catch {}
  try { return JSON.parse(t); } catch { return null; }
}

var _fetch = null;
async function doFetch(url, opts){
  if (_fetch) return _fetch(url, opts);
  if (typeof fetch === "function") { _fetch = fetch; return _fetch(url, opts); }
  try { _fetch = require("node-fetch"); return _fetch(url, opts); }
  catch { const mod = await import("node-fetch"); _fetch = mod.default; return _fetch(url, opts); }
}

function genDevice(prefix){ prefix = prefix || "WEB"; return prefix + "-" + Math.random().toString(36).slice(2,12); }
function normalizeDigits(s){ return String(s||"").replace(/\D/g,""); }

/* normaliza região digitada (ex.: "bh" -> "Belo Horizonte") */
function normalizeRegionInput(s){
  if (!s) return s;
  var m = String(s).trim().toLowerCase();
  if (m === "bh" || m === "bh/mg" || m === "b.h." || m === "b h" || m === "bh mg") return "Belo Horizonte";
  if (m === "belo horizonte mg" || m === "belo horizonte, mg") return "Belo Horizonte";
  return s;
}

function pushCandidate(out, obj, region, niche){
  var phone = normalizeDigits((obj && (obj.phone || obj.telefone || obj.number || obj.whatsapp)) || "");
  if (!phone) return;
  out.push({
    name:   (obj && (obj.name || obj.nome)) || "",
    phone:  phone,
    region: (obj && obj.region) || region || null,
    niche:  (obj && (obj.niche || obj.segment)) || niche || null
  });
}

function parsePhonesInText(out, text, region, niche){
  var matches = String(text||"").match(/\+?\d[\d\-\s\(\)]{8,}\d/g) || [];
  for (var i=0; i<matches.length; i++){
    var phone = normalizeDigits(matches[i]);
    if (phone.length >= 10 && phone.length <= 14) {
      out.push({ name:"", phone: phone, region: region || null, niche: niche || null });
    }
  }
}

/* leitura de stream */
async function readStream(resp, ctx, onActivity){
  var region = ctx.region, niche = ctx.niche;
  var out = [];
  var body = resp.body;

  if (body && typeof body.getReader === "function"){
    var reader = body.getReader();
    var decoder = new TextDecoder("utf-8");
    var buf = "";

    while (true){
      var r = await reader.read();
      if (r.done) break;
      onActivity();

      buf += decoder.decode(r.value, { stream: true });
      var idx;
      while ((idx = buf.indexOf("\n")) >= 0){
        var line = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 1);
        if (!line) continue;

        if (line.indexOf("data:") === 0){
          var raw = line.slice(5).trim();
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
    var text = await resp.text();
    var lines = String(text).split(/\r?\n/);
    for (var j=0; j<lines.length; j++){
      var ln = lines[j];
      if (ln.indexOf("data:") === 0){
        var raw = ln.slice(5).trim();
        try { pushCandidate(out, JSON.parse(raw), region, niche); }
        catch { parsePhonesInText(out, raw, region, niche); }
      } else {
        parsePhonesInText(out, ln, region, niche);
      }
      onActivity();
    }
  }

  var seen = Object.create(null), out2 = [];
  for (var k=0; k<out.length; k++){
    var it = out[k];
    if (!it.phone || seen[it.phone]) continue;
    seen[it.phone] = 1;
    out2.push(it);
  }
  return out2;
}

function makeIdleController(timeoutMs){
  var ctrl = new AbortController();
  var t = null;
  function arm(){ clearTimeout(t); t = setTimeout(function(){ ctrl.abort(new Error("idle-timeout")); }, timeoutMs); }
  arm();
  return { signal: ctrl.signal, activity: arm, dispose: function(){ clearTimeout(t); } };
}

/* JSON puro (não-SSE) */
function mapJsonPayload(data, region, niche){
  var out = [];
  if (Array.isArray(data)) { for (var i=0;i<data.length;i++) pushCandidate(out, data[i], region, niche); return out; }
  if (data && typeof data === "object") {
    var keys = ["items", "results", "leads", "data"];
    for (var j=0;j<keys.length;j++){
      var arr = data[keys[j]];
      if (Array.isArray(arr)) { for (var i2=0;i2<arr.length;i2++) pushCandidate(out, arr[i2], region, niche); return out; }
    }
    parsePhonesInText(out, JSON.stringify(data), region, niche);
  }
  return out;
}

/* API */
async function searchLeads(opts){
  opts = opts || {};
  var region = normalizeRegionInput(opts.region);
  var niche  = opts.niche;
  var limit  = opts.limit || 100;

  if (!BASE){ console.warn("[leadsSearcher] SMARTLEADS_URL não configurada."); return []; }
  if (METHOD !== "GET"){ console.warn("[leadsSearcher] Use SMARTLEADS_METHOD=GET para stream."); }

  var u = new URL(PATH, BASE);
  if (region) u.searchParams.set(QS_REGION, String(region));
  if (niche)  u.searchParams.set(QS_NICHE,  String(niche));
  if (limit)  u.searchParams.set(QS_LIMIT,  String(limit));
  if (QS_VERIFY) u.searchParams.set(QS_VERIFY, "1");

  if (TOKEN) {
    for (var i=0;i<TOKEN_QS_KEYS.length;i++){
      var key = (TOKEN_QS_KEYS[i] || "").trim();
      if (key) u.searchParams.set(key, TOKEN);
    }
  }

  // extras + device; se vier "shared" gera sid único por chamada
  var extra = EXTRA_QUERY || {};
  var device = extra.device || genDevice("WEB");
  var deviceId = extra.device_id || device;

  var unique = Date.now().toString(36) + '-' + Math.random().toString(36).slice(2,7);
  if (!extra.sid || String(extra.sid).toLowerCase() === 'shared')        extra.sid = 'sid-' + unique;
  if (!extra.session_id || String(extra.session_id).toLowerCase() === 'shared') extra.session_id = 'sid-' + unique;

  var all = Object.assign({}, extra, { device: device, device_id: deviceId });
  for (var k in all){
    if (Object.prototype.hasOwnProperty.call(all,k) && all[k] != null){
      u.searchParams.set(String(k), String(all[k]));
    }
  }

  if (DEBUG) console.log("[LEADS] URL:", u.toString());

  var idle = makeIdleController(TIMEOUT_MS);
  try {
    var resp = await doFetch(u.toString(), {
      method: "GET",
      headers: { "Accept": "application/json, text/event-stream, text/plain" },
      signal: idle.signal
    });

    var ct = (resp && resp.headers && resp.headers.get && resp.headers.get("content-type")) || "";
    ct = String(ct).toLowerCase();
    if (DEBUG) console.log("[LEADS] HTTP:", resp.status, ct);

    if (!resp.ok){
      var body = await safeText(resp);
      console.warn("[leadsSearcher] HTTP", resp.status, (body && body.slice) ? body.slice(0,200) : "");
      return [];
    }

    if (ct.indexOf("application/json") >= 0) {
      var data = await resp.json();
      return mapJsonPayload(data, region, niche);
    }

    var results = await readStream(resp, { region: region, niche: niche }, idle.activity);
    return results;
  } catch (err){
    if ((err && String(err.message).indexOf("idle-timeout") >= 0)){
      console.warn("[leadsSearcher] idle-timeout — finalizando stream sem erro");
      return [];
    }
    console.error("[leadsSearcher] erro:", (err && err.message) || err);
    return [];
  } finally {
    idle.dispose();
  }
}

async function safeText(r){ try { return await r.text(); } catch { return ""; } }

module.exports = { searchLeads };
