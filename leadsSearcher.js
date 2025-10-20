// leadsSearcher.js
require('dotenv').config();

/**
 * Configuração via ENV (ajuste conforme o seu Back-Smart-Leads):
 *  SMARTLEADS_URL            -> base do serviço (ex: https://smart-leads.meuservico.com)
 *  SMARTLEADS_SEARCH_PATH    -> rota (default: /api/search)
 *  SMARTLEADS_METHOD         -> POST | GET (default: POST)
 *  SMARTLEADS_TOKEN          -> token opcional
 *  SMARTLEADS_AUTH_HEADER    -> nome do header de auth (default: Authorization)
 *  SMARTLEADS_AUTH_SCHEME    -> prefixo (default: 'Bearer ')
 *  SMARTLEADS_RESULT_PATH    -> campo do JSON que contém a lista (default: results)
 *
 * Campos do payload:
 *  SMARTLEADS_REGION_FIELD   -> nome do campo 'região' (default: region)
 *  SMARTLEADS_NICHE_FIELD    -> nome do campo 'nicho'  (default: niche)
 *  SMARTLEADS_LIMIT_FIELD    -> nome do campo 'limite' (default: limit)
 */
const BASE   = process.env.SMARTLEADS_URL || '';
const PATH   = process.env.SMARTLEADS_SEARCH_PATH || '/api/search';
const METHOD = (process.env.SMARTLEADS_METHOD || 'POST').toUpperCase();

const TOKEN      = process.env.SMARTLEADS_TOKEN || '';
const AUTH_HDR   = process.env.SMARTLEADS_AUTH_HEADER || 'Authorization';
const AUTH_SCHEME= process.env.SMARTLEADS_AUTH_SCHEME ?? 'Bearer ';

const RESULT_KEY = process.env.SMARTLEADS_RESULT_PATH || 'results';

const REGION_KEY = process.env.SMARTLEADS_REGION_FIELD || 'region';
const NICHE_KEY  = process.env.SMARTLEADS_NICHE_FIELD  || 'niche';
const LIMIT_KEY  = process.env.SMARTLEADS_LIMIT_FIELD  || 'limit';

async function doFetch(url, opts) {
  if (typeof fetch === 'function') return fetch(url, opts);
  const nf = require('node-fetch');
  return nf(url, opts);
}

/**
 * searchLeads({ region, niche, limit }) -> Promise<Array<{name?, phone, region?, niche?}>>
 */
async function searchLeads({ region, niche, limit = 100 } = {}) {
  if (!BASE) {
    console.warn('[leadsSearcher] SMARTLEADS_URL não configurada. Retornando lista vazia.');
    return [];
  }

  const url = new URL(PATH, BASE).toString();
  const headers = { 'Content-Type': 'application/json' };
  if (TOKEN) headers[AUTH_HDR] = `${AUTH_SCHEME}${TOKEN}`;

  const payload = {};
  if (region) payload[REGION_KEY] = region;
  if (niche)  payload[NICHE_KEY]  = niche;
  if (limit)  payload[LIMIT_KEY]  = limit;

  const opts = { method: METHOD, headers };
  if (METHOD === 'POST') opts.body = JSON.stringify(payload);

  const resp = await doFetch(url, opts);
  let data;
  try { data = await resp.json(); } catch { data = await resp.text(); }

  const list = Array.isArray(data) ? data : (data?.[RESULT_KEY] || data?.items || []);
  if (!Array.isArray(list)) return [];

  // Normalização leve (phone como apenas dígitos)
  return list.map(it => ({
    name: it.name || it.nome || '',
    phone: String(it.phone || it.telefone || it.number || '').replace(/\D/g, ''),
    region: it.region || region || null,
    niche:  it.niche  || it.segment || niche || null,
  })).filter(x => x.phone);
}

module.exports = { searchLeads };
