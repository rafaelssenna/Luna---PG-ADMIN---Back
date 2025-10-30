// diagnostics/logger.js
// Habilita logs estruturados para requests, /export-analysis (payload)
// e /export.txt (detecção de stream abortada). Tudo vai para stdout.

const crypto = require('crypto');

function install(app) {
  // ---- Correlation ID + log de request/response
  app.use((req, res, next) => {
    req._rid = crypto.randomBytes(5).toString('hex');
    const start = Date.now();

    // Parse seguro de URL/query sem depender de libs
    let pathname = req.path || req.url;
    let qs = {};
    try {
      const full = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
      pathname = full.pathname;
      qs = Object.fromEntries(full.searchParams.entries());
    } catch {}

    console.log(`[REQ ${req._rid}] ${req.method} ${pathname} qs=${JSON.stringify(qs)}`);
    req.on('aborted', () => console.warn(`[REQ ${req._rid}] aborted by client`));
    res.on('finish', () => {
      const ms = Date.now() - start;
      console.log(`[RES ${req._rid}] ${res.statusCode} ${ms}ms`);
    });
    next();
  });

  // ---- Log do JSON de resposta da rota de análise
  app.use('/api/instances/:id/export-analysis', (req, res, next) => {
    const originalJson = res.json.bind(res);
    res.json = (body) => {
      try {
        const info = body && typeof body === 'object' ? (body.info || '') : '';
        const suggLen = (body && body.suggestions && String(body.suggestions).length) || 0;
        console.log(
          `[ANALYSIS ${req._rid}] status=ok:${!!(body && body.ok)} info="${info}" suggestions.len=${suggLen}`
        );

        // Log completo somente quando DEBUG=true (útil pra dev)
        if ((process.env.DEBUG || '').toLowerCase() === 'true') {
          const preview = JSON.stringify(body);
          console.log(`[ANALYSIS ${req._rid}] payload=${preview.substring(0, 4000)}`); // evita log gigante
        }
      } catch (e) {
        console.warn(`[ANALYSIS ${req._rid}] log-error`, e.message);
      }
      return originalJson(body);
    };
    next();
  });

  // ---- Detecta cliente fechando o download / stream antes do fim
  app.use('/api/instances/:id/export.txt', (req, res, next) => {
    res.on('close', () => {
      // Se não finalizou write/end, o cliente provavelmente cancelou
      if (!res.writableEnded) {
        console.warn(
          `[EXPORT ${req._rid}] stream closed before end (client aborted?)`
        );
      } else {
        console.log(`[EXPORT ${req._rid}] stream finished ok`);
      }
    });
    next();
  });
}

module.exports = { install };
