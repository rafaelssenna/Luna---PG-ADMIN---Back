const axios = require('axios');

function buildClient(baseURL) {
  const client = axios.create({
    baseURL,
    // timeout maior para export / paginações longas
    timeout: 60000,
    maxContentLength: Infinity,
    maxBodyLength: Infinity
  });

  client.interceptors.response.use(
    r => r,
    err => {
      if (err.response) {
        const msg = `[UAZAPI ${err.response.status}] ${JSON.stringify(err.response.data)}`;
        err.message = msg;
      }
      return Promise.reject(err);
    }
  );

  return {
    // GET /instance/all (admintoken)
    async listInstances(adminToken) {
      const { data } = await client.get('/instance/all', {
        headers: { admintoken: adminToken }
      });
      return data;
    },

    // GET /instance/status (token)
    async getInstanceStatus(instanceToken) {
      const { data } = await client.get('/instance/status', {
        headers: { token: instanceToken }
      });
      return data;
    },

    // POST /chat/find (token) - aceitar filtros, limit/offset
    async findChats(instanceToken, body = {}) {
      const { data } = await client.post('/chat/find', body, {
        headers: { token: instanceToken }
      });
      return data;
    },

    // POST /message/find (token) - por chatid (+ offset/limit)
    async findMessages(instanceToken, body) {
      const { data } = await client.post('/message/find', body, {
        headers: { token: instanceToken }
      });
      return data;
    },

    // POST genérico com header { token: <instanceToken> }
    async postWithToken(instanceToken, path, body = {}) {
      const { data } = await client.post(path, body, {
        headers: { token: instanceToken }
      });
      return data;
    }
  };
}

module.exports = { buildClient };
