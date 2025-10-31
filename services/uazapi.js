const axios = require('axios');

/**
 * UAZAPI Client
 *
 * This module exports a factory function that builds a client for communicating
 * with the UAZAPI service. It wraps the underlying axios instance and exposes
 * convenience methods for the API endpoints used by Luna. The timeout is set
 * high to handle long-running requests such as exports and paginated queries.
 */
function buildClient(baseURL) {
  const client = axios.create({
    baseURL,
    // Increase timeouts to handle streaming and pagination
    timeout: 60000,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
  });

  // Intercept error responses and reformat the error message
  client.interceptors.response.use(
    (r) => r,
    (err) => {
      if (err.response) {
        const msg = `[UAZAPI ${err.response.status}] ${JSON.stringify(err.response.data)}`;
        err.message = msg;
      }
      return Promise.reject(err);
    }
  );

  return {
    /**
     * List all instances available to an admin. Requires an admin token.
     *
     * @param {string} adminToken
     */
    async listInstances(adminToken) {
      const { data } = await client.get('/instance/all', {
        headers: { admintoken: adminToken },
      });
      return data;
    },

    /**
     * Retrieve the status for a specific instance. Requires the instance token.
     *
     * @param {string} instanceToken
     */
    async getInstanceStatus(instanceToken) {
      const { data } = await client.get('/instance/status', {
        headers: { token: instanceToken },
      });
      return data;
    },

    /**
     * Find chats for a given instance. Supports pagination and search filters.
     *
     * @param {string} instanceToken
     * @param {Object} body
     */
    async findChats(instanceToken, body = {}) {
      const { data } = await client.post('/chat/find', body, {
        headers: { token: instanceToken },
      });
      return data;
    },

    /**
     * Find messages for a chat. Supports pagination and offsets.
     *
     * @param {string} instanceToken
     * @param {Object} body
     */
    async findMessages(instanceToken, body) {
      const { data } = await client.post('/message/find', body, {
        headers: { token: instanceToken },
      });
      return data;
    },

    /**
     * Perform a generic POST request with the instance token in the header.
     *
     * @param {string} instanceToken
     * @param {string} path
     * @param {Object} body
     */
    async postWithToken(instanceToken, path, body = {}) {
      const { data } = await client.post(path, body, {
        headers: { token: instanceToken },
      });
      return data;
    },
  };
}

module.exports = { buildClient };