// helper/index.js
const grpc        = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path        = require('path');

const PROTO_DIR = path.join(__dirname, '../proto');

const pkg = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(PROTO_DIR, 'helper.proto'), {
    keepCase: true,
    longs:    String,
    enums:    String,
    defaults: true,
    oneofs:   true,
    includeDirs: [PROTO_DIR],
  })
).emergency;

const stub = new pkg.HelperService(
  'localhost:50051',
  grpc.credentials.createInsecure()
);

/**
 * Login as a helper agent.
 * @param {string} agentId
 * @param {string} password
 * @param {string} department  FIRE | MEDICAL | POLICE
 * @returns {Promise<HelperLoginResponse>}
 */
function login(agentId, password, department) {
  return new Promise((resolve, reject) =>
    stub.Login({ agent_id: agentId, password, department },
      (err, res) => err ? reject(err) : resolve(res))
  );
}

/**
 * Watch the pending queue for a department (server-streaming).
 * @param {string} agentId
 * @param {string} department
 * @returns {grpc.ClientReadableStream}
 */
function watchQueue(agentId, department) {
  return stub.WatchQueue({ agent_id: agentId, department });
}

/**
 * Self-assign a pending session.
 * @param {string} agentId
 * @param {string} sessionId
 * @returns {Promise<AssignResponse>}
 */
function assignClient(agentId, sessionId) {
  return new Promise((resolve, reject) =>
    stub.AssignClient({ agent_id: agentId, session_id: sessionId },
      (err, res) => err ? reject(err) : resolve(res))
  );
}

/**
 * Open a bidirectional chat stream.
 * @returns {grpc.ClientDuplexStream}
 */
function chatStream() {
  return stub.ChatStream();
}

/**
 * End (close or ban) a session.
 * @param {string} agentId
 * @param {string} sessionId
 * @param {'DONE'|'BANNED'} reason
 * @param {string} [note]
 * @returns {Promise<EndSessionResponse>}
 */
function endSession(agentId, sessionId, reason, note = '') {
  return new Promise((resolve, reject) =>
    stub.EndSession({ agent_id: agentId, session_id: sessionId, reason, note },
      (err, res) => err ? reject(err) : resolve(res))
  );
}

module.exports = { login, watchQueue, assignClient, chatStream, endSession };