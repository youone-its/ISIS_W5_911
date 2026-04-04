// clients/index.js
const grpc       = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path       = require('path');

const PROTO_DIR = path.join(__dirname, '../proto');

const pkg = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(PROTO_DIR, 'clients.proto'), {
    keepCase: true,
    longs:    String,
    enums:    String,
    defaults: true,
    oneofs:   true,
    includeDirs: [PROTO_DIR],
  })
).emergency;

const stub = new pkg.ClientService(
  'localhost:50051',
  grpc.credentials.createInsecure()
);

/**
 * Register a new client.
 * @param {string} phoneNum
 * @returns {Promise<RegisterResponse>}
 */
function register(phoneNum) {
  return new Promise((resolve, reject) =>
    stub.Register({ phone_num: phoneNum }, (err, res) => err ? reject(err) : resolve(res))
  );
}

/**
 * Request an emergency.
 * @param {string} clientId
 * @param {string} initialMessage
 * @returns {Promise<EmergencyResponse>}
 */
function requestEmergency(clientId, initialMessage) {
  return new Promise((resolve, reject) =>
    stub.RequestEmergency({ client_id: clientId, initial_message: initialMessage },
      (err, res) => err ? reject(err) : resolve(res))
  );
}

/**
 * Watch a session for status updates (server-streaming).
 * @param {string} clientId
 * @param {string} sessionId
 * @returns {grpc.ClientReadableStream}
 */
function watchSessionStatus(clientId, sessionId) {
  return stub.WatchSessionStatus({ client_id: clientId, session_id: sessionId });
}

/**
 * Open a bidirectional chat stream.
 * @returns {grpc.ClientDuplexStream}
 */
function chatStream() {
  return stub.ChatStream();
}

module.exports = { register, requestEmergency, watchSessionStatus, chatStream };