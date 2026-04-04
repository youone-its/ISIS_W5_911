// server/index.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const crypto = require('crypto');

const PROTO_DIR = path.join(__dirname, '../proto');

const LOAD_OPTS = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_DIR],
};

const clientPkg     = grpc.loadPackageDefinition(protoLoader.loadSync(path.join(PROTO_DIR, 'clients.proto'),    LOAD_OPTS)).emergency;
const helperPkg     = grpc.loadPackageDefinition(protoLoader.loadSync(path.join(PROTO_DIR, 'helper.proto'),     LOAD_OPTS)).emergency;

const newId = () => crypto.randomUUID();

// ─── In-memory state ────────────────────────────────────────────────────────
const clients       = new Map(); // client_id  → Client
const sessions      = new Map(); // session_id → Session
const chatStreams    = new Map(); // session_id → { CLIENT: call | null, HELPER: call | null }
const sessionWatchers = new Map(); // session_id → call[]
const queueWatchers   = new Map(); // department  → call[]

// Pre-seeded helper agents  (agent_id / password / department)
const agents = new Map([
  ['agent001', { agent_id: 'agent001', name: 'Alice',   department: 'FIRE',    password: 'pass123' }],
  ['agent002', { agent_id: 'agent002', name: 'Bob',     department: 'MEDICAL', password: 'pass123' }],
  ['agent003', { agent_id: 'agent003', name: 'Charlie', department: 'POLICE',  password: 'pass123' }],
]);

// ─── Helpers ────────────────────────────────────────────────────────────────
function routeMessage(msg = '') {
  const t = msg.toLowerCase();
  if (/fire|burn|smoke|flame/.test(t))                        return 'FIRE';
  if (/medical|ambulance|hurt|injur|sick|bleed|pain/.test(t)) return 'MEDICAL';
  if (/police|crime|robbery|attack|thief|steal/.test(t))      return 'POLICE';
  return 'UNKNOWN';
}

function safeWrite(call, data) {
  try { call.write(data); } catch (_) { /* stream may be gone */ }
}

function pushSessionUpdate(sessionId, notification = '') {
  const session = sessions.get(sessionId);
  if (!session) return;
  (sessionWatchers.get(sessionId) || []).forEach(c => safeWrite(c, { session, notification }));
}

function pushQueueUpdate(department, newSession) {
  const pending = [...sessions.values()].filter(s => s.type === department && s.status === 'PENDING');
  (queueWatchers.get(department) || []).forEach(c =>
    safeWrite(c, { PENDING: pending, new_session: newSession, event: 'NEW_SESSION' })
  );
}

function removeWatcher(map, key, call) {
  const list = map.get(key);
  if (!list) return;
  const i = list.indexOf(call);
  if (i !== -1) list.splice(i, 1);
}

// ─── ClientService ──────────────────────────────────────────────────────────
const clientServiceImpl = {
  Register(call, cb) {
    const { phone_num } = call.request;
    if (!phone_num)
      return cb(null, { success: false, message: 'Phone number is required' });

    // Idempotent — return existing client if phone already registered
    for (const c of clients.values()) {
      if (c.phone_num === phone_num)
        return cb(null, { client: c, success: true, message: 'Already registered' });
    }

    const client = { client_id: newId(), phone_num, is_banned: false };
    clients.set(client.client_id, client);
    console.log(`[REGISTER] ${phone_num} → ${client.client_id}`);
    cb(null, { client, success: true, message: 'Registration successful' });
  },

  RequestEmergency(call, cb) {
    const { client_id, initial_message } = call.request;
    const client = clients.get(client_id);
    if (!client)
      return cb(null, { status: 'PENDING', info: 'Client not found — please register first' });
    if (client.is_banned)
      return cb(null, { status: 'BANNED',  info: 'You have been banned from this service' });

    const routed_to = routeMessage(initial_message);
    const session = {
      session_id:      newId(),
      client,
      assigned_agent:  null,
      type:            routed_to,
      status:          'PENDING',
      initial_message,
      created_at:      Date.now().toString(),
    };
    sessions.set(session.session_id, session);
    console.log(`[EMERGENCY] ${client_id} → session ${session.session_id} [${routed_to}]`);

    pushQueueUpdate(routed_to, session);
    cb(null, { session_id: session.session_id, routed_to, status: 'PENDING', info: 'Emergency request received' });
  },

  WatchSessionStatus(call) {
    const { client_id, session_id } = call.request;
    if (!sessionWatchers.has(session_id)) sessionWatchers.set(session_id, []);
    sessionWatchers.get(session_id).push(call);

    const session = sessions.get(session_id);
    if (session) safeWrite(call, { session, notification: 'Connected — watching session' });

    call.on('cancelled', () => removeWatcher(sessionWatchers, session_id, call));
    call.on('error',     () => removeWatcher(sessionWatchers, session_id, call));
  },

  ChatStream(call) {
    let sessionId = null;
    let role      = null;

    call.on('data', msg => {
      sessionId = msg.session_id;
      role      = msg.sender_role;   // 'CLIENT' or 'HELPER'

      if (!chatStreams.has(sessionId))
        chatStreams.set(sessionId, { CLIENT: null, HELPER: null });

      chatStreams.get(sessionId)[role] = call;

      // Skip internal connect ping
      if (msg.content === '__CONNECT__') return;

      console.log(`[CHAT][${sessionId}][${role}]: ${msg.content}`);

      // Forward to the other party
      const streams = chatStreams.get(sessionId);
      const target  = role === 'CLIENT' ? streams['HELPER'] : streams['CLIENT'];
      if (target) safeWrite(target, msg);
    });

    call.on('end', () => {
      if (sessionId && chatStreams.has(sessionId))
        chatStreams.get(sessionId)[role] = null;
      call.end();
    });

    call.on('error', () => {
      if (sessionId && chatStreams.has(sessionId))
        chatStreams.get(sessionId)[role] = null;
    });
  },
};

// ─── HelperService ──────────────────────────────────────────────────────────
const helperServiceImpl = {
  Login(call, cb) {
    const { agent_id, password, department } = call.request;
    const agent = agents.get(agent_id);
    if (!agent || agent.password !== password)
      return cb(null, { success: false, message: 'Invalid credentials' });
    if (agent.department !== department)
      return cb(null, { success: false, message: `Wrong department (you belong to ${agent.department})` });

    console.log(`[LOGIN] Agent ${agent_id} (${department})`);
    const { password: _, ...agentPublic } = agent;
    cb(null, { agent: agentPublic, token: newId(), success: true, message: 'Login successful' });
  },

  WatchQueue(call) {
    const { department } = call.request;
    if (!queueWatchers.has(department)) queueWatchers.set(department, []);
    queueWatchers.get(department).push(call);

    // Send current queue immediately
    const pending = [...sessions.values()].filter(s => s.type === department && s.status === 'PENDING');
    safeWrite(call, { PENDING: pending, new_session: null, event: 'INITIAL_QUEUE' });

    call.on('cancelled', () => removeWatcher(queueWatchers, department, call));
    call.on('error',     () => removeWatcher(queueWatchers, department, call));
  },

  AssignClient(call, cb) {
    const { agent_id, session_id } = call.request;
    const session = sessions.get(session_id);
    const agent   = agents.get(agent_id);
    if (!session) return cb(null, { success: false, message: 'Session not found' });
    if (!agent)   return cb(null, { success: false, message: 'Agent not found' });

    const { password: _, ...agentPublic } = agent;
    session.assigned_agent = agentPublic;
    session.status         = 'ONGOING';
    sessions.set(session_id, session);

    pushSessionUpdate(session_id, `Agent ${agent.name} has been assigned to your emergency`);
    console.log(`[ASSIGN] Agent ${agent_id} → session ${session_id}`);
    cb(null, { session, success: true, message: 'Session assigned successfully' });
  },

  ChatStream: clientServiceImpl.ChatStream,

  EndSession(call, cb) {
    const { agent_id, session_id, reason, note } = call.request;
    const session = sessions.get(session_id);
    if (!session) return cb(null, { success: false, message: 'Session not found' });

    const finalStatus = reason === 'BANNED' ? 'BANNED' : 'DONE';
    session.status    = finalStatus;

    if (reason === 'BANNED') {
      const client = clients.get(session.client?.client_id);
      if (client) { client.is_banned = true; clients.set(client.client_id, client); }
    }

    sessions.set(session_id, session);
    pushSessionUpdate(session_id, `Session ended: ${note || reason}`);
    console.log(`[END_SESSION] ${session_id} → ${finalStatus}`);
    cb(null, { success: true, final_status: finalStatus, message: note || 'Session closed' });
  },
};

// ─── Start server ────────────────────────────────────────────────────────────
const server = new grpc.Server();
server.addService(clientPkg.ClientService.service, clientServiceImpl);
server.addService(helperPkg.HelperService.service, helperServiceImpl);

server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) { console.error('Failed to start server:', err); process.exit(1); }

  console.log('');
  console.log('╔══════════════════════════════════════╗');
  console.log('║      Emergency gRPC Server           ║');
  console.log(`║  Listening on port ${port}           ║`);
  console.log('╚══════════════════════════════════════╝');
  console.log('');
  console.log('Pre-registered agents:');
  console.log('  agent001 / pass123  →  FIRE');
  console.log('  agent002 / pass123  →  MEDICAL');
  console.log('  agent003 / pass123  →  POLICE');
});