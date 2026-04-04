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

const clients       = new Map();
const sessions      = new Map();
const chatStreams    = new Map();
const sessionWatchers = new Map();
const queueWatchers   = new Map();

const agents = new Map([
  ['agent001', { agent_id: 'agent001', name: 'Alice',   department: 'FIRE',    password: 'pass123' }],
  ['agent002', { agent_id: 'agent002', name: 'Bob',     department: 'MEDICAL', password: 'pass123' }],
  ['agent003', { agent_id: 'agent003', name: 'Charlie', department: 'POLICE',  password: 'pass123' }],
]);
