import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
const packageDefinition=protoLoader.loadSync([
  './proto/helper.proto',
], {keepCase:true, longs: String, enums: String, defaults: true, oneofs: true});
const proto=grpc.loadPackageDefinition(packageDefinition).emergency;
const centralStub = new proto.HelperService('localhost:50051', grpc.credentials.createInsecure());
const activeSessions = new Map();
let helperNode = null;

export function startHelper(address,onClientAssigned){
  const helperNode=new grpc.Server();
  helperNode.addService(proto.HelperService.service, {
    AssignClient:(call, callback)=>{
      const {session_id, agent_id}=call.request;

      activeSessions.set(session_id, agent_id);

      if (onClientAssigned) onClientAssigned(session_id);

      callback(null, {
        success:true,
        message: "Client assigned to local node",
        session: { session_id: session_id, status: "ONGOING" }
      });
    }
  });
  
  const port = address.split(':')[1];
  helperNode.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), () => {
    console.log(`Helper server running on ${address}`);
  });
}

export function login(id,pass,callback){
  const request={
    agent_id: id,
    password: pass,
  };
  
  centralStub.Login(request, (err,response)=>{
    if (typeof callback === 'function') {
      callback(err, response);
    }
  });
}

export function logout(agentId, callback) {
  const request = {
    agent_id: agentId
  };

  // Memanggil RPC Logout yang sudah dibuat di .proto
  centralStub.Logout(request, (err, response) => {
    if (typeof callback === 'function') {
      callback(err, response);
    }
  });
}

export function watchQueue(agentId,dept,onUpdate){
  const request={
    agent_id: agentId,
    department: dept
  };

  const stream = centralStub.WatchQueue(request);

  stream.on('data', (response) => {
    if(onUpdate) onUpdate(response);
  });

  stream.on('error', (err) => {
    console.error("Stream error:", err);
  });
}

export function updateStatus(sessionId, status, agentId, callback) {
  const request = {
    session_id: sessionId,
    status: status,
    agent_id: agentId
  };

  centralStub.UpdateSessionStatus(request, (err, response) => {
    callback(err, response);
  });
}