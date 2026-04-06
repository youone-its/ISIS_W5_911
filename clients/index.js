import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
const packageDefinition = protoLoader.loadSync([
  './proto/helper.proto',
  './proto/clients.proto',
], { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
const proto = grpc.loadPackageDefinition(packageDefinition).emergency;
const centralStub = new proto.ClientService('localhost:50051', grpc.credentials.createInsecure());
export const getStub = () => centralStub;
export let mylocalprofile = {
  client_id: null,
  phone_num: "089xxxxxxxx"
};
export let currentSessionId = null;

export function register(phone, callback) {
  const request = {
    phone_num: phone
  };
  centralStub.Register(request, (err, response) => {
    if (!err && response.success) {
      // Simpan ke memori lokal
      mylocalprofile.client_id = response.client.client_id;
      mylocalprofile.phone_num = response.client.phone_num;
    }
    callback(err, response);
  });
}

export function requestEmergency(message, type, callback) {
  const request = {
    client_id: mylocalprofile.client_id,
    initial_message: message,
  };

  centralStub.RequestEmergency(request, (err, response) => {
    if (!err && response.session_id) {
      currentSessionId = response.session_id;
    }
    callback(err, response);
  });
}

export function cancelEmergency(callback) {
  if (!currentSessionId) {
    return callback(new Error("Tidak ada sesi aktif yang bisa dibatalkan"), null);
  }

  const request = {
    session_id: currentSessionId,
    client_id: mylocalprofile.client_id
  };

  centralStub.CancelEmergency(request, (err, response) => {
    if (!err && response.success) {
      currentSessionId = null;
    }
    callback(err, response);
  });
}

export function getCurrentSessionId() {
  return currentSessionId;
}

export function watchMyStatus(onUpdate) {
  const stream = centralStub.WatchSessionStatus({
    client_id: mylocalprofile.client_id
  });

  stream.on('data', (response) => {
    if (onUpdate) onUpdate(response);
  });

  stream.on('error', (err) => {
    // Diamkan jika error karena cancel
  });

  return stream;
}