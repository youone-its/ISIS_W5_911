import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import * as wsDashboard from '../helper/web_socket.js'; // [WS] import dashboard module
 
const packageDefinition = protoLoader.loadSync([
  './proto/clients.proto',
  './proto/helper.proto',
], { keepCase: true });
 
const proto = grpc.loadPackageDefinition(packageDefinition).emergency;
 
const registry = {
  helpers: new Map([
    ["H001", { password: "123", name: "Budi Damkar", department: "FIRE", address: "localhost:50052" }],
    ["H002", { password: "456", name: "Siti Medis", department: "MEDICAL", address: "localhost:50053" }],
    ["H003", { password: "789", name: "Andi Polisi", department: "POLICE", address: "localhost:50054" }],
  ]),
};
 
const EmergencyType = {
  UNKNOWN: 0,
  FIRE: 1,
  MEDICAL: 2,
  POLICE: 3
};
 
const queues = {
  FIRE: [],
  MEDICAL: [],
  POLICE: []
};
 
const helperStreams = new Map();
const server = new grpc.Server();
const activeClients = new Map();
const loggedInHelpers = new Map();
const activeSessionsByClient = new Map();
const clientStatusStreams = new Map();
const bannedClients = new Set();
const bannedPhones = new Set();
const clientToPhone = new Map();
 
const chatSessions = new Map();
 
const chatStreamHandler = (call) => {
  let currentSessionId = null;
  call.on('data', (msg) => {
    currentSessionId = msg.session_id;
    if (!chatSessions.has(currentSessionId)) {
      chatSessions.set(currentSessionId, new Set());
    }
    chatSessions.get(currentSessionId).add(call);
 
    // Broadcast ke semua gRPC stream dalam sesi ini
    if (msg.content || msg.status) {
      chatSessions.get(currentSessionId).forEach(c => {
        c.write(msg);
      });
      if (msg.content) wsDashboard.onChatMessage(currentSessionId, msg); // [WS] forward pesan ke dashboard
    }
  });
 
  call.on('end', () => {
    if (currentSessionId && chatSessions.has(currentSessionId)) {
      chatSessions.get(currentSessionId).delete(call);
    }
    call.end();
  });
 
  call.on('error', () => {
    if (currentSessionId && chatSessions.has(currentSessionId)) {
      chatSessions.get(currentSessionId).delete(call);
    }
  });
};
 
server.addService(proto.ClientService.service, {
  ChatStream: chatStreamHandler,
  Register: (call, callback) => {
    const { phone_num } = call.request;
 
    if (bannedPhones.has(phone_num)) {
      return callback(null, {
        success: false,
        message: "Gagal: Nomor telepon Anda telah diblokir dari sistem ini."
      });
    }
 
    if (activeClients.has(phone_num)) {
      return callback(null, {
        success: false,
        message: "Nomor telepon sudah terdaftar dan aktif!"
      });
    }
    const clientId = `C-${Date.now()}`;
    activeClients.set(phone_num, clientId);
    clientToPhone.set(clientId, phone_num);
 
    console.log(`Registered client: ${phone_num} (ID: ${clientId})`);
    callback(null, {
      success: true,
      client: { client_id: clientId, phone_num: phone_num },
      message: "Berhasil REgistrasi"
    });
  },
 
  RequestEmergency: (call, callback) => {
    const { client_id, initial_message } = call.request;
 
    if (bannedClients.has(client_id)) {
      return callback(null, {
        status: 3,
        info: "Gagal: Akun Anda telah diblokir oleh petugas. Anda tidak dapat melakukan permintaan bantuan lagi."
      });
    }
 
    if (activeSessionsByClient.has(client_id)) {
      return callback(null, {
        status: 3,
        info: "Gagal: Anda masih memiliki sesi darurat yang belum selesai!"
      });
    }
 
    const input = initial_message.toLowerCase();
    let type = "POLICE";
 
    if (input.includes("kebakaran") || input.includes("api") || input.includes("hangus") || input.includes("asap")) {
      type = "FIRE";
    } else if (input.includes("kecelakaan") || input.includes("ambulans") || input.includes("sakit") || input.includes("medis") || input.includes("darah")) {
      type = "MEDICAL";
    }
 
    const session_id = `S-${Date.now()}`;
    activeSessionsByClient.set(client_id, session_id);
    const newSession = {
      session_id: session_id,
      client_id: client_id,
      initial_message: initial_message,
      status: 0
    };
    queues[type].push(newSession);
    wsDashboard.onQueueUpdate(queues); // [WS] queue bertambah → update dashboard
    console.log(`New emergency request from Client ${client_id} categorized as ${type}. Session ID: ${session_id}`);
 
    helperStreams.forEach((helper, id) => {
      const targetDept = (type === "FIRE") ? 1 : (type === "MEDICAL") ? 2 : 3;
      if (helper.department === targetDept) {
        console.log(`[Stream] Mengirim update ke Helper ID: ${id}`);
        helper.stream.write({
          new_session: newSession,
          event: `Ada panggilan baru di kategori ${type}!`
        });
      }
    });
 
    callback(null, {
      session_id: session_id,
      routed_to: EmergencyType[type],
      status: 0,
      info: `Laporan Anda telah diteruskan ke departemen ${type}. Mohon tunggu.`
    });
  },
 
  WatchSessionStatus: (call) => {
    const { client_id } = call.request;
 
    clientStatusStreams.set(client_id, call);
 
    const session_id = activeSessionsByClient.get(client_id);
    if (session_id) {
      call.write({
        session: {
          session_id: session_id,
          status: 0
        },
        notification: "Menunggu respons petugas..."
      });
    }
 
    call.on('cancelled', () => {
      clientStatusStreams.delete(client_id);
    });
  },
 
  CancelEmergency: (call, callback) => {
    const { session_id, client_id } = call.request;
    let targetDept = null;
    let foundInQueue = false;
 
    for (let dept in queues) {
      const index = queues[dept].findIndex(s => s.session_id === session_id);
      if (index !== -1) {
        queues[dept].splice(index, 1);
        targetDept = dept;
        foundInQueue = true;
        break;
      }
    }
 
    if (foundInQueue) {
      activeSessionsByClient.delete(client_id);
 
      const deptNum = (targetDept === "FIRE") ? 1 : (targetDept === "MEDICAL") ? 2 : 3;
      helperStreams.forEach((helper, agent_id) => {
        if (helper.department == deptNum) {
          helper.stream.write({
            event: `CANCEL_EVENT:${session_id}`,
            PENDING: queues[targetDept]
          });
        }
      });
 
      wsDashboard.onQueueUpdate(queues); // [WS] queue berkurang → update dashboard (di dalam if foundInQueue)
 
      callback(null, { success: true, message: "Sesi berhasil dibatalkan." });
    } else {
      callback(null, { success: false, message: "Gagal: Sesi tidak ditemukan atau sudah ditangani." });
    }
  }
 
});
 
server.addService(proto.HelperService.service, {
  ChatStream: chatStreamHandler,
  Login: (call, callback) => {
    const { agent_id, password, department } = call.request;
    const helperData = registry.helpers.get(agent_id);
    if (!helperData || helperData.password !== password) {
      return callback(null, { success: false, message: "Auth Gagal" });
    }
    if (loggedInHelpers.has(agent_id)) {
      return callback(null, { success: false, message: "Agent sudah login di perangkat lain!" });
    }
    loggedInHelpers.set(agent_id, true);
    wsDashboard.onHelperLogin(agent_id, helperData); // [WS] helper masuk → munculkan card di dashboard
 
    console.log(`Helper server running on ${helperData.address}`);
 
    callback(null, {
      success: true,
      agent: {
        agent_id: agent_id,
        name: helperData.name,
        department: helperData.department,
      },
      token: `token-${agent_id}-${Date.now()}`,
      message: `Login berhasil untuk helper: ${helperData.name} dengan alamat ${helperData.address} dari department ${helperData.department}`,
    });
  },
 
  Logout: (call, callback) => {
    const { agent_id } = call.request;
 
    if (loggedInHelpers.has(agent_id)) {
      loggedInHelpers.delete(agent_id);
      wsDashboard.onHelperLogout(agent_id); // [WS] helper logout → hapus card dari dashboard
      console.log(`[Auth] Helper ${agent_id} berhasil logout.`);
 
      callback(null, {
        success: true,
        message: "Logout berhasil, Anda sekarang bisa login kembali."
      });
    } else {
      callback(null, {
        success: false,
        message: "Agent ID tidak ditemukan dalam daftar login."
      });
    }
  },
 
  WatchQueue: (call) => {
    const { agent_id, department } = call.request;
    const deptKey = (department == 1) ? "FIRE" : (department == 2) ? "MEDICAL" : "POLICE";
    console.log(`[Stream] Helper ${agent_id} watching queue for ${department}`);
    helperStreams.set(agent_id, {
      stream: call,
      department: department
    });
 
    const existingSessions = queues[deptKey] || [];
    if (existingSessions.length > 0) {
      call.write({
        event: `Koneksi berhasil. Ada ${existingSessions.length} antrean menunggu.`,
        PENDING: existingSessions
      });
    } else {
      call.write({ event: "Koneksi berhasil. Antrean kosong." });
    }
 
    call.on('cancelled', () => {
      helperStreams.delete(agent_id);
      loggedInHelpers.delete(agent_id);
      wsDashboard.onHelperLogout(agent_id); // [WS] stream terputus paksa → hapus card dari dashboard
      console.log(`[Stream] Helper ${agent_id} stopped watching.`);
    });
  },
 
  UpdateSessionStatus: (call, callback) => {
    const { session_id, status, agent_id } = call.request;
    console.log(`[UpdateSessionStatus] agent=${agent_id} session=${session_id} status=${status}`);
    const statusInt = parseInt(status); // [WS] parse sekali, dipakai di bawah
 
    // 1. Cari client_id berdasarkan session_id
    let targetClientId = null;
    activeSessionsByClient.forEach((sId, cId) => {
      if (sId === session_id) targetClientId = cId;
    });
 
    if (!targetClientId) {
      return callback(null, { success: false, message: "Sesi tidak ditemukan." });
    }
 
    // Update status di memory server
    for (let dept in queues) {
      const session = queues[dept].find(s => s.session_id === session_id);
      if (session) {
        session.status = statusInt;
        break;
      }
    }
 
    if (statusInt === 1) {
      wsDashboard.onSessionAssigned(agent_id, session_id); // [WS] helper ambil sesi → badge jadi MISI
      wsDashboard.onQueueUpdate(queues); // [WS] FIX: broadcast queue update agar item di dashboard langsung refresh ke ONGOING
    }
 
    // 2. Tentukan pesan info
    const infoMsgs = [
      "Menunggu petugas...",
      `Laporan sedang ditangani oleh petugas (${agent_id}).`,
      "Laporan telah selesai ditangani. Terima kasih.",
      "Laporan Anda ditolak/diblokir oleh sistem."
    ];
 
    // 3. Kirim UPDATE ke Client via Stream
    const clientStream = clientStatusStreams.get(targetClientId);
    if (clientStream) {
      console.log(`[Stream] Mengirim update status ${status} ke Client ${targetClientId}`);
      clientStream.write({
        session: {
          session_id: session_id,
          status: statusInt,
        },
        notification: infoMsgs[statusInt]
      });
    }
 
    // 4. Jika status DONE atau BANNED, baru hapus
    if (statusInt === 2 || statusInt === 3) { // FIX: pakai statusInt bukan status mentah (bisa string dari gRPC)
      if (chatSessions.has(session_id)) {
        chatSessions.get(session_id).forEach(c => {
          c.write({
            sender_id: "SYSTEM",
            content: statusInt === 3 ? "Laporan telah diblokir." : "Sesi telah ditutup oleh agen.",
            status: statusInt === 2 ? "done" : "banned"
          });
        });
        chatSessions.delete(session_id);
      }
 
      if (statusInt === 3) {
        console.log(`[BAN] Menambahkan Client ${targetClientId} ke daftar blokir.`);
        bannedClients.add(targetClientId);
        const phone = clientToPhone.get(targetClientId);
        if (phone) {
          bannedPhones.add(phone);
          console.log(`[BAN] Nomor ${phone} juga telah diblokir.`);
        }
      }
      activeSessionsByClient.delete(targetClientId);
      for (let dept in queues) {
        queues[dept] = queues[dept].filter(s => s.session_id !== session_id);
      }
    }
 
    // [WS] Setelah semua state bersih, update dashboard
    if (statusInt === 2 || statusInt === 3) {
      wsDashboard.onSessionEnded(session_id);  // helper kembali LUANG, tutup chat watcher
      wsDashboard.onQueueUpdate(queues);        // queue sudah dipurge, sync ke dashboard
    }
 
    callback(null, {
      success: true,
      message: `Berhasil mengubah status menjadi ${status}`
    });
  }
});
 
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error(`Gagal bind server: ${err.message}`);
    return;
  }
  console.clear();
  console.log(`Server berjalan di port ${port}`);
  server.start();
  wsDashboard.init(3000); // [WS] jalankan WebSocket dashboard server
});