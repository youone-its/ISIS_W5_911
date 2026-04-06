import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';

const packageDefinition=protoLoader.loadSync([
  './proto/clients.proto',
  './proto/helper.proto',
], {keepCase:true});

const proto=grpc.loadPackageDefinition(packageDefinition).emergency;

const registry={
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
const server=new grpc.Server();
const activeClients = new Map();
const loggedInHelpers = new Map();
const activeSessionsByClient = new Map();
const clientStatusStreams = new Map();

server.addService(proto.ClientService.service, {
  Register: (call, callback) => {
    const {phone_num} = call.request;
    if (activeClients.has(phone_num)) {
      return callback(null, {
        success: false,
        message: "Nomor telepon sudah terdaftar dan aktif!"
      });
    }
    const clientId=`C-${Date.now()}`;
    activeClients.set(phone_num, clientId);

    console.log(`Registered client: ${phone_num} (ID: ${clientId})`);
    callback(null, {
      success:true, 
      client:{client_id:clientId, phone_num:phone_num}, 
      message:"Berhasil REgistrasi"
    });
  },

  RequestEmergency: (call, callback) => {
    const {client_id, initial_message} = call.request;
    if (activeSessionsByClient.has(client_id)) {
      return callback(null, {
        status: 3, // BANNED atau REJECTED dalam konteks ini
        info: "Gagal: Anda masih memiliki sesi darurat yang belum selesai!"
      });
    }
  
    const input = initial_message.toLowerCase();
    let type = "POLICE"; // Default

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
    console.log(`New emergency request from Client ${client_id} categorized as ${type}. Session ID: ${session_id}`);

    helperStreams.forEach((helper,id) => {
      const targetDept = (type === "FIRE") ? 1 : (type === "MEDICAL") ? 2 : 3;
      if(helper.department === targetDept){
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
    
    // Daftarkan stream agar bisa di-push nanti
    clientStatusStreams.set(client_id, call);

    // KIRIM STATUS AWAL JIKA ADA SESI AKTIF
    const session_id = activeSessionsByClient.get(client_id);
    if (session_id) {
      // Pastikan mengirim objek yang lengkap agar terminal client tidak undefined
      call.write({
        session: {
          session_id: session_id,
          status: 0 // PENDING
        },
        notification: "Menunggu respons petugas..."
      });
    }

    call.on('cancelled', () => {
      clientStatusStreams.delete(client_id);
    });
  },
  // [cite: 17-23] Perbaikan CancelEmergency
  CancelEmergency: (call, callback) => {
    const { session_id, client_id } = call.request;
    let targetDept = null;    
    let foundInQueue = false;

    // Cari dan hapus dari antrean
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

      // BERITAHU HELPER: Kirim event pembatalan dan antrean terbaru
      const deptNum = (targetDept === "FIRE") ? 1 : (targetDept === "MEDICAL") ? 2 : 3;
      helperStreams.forEach((helper, agent_id) => {
        if (helper.department == deptNum) {
          helper.stream.write({
            event: `CANCEL_EVENT:${session_id}`, // Identifikasi sesi yang dibatalkan
            PENDING: queues[targetDept] // Kirim ulang daftar antrean yang sudah bersih [cite: 22]
          });
        }
      });

      callback(null, { success: true, message: "Sesi berhasil dibatalkan." });
    } else {
      callback(null, { success: false, message: "Gagal: Sesi tidak ditemukan atau sudah ditangani." });
    }
  }

});

server.addService(proto.HelperService.service, {
  Login: (call, callback) => {
    const {agent_id, password, department} = call.request;
    const helperData=registry.helpers.get(agent_id);
    if (!helperData || helperData.password !== password) {
      return callback(null, { success: false, message: "Auth Gagal" });
    }
    if (loggedInHelpers.has(agent_id)) {
      return callback(null, { success: false, message: "Agent sudah login di perangkat lain!" });
    }
    loggedInHelpers.set(agent_id, true);
    callback(null, {
      success:true,
      agent:{
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
      loggedInHelpers.delete(agent_id); // Menghapus status login dari Map [cite: 5]
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
    const {agent_id, department} = call.request;
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
        // Pastikan field PENDING di proto kamu adalah 'repeated'
        PENDING: existingSessions 
      });
    } else {
      call.write({ event: "Koneksi berhasil. Antrean kosong." });
    }

    call.on('cancelled', () => {
      helperStreams.delete(agent_id);
      loggedInHelpers.delete(agent_id);
      console.log(`[Stream] Helper ${agent_id} stopped watching.`);
    });
  },

  UpdateSessionStatus: (call, callback) => {
    const { session_id, status, agent_id } = call.request;
    
    // 1. Cari client_id berdasarkan session_id
    let targetClientId = null;
    activeSessionsByClient.forEach((sId, cId) => {
      if (sId === session_id) targetClientId = cId;
    });

    if (!targetClientId) {
      return callback(null, { success: false, message: "Sesi tidak ditemukan." });
    }

    // --- PERBAIKAN DI SINI: Update status di memory server ---
    // Cari di antrean dan update statusnya agar saat di-broadcast datanya benar
    for (let dept in queues) {
      const session = queues[dept].find(s => s.session_id === session_id);
      if (session) {
        session.status = parseInt(status); // Update status jadi 1 (ONGOING), dst.
        break;
      }
    }
    // -------------------------------------------------------

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
          status: parseInt(status), // Menggunakan enum RequestStatus
        },
        notification: infoMsgs[status]
      });
    }

    // 4. Jika status DONE atau BANNED, baru hapus
    if (status === 2 || status === 3) {
      activeSessionsByClient.delete(targetClientId);
      for (let dept in queues) {
        queues[dept] = queues[dept].filter(s => s.session_id !== session_id);
      }
    }

    callback(null, { 
      success: true, 
      message: `Berhasil mengubah status menjadi ${status}` 
    });
  }
});

server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
  console.clear();
  console.log('Server berjalan di port 50051');
  server.start();
});