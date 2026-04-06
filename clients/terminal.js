import * as clientService from "./index.js";
import { setTimeout } from 'node:timers/promises';
import startChat from "./client_chat.js";
import readline from "readline";
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});
let clientState = {
  activeSession: null,
  currentStatus: "N/A",
  lastInfo: "-",
  initialMessage: "-",
  isBanned: false
};

async function showMainMenu() {
  console.clear();
  await setTimeout(200);
  console.log(`\n=== CLIENT DASHBOARD ===`);
  console.log(`ID    : ${clientService.mylocalprofile.client_id}`);
  console.log(`Phone : ${clientService.mylocalprofile.phone_num}`);
  if (clientState.activeSession) {
    console.log(`Request : ${clientState.initialMessage}`);
    console.log(`Status  : [${clientState.currentStatus}] - ${clientState.lastInfo}`);
  } else {
    console.log(`Status  : Tidak ada laporan aktif.`);
  }
  console.log(`------------------------`);
  console.log("1. Minta Bantuan (Emergency)");
  console.log("2. Refresh Tampilan");
  console.log("3. Keluar");
  console.log("4. Batalkan Sesi");
  console.log("5. Masuk ke Chat");

  rl.question("\nPilih Menu: ", (choice) => {
    switch (choice) {
      case '1':
        if (clientState.isBanned) {
          console.log("\n[!] Gagal: Anda telah diblokir. Tidak dapat meminta bantuan lagi.");
          return setTimeout(2000).then(showMainMenu);
        }
        if (clientState.activeSession) {
          console.log("\n[!] Gagal: Selesaikan atau batalkan sesi aktif terlebih dahulu.");
          return setTimeout(1500).then(showMainMenu);
        }
        rl.question("\nApa keadaan darurat Anda? ", (msg) => {
          clientService.requestEmergency(msg, 3, (err, res) => {
            if (err || !res.session_id) {
              console.log("\n[!] Gagal mengirim permintaan.");
              setTimeout(1500).then(showMainMenu);
            } else {
              // --- TAMBAHKAN TIGA BARIS INI ---
              clientState.activeSession = res.session_id;
              clientState.initialMessage = msg;
              clientState.currentStatus = "PENDING";
              // --------------------------------

              showMainMenu();
            }
          });
        });
        break;
      case '2':
        showMainMenu();
        break;
      case '3':
        console.log("Terima kasih.");
        process.exit();
        break;
      case '4': // LOGIKA CANCEL
        const sessionToCancel = clientService.getCurrentSessionId();
        if (!sessionToCancel) {
          console.log("\n[!] Anda tidak memiliki permintaan aktif.");
          console.clear();
          return showMainMenu();
        }

        rl.question(`\nBatalkan sesi ${sessionToCancel}? (ya/tidak): `, (confirm) => {
          if (confirm.toLowerCase() === 'ya') {
            clientService.cancelEmergency((err, res) => {
              if (err || !res.success) {
                console.log(`\n  Gagal: ${res ? res.message : "Server error"}`);
              } else {
                console.log(`\n  Sesi berhasil dibatalkan.`);
              }
              console.clear();
              showMainMenu();
            });
          } else {
            console.clear();
            showMainMenu();
          }
        });
        break;
      case '5':
        if (!clientState.activeSession) {
          console.log("\n[!] Anda tidak memiliki permintaan aktif.");
          setTimeout(1500).then(showMainMenu);
        } else {
          console.clear();
          startChat(clientService.getStub(), clientService.mylocalprofile.client_id, clientState.activeSession, rl, showMainMenu);
        }
        break;
      default:
        console.clear();
        showMainMenu();
        break;
    }
  });
}

function startStatusWatcher() {
  clientService.watchMyStatus((update) => {
    if (update && update.session) {
      // Mapping untuk menangani Enum yang datang sebagai String atau Number
      const statusMap = {
        "PENDING": 0, 0: 0,
        "ONGOING": 1, 1: 1,
        "DONE": 2, 2: 2,
        "BANNED": 3, 3: 3
      };

      const statusNames = ["PENDING", "ONGOING", "DONE", "BANNED"];
      const sessionData = update.session;

      // Ambil nilai mentah dari status
      const rawStatus = sessionData.status !== undefined ? sessionData.status : sessionData.Status;

      // Konversi ke index angka yang pasti (0-3)
      const statusIndex = statusMap[rawStatus];

      clientState.activeSession = sessionData.session_id || sessionData.session_id;

      // Update status ke dashboard
      clientState.currentStatus = statusNames[statusIndex] !== undefined ? statusNames[statusIndex] : "UNKNOWN";

      clientState.lastInfo = update.notification || "-";

      if (statusIndex === 3) {
        clientState.isBanned = true;
      }

      showMainMenu();

      // Logika pembersihan sesi
      if (statusIndex === 2 || statusIndex === 3) {
        setTimeout(5000).then(() => {
          clientState.activeSession = null;
          clientState.initialMessage = "-";
          showMainMenu();
        });
      }
    }
  });
}

function startApp() {
  console.clear();
  console.log("=== PENDAFTARAN CLIENT 911 ===");
  rl.question("Masukkan Nomor Telepon: ", (phone) => {
    if (!phone) {
      console.log("nomor telepon tidak ditemukan");
      return startApp();
    }
    clientService.register(phone, (err, response) => {
      if (err || !response.success) {
        console.log(`registrasi gagal: ${err ? err.message : response.message}`);
        return startApp();
      }
      console.clear();
      console.log(`Registrasi berhasil`);
      console.log(`Pesan: ${response.message}`);
      console.clear();
      startStatusWatcher();
      showMainMenu();
    });
  });
}

startApp();