import * as startHelper from "./index.js";
import startChat from "./helper_chat.js";
import { setTimeout } from 'node:timers/promises';
import readline from "readline";
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});
const state = {
  profile: null,
  sessions: []
};

async function showDashboard() {
  console.clear();
  console.log(`\n=== DASHBOARD AGENT: ${state.profile.name} ===`);
  console.log(`ID: ${state.profile.agent_id} | Dept: ${state.profile.department}`);
  console.log(`-------------------------------------------`);

  console.log("\n--- SESI DARURAT SAAT INI ---");
  if (state.sessions.length === 0) {
    console.log("Tidak ada panggilan aktif.");
  } else {
    state.sessions.forEach((s, i) => console.log(`${i + 1}. Session ID: ${s}`));
  }
  console.log(`-------------------------------------------`);

  console.log("1. Assign Helper to CLient");
  console.log("2. Chat");
  console.log("3. Logout \u0026 Exit");

  rl.question("\nPilih Menu: ", (choice) => {
    if (choice === '1') {
      console.log("\n--- DAFTAR ANTREAN ---");
      if (state.sessions.length === 0) {
        console.log("Tidak ada panggilan aktif.");
        return showDashboard();
      }

      state.sessions.forEach((s, i) => console.log(`${i + 1}. ID: ${s}`));

      rl.question("\nPilih nomor sesi untuk diproses (atau 'b' untuk kembali): ", (idx) => {
        if (idx.toLowerCase() === 'b') return showDashboard();

        const selectedSession = state.sessions[parseInt(idx) - 1];
        if (!selectedSession) return showDashboard();

        console.log(`\nProses Sesi: ${selectedSession}`);
        console.log("1. Assign (On Going)");
        console.log("2. Mark as Done");
        console.log("3. Ban/Reject");

        rl.question("Pilih aksi: ", (action) => {
          let statusCode = (action === '1') ? 1 : (action === '2') ? 2 : (action === '3') ? 3 : null;

          if (statusCode) {
            startHelper.updateStatus(selectedSession, statusCode, state.profile.agent_id, (err, res) => {
              if (!err && res.success) {
                console.log(`\n  ${res.message}`);
                if (statusCode > 1) {
                  state.sessions = state.sessions.filter(id => id !== selectedSession);
                }
              } else {
                console.log("\n  Gagal update status.");
              }
              showDashboard();
            });
          } else {
            showDashboard();
          }
        });
      });
    } else if (choice === '2') {
      if (state.sessions.length === 0) {
        console.log("\n[!] Tidak ada panggilan aktif untuk di-chat.");
        setTimeout(1500).then(showDashboard);
      } else {
        rl.question("\nPilih nomor sesi untuk di-chat: ", (idx) => {
          const selectedSession = state.sessions[parseInt(idx) - 1];
          if (!selectedSession) return showDashboard();
          console.clear();
          startChat(startHelper.getStub(), state.profile.agent_id, selectedSession, rl, showDashboard);
        });
      }
    } else if (choice === '3') {
      console.log("Logging out...");
      startHelper.logout(state.profile.agent_id, () => {
        process.exit();
      });
    } else {
      showDashboard();
    }
  });
}

function startApp() {
  console.clear();
  console.log("=== EMERGENCY SYSTEM LOGIN ===");
  rl.question("Agent ID   : ", (id) => {
    rl.question("Password   : ", (pass) => {
      startHelper.login(id, pass, (err, response) => {
        if (err || !response.success) {
          console.log(`\n  LOGIN GAGAL: ${response ? response.message : "Server Down"}`);
          return startApp();
        }

        state.profile = response.agent;
        const address = response.message.match(/localhost:\d+/)[0];

        startHelper.startHelper(address, (newSessionId) => {
          state.sessions.push(newSessionId);
          console.log(`\n\n  PANGGILAN MASUK! ID Sesi: ${newSessionId}`);
          process.stdout.write("Pilih Menu: "); // Kembalikan prompt menu
        });

        console.clear();
        console.log(`  Login Berhasil sebagai ${state.profile.name}`);
        console.clear();
        startHelper.watchQueue(state.profile.agent_id, state.profile.department, async (update) => {
          if (update.PENDING) {
            state.sessions = update.PENDING.map(s => s.session_id); // Ganti list lama dengan yang baru [cite: 86]
          }

          if (update.new_session) {
            if (!state.sessions.includes(update.new_session.session_id)) {
              state.sessions.push(update.new_session.session_id);
            }
            console.log(`\n[QUEUE UPDATE] Ada permintaan baru: ${update.new_session.session_id}`);
            await setTimeout(1000);
            console.clear();
            showDashboard();
          }

          if (update.event && update.event.startsWith("CANCEL_EVENT:")) {
            const cancelledId = update.event.split(":")[1];
            console.log(`\n[NOTIF] Sesi ${cancelledId} telah dibatalkan oleh Client.`);
            state.sessions = state.sessions.filter(id => id !== cancelledId);
            await setTimeout(1000);
            console.clear();
            showDashboard();
          }

          if (update.event && update.event.startsWith("END_EVENT:")) {
            const endedId = update.event.split(":")[1];
            console.log(`\n[NOTIF] Sesi ${endedId} telah selesai/ditutup dari Web.`);
            state.sessions = state.sessions.filter(id => id !== endedId);
            await setTimeout(1000);
            console.clear();
            showDashboard();
          }

          if (!update.new_session && !(update.event && (update.event.startsWith("CANCEL_EVENT:") || update.event.startsWith("END_EVENT:")))) {
            process.stdout.write("Pilih Menu: ");
          }
        });
        console.clear();
        showDashboard();
      });
    });
  });
}

startApp();