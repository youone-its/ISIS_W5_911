/**
 * web_socket.js  —  Emergency 911 WebSocket Dashboard Module
 *
 * Cara pakai:
 *   1. npm install ws
 *   2. Di server/index.js, import & panggil hook di tempat yang tepat (lihat komentar di bawah)
 *   3. Akses dashboard di http://localhost:8080
 *
 * ─── Modifikasi yang diperlukan di server/index.js ───────────────────────────
 *
 *  TOP OF FILE — tambahkan import:
 *    import * as wsDashboard from '../web_socket.js';
 *
 *  Login handler  (setelah loggedInHelpers.set):
 *    wsDashboard.onHelperLogin(agent_id, helperData);
 *
 *  Logout handler (setelah loggedInHelpers.delete):
 *    wsDashboard.onHelperLogout(agent_id);
 *
 *  WatchQueue 'cancelled' (setelah helperStreams.delete & loggedInHelpers.delete):
 *    wsDashboard.onHelperLogout(agent_id);
 *
 *  RequestEmergency (setelah queues[type].push(newSession)):
 *    wsDashboard.onQueueUpdate(queues);
 *
 *  CancelEmergency (setelah foundInQueue block):
 *    wsDashboard.onQueueUpdate(queues);
 *
 *  UpdateSessionStatus — tambahkan setelah section update status di memory:
 *    if (parseInt(status) === 1) wsDashboard.onSessionAssigned(agent_id, session_id);
 *    if (status === 2 || status === 3) {
 *      wsDashboard.onSessionEnded(session_id);
 *      wsDashboard.onQueueUpdate(queues);
 *    }
 *
 *  chatStreamHandler — di dalam blok broadcast (setelah forEach c.write(msg)):
 *    if (msg.content) wsDashboard.onChatMessage(currentSessionId, msg);
 *
 *  server.bindAsync callback (setelah server.start()):
 *    wsDashboard.init(8080);
 * ─────────────────────────────────────────────────────────────────────────────
 */
 
import { WebSocketServer } from 'ws';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
 
// ═══════════════════════════════════════════════════════════════════════════════
//  INTERNAL STATE
// ═══════════════════════════════════════════════════════════════════════════════
 
/** @type {Map<string, {agent_id, name, department, status:'LUANG'|'MISI', session_id:string|null}>} */
const helperCards  = new Map();
const queueSnap    = { FIRE: [], MEDICAL: [], POLICE: [] };
const chatWatchers = new Map();   // sessionId → Set<WebSocket>
const sessionAgent = new Map();   // sessionId → agentId
 
/** @type {WebSocketServer|null} */
let wss = null;
 
// ═══════════════════════════════════════════════════════════════════════════════
//  INTERNAL UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════
 
const safeSend = (ws, payload) => {
  if (ws.readyState === 1) ws.send(JSON.stringify(payload));
};
 
const broadcast = (event, data) => {
  if (!wss) { console.log(`[WS] broadcast SKIP — wss null (event: ${event})`); return; }
  const clientCount = [...wss.clients].filter(c => c.readyState === 1).length;
  console.log(`[WS] broadcast → ${event} | klien aktif: ${clientCount}`);
  const msg = JSON.stringify({ event, data, ts: Date.now() });
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
};
 
const getFullSnapshot = () => ({
  helpers: [...helperCards.values()],
  queues : { FIRE: [...queueSnap.FIRE], MEDICAL: [...queueSnap.MEDICAL], POLICE: [...queueSnap.POLICE] }
});
 
// ═══════════════════════════════════════════════════════════════════════════════
//  WEBSOCKET SERVER INIT
// ═══════════════════════════════════════════════════════════════════════════════
 
export function init(port = 8080) {
  const httpServer = http.createServer((req, res) => {
    let filePath = '';
    let contentType = 'text/html';

    if (req.url === '/' || req.url === '/index.html') {
      filePath = path.join(__dirname, '../dashboard/index.html');
      contentType = 'text/html';
    } else if (req.url === '/style.css') {
      filePath = path.join(__dirname, '../dashboard/style.css');
      contentType = 'text/css';
    } else if (req.url === '/script.js') {
      filePath = path.join(__dirname, '../dashboard/script.js');
      contentType = 'text/javascript';
    }

    if (filePath) {
      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(404);
          res.end('File not found');
          return;
        }
        res.writeHead(200, { 
          'Content-Type': contentType,
          'Cache-Control': 'no-store, no-cache, must-revalidate',
          'Pragma': 'no-cache',  
        });
        res.end(data);
      });
    } else {
      res.writeHead(404);
      res.end('Not found');
    }
  });

  wss = new WebSocketServer({ server: httpServer });
 
  wss.on('connection', (ws) => {
    console.log('[WS] Dashboard terhubung');
    // Kirim state awal saat klien connect
    safeSend(ws, { event: 'INIT', data: getFullSnapshot(), ts: Date.now() });
 
    ws.on('message', (raw) => {
      try { handleDashboardMsg(ws, JSON.parse(raw.toString())); } catch (_) {}
    });
 
    ws.on('close', () => {
      // Bersihkan semua referensi watcher saat koneksi ditutup
      chatWatchers.forEach(set => set.delete(ws));
      console.log('[WS] Dashboard terputus');
    });
  });
 
  httpServer.listen(port, () =>
    console.log(`[WS] Admin Dashboard → http://localhost:${port}`)
  );
}
 
function handleDashboardMsg(ws, msg) {
  switch (msg.type) {
    case 'WATCH_CHAT': {
      const sid = msg.session_id;
      if (!sid) return;
      if (!chatWatchers.has(sid)) chatWatchers.set(sid, new Set());
      chatWatchers.get(sid).add(ws);
      safeSend(ws, { event: 'CHAT_WATCHING', data: { session_id: sid }, ts: Date.now() });
      break;
    }
    case 'UNWATCH_CHAT': {
      const sid = msg.session_id;
      if (sid && chatWatchers.has(sid)) chatWatchers.get(sid).delete(ws);
      safeSend(ws, { event: 'CHAT_UNWATCHED', data: { session_id: sid }, ts: Date.now() });
      break;
    }
    case 'GET_STATE':
      safeSend(ws, { event: 'INIT', data: getFullSnapshot(), ts: Date.now() });
      break;
  }
}
 
// ═══════════════════════════════════════════════════════════════════════════════
//  PUBLIC API — dipanggil dari server/index.js
// ═══════════════════════════════════════════════════════════════════════════════
 
/**
 * Panggil saat helper berhasil login.
 * @param {string} agentId
 * @param {{name: string, department: string}} data
 */
export function onHelperLogin(agentId, data) {
  const cleanId = (agentId || '').trim();
  console.log(`[WS] onHelperLogin → agentId="${cleanId}" name="${data.name}" dept="${data.department}"`);
  
  helperCards.set(cleanId, {
    agent_id  : cleanId,
    name      : data.name,
    department: data.department,
    status    : 'LUANG',
    session_id: null
  });
  broadcast('HELPER_LOGIN', { helper: helperCards.get(cleanId) });
}
 
/**
 * Panggil saat helper logout atau koneksi stream terputus.
 * @param {string} agentId
 */
export function onHelperLogout(agentId) {
  const cleanId = (agentId || '').trim();
  console.log(`[WS] onHelperLogout → agentId="${cleanId}"`);
  helperCards.delete(cleanId);
  broadcast('HELPER_LOGOUT', { agent_id: cleanId });
}
 
/**
 * Panggil setiap kali antrean berubah (request baru, cancel, selesai).
 * @param {{FIRE: Array, MEDICAL: Array, POLICE: Array}} queues
 */
export function onQueueUpdate(queues) {
  ['FIRE', 'MEDICAL', 'POLICE'].forEach(k => {
    queueSnap[k] = (queues[k] || []).map(({ session_id, client_id, initial_message, status }) =>
      ({ session_id, client_id, initial_message, status })
    );
  });
  console.log(`[WS] onQueueUpdate → Broadcasting full snapshot (${helperCards.size} helpers)`);
  broadcast('QUEUE_UPDATE', getFullSnapshot());
}
 
/**
 * Panggil saat helper mengambil sesi (UpdateSessionStatus status=1 / ONGOING).
 * @param {string} agentId
 * @param {string} sessionId
 */
export function onSessionAssigned(agentId, sessionId) {
  const cleanId = (agentId || '').trim();
  console.log(`[WS] onSessionAssigned → agentId="${cleanId}" sessionId="${sessionId}" helperCards.has=${helperCards.has(cleanId)}`);
  
  sessionAgent.set(sessionId, cleanId);
  
  if (helperCards.has(cleanId)) {
    const h = helperCards.get(cleanId);
    h.status     = 'MISI';
    h.session_id = sessionId;
    console.log(`[WS] Updated helperCards for ${cleanId} to MISI`);
  } else {
    console.warn(`[WS] WARNING: agentId "${cleanId}" not found in helperCards during assignment!`);
  }
  
  broadcast('HELPER_STATUS_CHANGE', { agent_id: cleanId, status: 'MISI', session_id: sessionId });
}
 
/**
 * Panggil saat sesi selesai / dibatalkan (status=2 DONE atau status=3 BANNED).
 * @param {string} sessionId
 */
export function onSessionEnded(sessionId) {
  const rawAgentId = sessionAgent.get(sessionId);
  const cleanId = (rawAgentId || '').trim();
  sessionAgent.delete(sessionId);
  
  console.log(`[WS] onSessionEnded → session="${sessionId}" agentId="${cleanId}"`);
  
  // Update helperCards jika ada (opsional, untuk snapshot)
  if (cleanId && helperCards.has(cleanId)) {
    const h = helperCards.get(cleanId);
    h.status     = 'LUANG';
    h.session_id = null;
    console.log(`[WS] Updated helperCards for ${cleanId} to LUANG`);
  }
  
  // Broadcast SELALU jika agentId diketahui — jangan block di balik guard helperCards
  if (cleanId) {
    broadcast('HELPER_STATUS_CHANGE', { agent_id: cleanId, status: 'LUANG', session_id: null });
  }
 
  // Beritahu semua dashboard yang sedang menonton chat sesi ini
  if (chatWatchers.has(sessionId)) {
    chatWatchers.get(sessionId).forEach(ws =>
      safeSend(ws, { event: 'CHAT_SESSION_ENDED', data: { session_id: sessionId }, ts: Date.now() })
    );
    chatWatchers.delete(sessionId);
  }
}
 
/**
 * Panggil setiap kali ada pesan masuk di chatStreamHandler.
 * @param {string} sessionId
 * @param {{sender_id: string, content: string, status?: string}} message
 */
export function onChatMessage(sessionId, message) {
  if (!chatWatchers.has(sessionId)) return;
  chatWatchers.get(sessionId).forEach(ws =>
    safeSend(ws, { event: 'CHAT_MESSAGE', data: { session_id: sessionId, message }, ts: Date.now() })
  );
}
