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
  const httpServer = http.createServer((_req, res) => {
    res.writeHead(200, { 
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store, no-cache, must-revalidate',
      'Pragma': 'no-cache',  
    });
    res.end(DASHBOARD_HTML);
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
  console.log(`[WS] onHelperLogin → agentId="${agentId}" name="${data.name}" dept="${data.department}"`);
  helperCards.set(agentId, {
    agent_id  : agentId,
    name      : data.name,
    department: data.department,
    status    : 'LUANG',
    session_id: null
  });
  broadcast('HELPER_LOGIN', { helper: helperCards.get(agentId) });
}
 
/**
 * Panggil saat helper logout atau koneksi stream terputus.
 * @param {string} agentId
 */
export function onHelperLogout(agentId) {
  helperCards.delete(agentId);
  broadcast('HELPER_LOGOUT', { agent_id: agentId });
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
  broadcast('QUEUE_UPDATE', { queues: getFullSnapshot().queues });
}
 
/**
 * Panggil saat helper mengambil sesi (UpdateSessionStatus status=1 / ONGOING).
 * @param {string} agentId
 * @param {string} sessionId
 */
export function onSessionAssigned(agentId, sessionId) {
  console.log(`[WS] onSessionAssigned → agentId="${agentId}" sessionId="${sessionId}" helperCards.has=${helperCards.has(agentId)}`);
  sessionAgent.set(sessionId, agentId);
  // Update helperCards jika ada (opsional, untuk snapshot)
  if (helperCards.has(agentId)) {
    const h = helperCards.get(agentId);
    h.status     = 'MISI';
    h.session_id = sessionId;
  }
  // Broadcast SELALU — jangan block di balik guard helperCards
  broadcast('HELPER_STATUS_CHANGE', { agent_id: agentId, status: 'MISI', session_id: sessionId });
}
 
/**
 * Panggil saat sesi selesai / dibatalkan (status=2 DONE atau status=3 BANNED).
 * @param {string} sessionId
 */
export function onSessionEnded(sessionId) {
  const agentId = sessionAgent.get(sessionId);
  sessionAgent.delete(sessionId);
  // Update helperCards jika ada (opsional, untuk snapshot)
  if (agentId && helperCards.has(agentId)) {
    const h = helperCards.get(agentId);
    h.status     = 'LUANG';
    h.session_id = null;
  }
  // Broadcast SELALU jika agentId diketahui — jangan block di balik guard helperCards
  if (agentId) {
    broadcast('HELPER_STATUS_CHANGE', { agent_id: agentId, status: 'LUANG', session_id: null });
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
 
// ═══════════════════════════════════════════════════════════════════════════════
//  EMBEDDED DASHBOARD HTML
// ═══════════════════════════════════════════════════════════════════════════════
 
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>911 DISPATCH — ADMIN DASHBOARD</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@400;600;700;800&display=swap" rel="stylesheet"/>
<style>
  :root{
    --bg:        #060a0f;
    --surface:   #0c1219;
    --surface2:  #111820;
    --border:    #1d2d3e;
    --border2:   #263545;
    --text:      #cdd9e5;
    --muted:     #4a6478;
    --amber:     #f0a500;
    --amber-dim: #7a5200;
    --fire:      #ff4d1c;
    --fire-dim:  #4a1200;
    --medical:   #00d4a0;
    --medical-dim:#003d2e;
    --police:    #3d8bff;
    --police-dim:#0d2a55;
    --luang:     #22d46a;
    --misi:      #f59e0b;
    --danger:    #ef4444;
    --font-mono: 'Share Tech Mono', monospace;
    --font-ui:   'Barlow Condensed', sans-serif;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--font-ui)}
  body{display:flex;flex-direction:column;overflow:hidden}
 
  /* ── SCANLINE OVERLAY ───────────────────────────────── */
  body::after{
    content:'';position:fixed;inset:0;pointer-events:none;z-index:9999;
    background:repeating-linear-gradient(
      to bottom,
      transparent 0px, transparent 3px,
      rgba(0,0,0,.08) 3px, rgba(0,0,0,.08) 4px
    );
  }
 
  /* ── TOPBAR ─────────────────────────────────────────── */
  #topbar{
    display:flex;align-items:center;justify-content:space-between;
    padding:10px 20px;background:var(--surface);
    border-bottom:1px solid var(--border);
    flex-shrink:0;
  }
  .logo{display:flex;align-items:center;gap:12px}
  .logo-badge{
    background:var(--fire);color:#fff;
    font-family:var(--font-mono);font-size:20px;font-weight:700;
    padding:4px 10px;letter-spacing:2px;
    clip-path:polygon(6px 0,100% 0,calc(100% - 6px) 100%,0 100%);
  }
  .logo-title{font-size:18px;font-weight:800;letter-spacing:3px;text-transform:uppercase;color:var(--text)}
  .logo-sub{font-family:var(--font-mono);font-size:10px;color:var(--muted);letter-spacing:2px}
  .topbar-right{display:flex;align-items:center;gap:24px}
  .stat-box{text-align:center}
  .stat-val{font-family:var(--font-mono);font-size:28px;color:var(--amber);line-height:1}
  .stat-lbl{font-size:12px;letter-spacing:2px;color:var(--muted);text-transform:uppercase}
  #conn-status{
    display:flex;align-items:center;gap:8px;
    font-family:var(--font-mono);font-size:13px;
  }
  .conn-dot{width:8px;height:8px;border-radius:50%;background:var(--danger)}
  .conn-dot.live{
    background:var(--luang);
    box-shadow:0 0 8px var(--luang);
    animation:blink 2s ease-in-out infinite;
  }
  @keyframes blink{0%,100%{opacity:1}50%{opacity:.4}}
  #time{font-family:var(--font-mono);font-size:15px;color:var(--amber);min-width:80px;text-align:right}
 
  /* ── MAIN LAYOUT ────────────────────────────────────── */
  #main{display:flex;flex:1;overflow:hidden}
 
  /* ── HELPER PANEL ───────────────────────────────────── */
  #helpers-panel{
    width:340px;flex-shrink:0;
    display:flex;flex-direction:column;
    border-right:1px solid var(--border);
  }
  .panel-head{
    display:flex;align-items:center;justify-content:space-between;
    padding:12px 16px;background:var(--surface);
    border-bottom:1px solid var(--border);flex-shrink:0;
  }
  .panel-head h2{
    font-size:13px;font-weight:700;letter-spacing:3px;
    text-transform:uppercase;color:var(--muted);
  }
  .panel-head .badge{
    font-family:var(--font-mono);font-size:14px;
    background:var(--surface2);border:1px solid var(--border2);
    padding:2px 8px;
  }
  #helpers-list{
    flex:1;overflow-y:auto;padding:12px;
    display:flex;flex-direction:column;gap:8px;
  }
  #helpers-list::-webkit-scrollbar{width:4px}
  #helpers-list::-webkit-scrollbar-track{background:transparent}
  #helpers-list::-webkit-scrollbar-thumb{background:var(--border2)}
 
  /* ── HELPER CARD ────────────────────────────────────── */
  .helper-card{
    background:var(--surface);
    border:1px solid var(--border);
    padding:14px;cursor:default;
    transition:border-color .2s,box-shadow .2s,transform .15s;
    position:relative;overflow:hidden;
  }
  .helper-card::before{
    content:'';position:absolute;left:0;top:0;bottom:0;width:3px;
    background:var(--muted);transition:background .2s;
  }
  .helper-card.dept-FIRE::before{background:var(--fire)}
  .helper-card.dept-MEDICAL::before{background:var(--medical)}
  .helper-card.dept-POLICE::before{background:var(--police)}
  .helper-card.status-MISI{
    cursor:pointer;
    border-color:var(--misi);
  }
  .helper-card.status-MISI:hover{
    box-shadow:0 0 16px rgba(245,158,11,.15);
    transform:translateX(2px);
  }
  .helper-card.status-MISI::after{
    content:'KLIK → PANTAU CHAT';
    position:absolute;bottom:8px;right:10px;
    font-family:var(--font-mono);font-size:11px;color:var(--misi);
    letter-spacing:1px;opacity:.7;
  }
  .card-top{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:8px}
  .card-name{font-size:19px;font-weight:700;letter-spacing:.5px;line-height:1.2}
  .card-id{font-family:var(--font-mono);font-size:12px;color:var(--muted);margin-top:2px}
  .status-badge{
    font-family:var(--font-mono);font-size:12px;font-weight:700;
    letter-spacing:2px;padding:3px 8px;flex-shrink:0;
  }
  .status-badge.LUANG{
    background:rgba(34,212,106,.12);border:1px solid rgba(34,212,106,.4);
    color:var(--luang);
  }
  .status-badge.MISI{
    background:rgba(245,158,11,.15);border:1px solid rgba(245,158,11,.5);
    color:var(--misi);
    animation:pulse-amber 1.5s ease-in-out infinite;
  }
  @keyframes pulse-amber{
    0%,100%{box-shadow:0 0 0 0 rgba(245,158,11,.3)}
    50%{box-shadow:0 0 0 4px rgba(245,158,11,0)}
  }
  .card-dept{
    display:inline-flex;align-items:center;gap:5px;
    font-size:13px;font-weight:700;letter-spacing:2px;
    text-transform:uppercase;
  }
  .card-dept.FIRE{color:var(--fire)}
  .card-dept.MEDICAL{color:var(--medical)}
  .card-dept.POLICE{color:var(--police)}
  .dept-icon{font-size:15px}
  .card-session{
    margin-top:6px;
    font-family:var(--font-mono);font-size:12px;color:var(--muted);
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  }
  .empty-helpers{
    display:flex;flex-direction:column;align-items:center;justify-content:center;
    flex:1;color:var(--muted);font-family:var(--font-mono);font-size:14px;
    letter-spacing:2px;text-align:center;gap:8px;opacity:.5;
  }
  .empty-icon{font-size:32px;opacity:.3}
 
  /* ── QUEUE PANEL ────────────────────────────────────── */
  #queue-panel{
    flex:1;display:flex;flex-direction:column;overflow:hidden;
  }
  #queue-container{
    flex:1;overflow-y:auto;padding:16px;
    display:grid;grid-template-columns:repeat(3,1fr);gap:16px;
    align-content:start;
  }
  #queue-container::-webkit-scrollbar{width:4px}
  #queue-container::-webkit-scrollbar-thumb{background:var(--border2)}
 
  .queue-col{display:flex;flex-direction:column;gap:8px}
  .queue-header{
    display:flex;align-items:center;justify-content:space-between;
    padding:10px 12px;background:var(--surface);
    border:1px solid var(--border);margin-bottom:4px;
  }
  .queue-title{
    font-size:15px;font-weight:800;letter-spacing:3px;
    text-transform:uppercase;display:flex;align-items:center;gap:8px;
  }
  .q-FIRE .queue-title{color:var(--fire)}
  .q-MEDICAL .queue-title{color:var(--medical)}
  .q-POLICE .queue-title{color:var(--police)}
  .queue-count{
    font-family:var(--font-mono);font-size:24px;font-weight:700;
  }
  .q-FIRE .queue-count{color:var(--fire)}
  .q-MEDICAL .queue-count{color:var(--medical)}
  .q-POLICE .queue-count{color:var(--police)}
  .queue-bar{height:2px;margin:0 12px 0;
    transition:background .3s;
  }
  .q-FIRE   .queue-bar{background:linear-gradient(90deg,var(--fire),transparent)}
  .q-MEDICAL .queue-bar{background:linear-gradient(90deg,var(--medical),transparent)}
  .q-POLICE  .queue-bar{background:linear-gradient(90deg,var(--police),transparent)}
 
  .queue-item{
    background:var(--surface);border:1px solid var(--border);
    padding:10px 12px;
    animation:slide-in .25s ease-out;
  }
  @keyframes slide-in{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:translateY(0)}}
  .qi-sid{font-family:var(--font-mono);font-size:12px;color:var(--muted);margin-bottom:4px}
  .qi-client{font-size:14px;font-weight:600;letter-spacing:1px;margin-bottom:4px}
  .qi-msg{
    font-size:13px;color:var(--muted);
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    font-style:italic;
  }
  .qi-status{
    margin-top:6px;
    font-family:var(--font-mono);font-size:11px;letter-spacing:1.5px;
  }
  .qi-status.s0{color:var(--amber)}
  .qi-status.s1{color:var(--luang)}
  .empty-queue{
    text-align:center;padding:20px;
    font-family:var(--font-mono);font-size:12px;
    color:var(--muted);letter-spacing:2px;opacity:.4;
  }
 
  /* ── CHAT MODAL ─────────────────────────────────────── */
  #chat-overlay{
    position:fixed;inset:0;background:rgba(0,0,0,.7);
    display:none;align-items:flex-end;justify-content:center;
    z-index:1000;backdrop-filter:blur(4px);
    padding:20px;
  }
  #chat-overlay.open{display:flex}
  #chat-modal{
    background:var(--surface);border:1px solid var(--border2);
    width:100%;max-width:680px;
    display:flex;flex-direction:column;
    max-height:70vh;
    animation:rise .3s cubic-bezier(.22,.68,0,1.2);
  }
  @keyframes rise{from{opacity:0;transform:translateY(30px)}to{opacity:1;transform:translateY(0)}}
  #chat-modal-head{
    display:flex;align-items:center;justify-content:space-between;
    padding:12px 16px;background:var(--surface2);
    border-bottom:1px solid var(--border);flex-shrink:0;
  }
  #chat-modal-title{
    font-size:15px;font-weight:700;letter-spacing:2px;
    text-transform:uppercase;display:flex;align-items:center;gap:10px;
  }
  .watching-dot{
    width:7px;height:7px;border-radius:50%;background:var(--luang);
    box-shadow:0 0 6px var(--luang);
    animation:blink 1s ease-in-out infinite;
  }
  #chat-modal-sub{font-family:var(--font-mono);font-size:12px;color:var(--muted)}
  #chat-close{
    background:none;border:1px solid var(--border2);color:var(--muted);
    padding:4px 12px;font-family:var(--font-mono);font-size:13px;
    cursor:pointer;letter-spacing:1px;
    transition:border-color .15s,color .15s;
  }
  #chat-close:hover{border-color:var(--danger);color:var(--danger)}
  #chat-messages{
    flex:1;overflow-y:auto;padding:16px;
    display:flex;flex-direction:column;gap:8px;
    min-height:200px;
  }
  #chat-messages::-webkit-scrollbar{width:4px}
  #chat-messages::-webkit-scrollbar-thumb{background:var(--border2)}
  .msg-row{display:flex;gap:10px;align-items:flex-start;animation:slide-in .2s ease-out}
  .msg-sender{
    font-family:var(--font-mono);font-size:13px;
    min-width:90px;text-align:right;padding-top:2px;flex-shrink:0;
  }
  .msg-sender.client{color:var(--luang)}
  .msg-sender.helper{color:var(--amber)}
  .msg-sender.system{color:var(--muted)}
  .msg-bubble{
    background:var(--surface2);border:1px solid var(--border);
    padding:8px 12px;font-size:15px;flex:1;max-width:460px;
  }
  .msg-bubble.system-bubble{
    border-color:var(--border2);color:var(--muted);
    font-style:italic;font-size:13px;font-family:var(--font-mono);
  }
  #chat-footer{
    padding:10px 16px;background:var(--surface2);
    border-top:1px solid var(--border);
    font-family:var(--font-mono);font-size:12px;color:var(--muted);
    display:flex;align-items:center;gap:8px;flex-shrink:0;
  }
  #chat-footer .watching-dot{width:6px;height:6px}
  .ended-notice{
    text-align:center;padding:12px;
    font-family:var(--font-mono);font-size:13px;
    color:var(--danger);letter-spacing:1.5px;
    border:1px solid rgba(239,68,68,.3);background:rgba(239,68,68,.05);
  }
 
  /* ── LOG TICKER ─────────────────────────────────────── */
  #log-bar{
    height:32px;background:var(--surface);
    border-top:1px solid var(--border);
    overflow:hidden;display:flex;align-items:center;
    padding:0 12px;flex-shrink:0;
    font-family:var(--font-mono);font-size:12px;color:var(--muted);
    gap:8px;
  }
  #log-label{color:var(--amber);letter-spacing:2px;flex-shrink:0}
  #log-content{white-space:nowrap;overflow:hidden}
 
  /* ── DEPT ICON HELPER ───────────────────────────────── */
  .deptIcon-FIRE    { content:'🔥' }
  .deptIcon-MEDICAL { content:'🏥' }
  .deptIcon-POLICE  { content:'🚔' }
</style>
</head>
<body>
 
<!-- TOPBAR -->
<div id="topbar">
  <div class="logo">
    <div class="logo-badge">911</div>
    <div>
      <div class="logo-title">Dispatch Monitor</div>
      <div class="logo-sub">Emergency Response Admin Dashboard</div>
    </div>
  </div>
  <div class="topbar-right">
    <div class="stat-box">
      <div class="stat-val" id="stat-helpers">0</div>
      <div class="stat-lbl">Helpers Online</div>
    </div>
    <div class="stat-box">
      <div class="stat-val" id="stat-misi">0</div>
      <div class="stat-lbl">Sedang Misi</div>
    </div>
    <div class="stat-box">
      <div class="stat-val" id="stat-queue">0</div>
      <div class="stat-lbl">Total Antrean</div>
    </div>
    <div id="conn-status">
      <div class="conn-dot" id="conn-dot"></div>
      <span id="conn-text">DISCONNECTED</span>
    </div>
    <div id="time">--:--:--</div>
  </div>
</div>
 
<!-- MAIN AREA -->
<div id="main">
 
  <!-- HELPER PANEL -->
  <div id="helpers-panel">
    <div class="panel-head">
      <h2>Active Helpers</h2>
      <span class="badge" id="helper-badge">0</span>
    </div>
    <div id="helpers-list">
      <div class="empty-helpers" id="helpers-empty">
        <div class="empty-icon">📡</div>
        <div>MENUNGGU HELPER LOGIN...</div>
      </div>
    </div>
  </div>
 
  <!-- QUEUE PANEL -->
  <div id="queue-panel">
    <div class="panel-head">
      <h2>Queue Monitor</h2>
      <span class="badge" id="queue-badge">REAL-TIME</span>
    </div>
    <div id="queue-container">
      <div class="queue-col q-FIRE" id="col-FIRE">
        <div class="queue-header">
          <div class="queue-title">🔥 Fire</div>
          <div class="queue-count" id="cnt-FIRE">0</div>
        </div>
        <div class="queue-bar"></div>
        <div id="list-FIRE"></div>
      </div>
      <div class="queue-col q-MEDICAL" id="col-MEDICAL">
        <div class="queue-header">
          <div class="queue-title">🏥 Medical</div>
          <div class="queue-count" id="cnt-MEDICAL">0</div>
        </div>
        <div class="queue-bar"></div>
        <div id="list-MEDICAL"></div>
      </div>
      <div class="queue-col q-POLICE" id="col-POLICE">
        <div class="queue-header">
          <div class="queue-title">🚔 Police</div>
          <div class="queue-count" id="cnt-POLICE">0</div>
        </div>
        <div class="queue-bar"></div>
        <div id="list-POLICE"></div>
      </div>
    </div>
  </div>
</div>
 
<!-- CHAT MONITOR MODAL -->
<div id="chat-overlay">
  <div id="chat-modal">
    <div id="chat-modal-head">
      <div>
        <div id="chat-modal-title">
          <span class="watching-dot"></span>
          <span id="chat-modal-title-text">MEMANTAU CHAT</span>
        </div>
        <div id="chat-modal-sub"></div>
      </div>
      <button id="chat-close">✕ TUTUP</button>
    </div>
    <div id="chat-messages"></div>
    <div id="chat-footer">
      <span class="watching-dot"></span>
      <span id="chat-footer-text">Menonton secara real-time &mdash; pesan baru akan muncul otomatis</span>
    </div>
  </div>
</div>
 
<!-- LOG TICKER -->
<div id="log-bar">
  <span id="log-label">LOG ▶</span>
  <span id="log-content">Menghubungkan ke server...</span>
</div>
 
<script>
(function () {
  // ── STATE ──────────────────────────────────────────────────────────────────
  const helpers   = new Map();   // agentId → helper object
  const queues    = { FIRE: [], MEDICAL: [], POLICE: [] };
  let watchingSid = null;
 
  // ── WEBSOCKET ──────────────────────────────────────────────────────────────
  let ws;
  let reconnectTimer;
 
  function connect() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(protocol + '//' + location.host);
 
    ws.onopen = function () {
      setConnected(true);
      pushLog('Koneksi dashboard berhasil');
    };
 
    ws.onclose = function () {
      setConnected(false);
      pushLog('Koneksi terputus — mencoba ulang dalam 3 detik...');
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connect, 3000);
    };
 
    ws.onerror = function () {
      pushLog('WebSocket error — pastikan server berjalan');
    };
 
    ws.onmessage = function (e) {
      try {
        const pkt = JSON.parse(e.data);
        console.log('[MSG]', pkt.event);
        handlePacket(pkt);
      } catch (_) {}
    };
  }
 
  function send(obj) {
    if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
  }
 
  // ── PACKET HANDLER ─────────────────────────────────────────────────────────
  function handlePacket(pkt) {
    switch (pkt.event) {
 
      case 'INIT': {
        const data = pkt.data;
        console.log('[INIT] helpers count:', (data.helpers||[]).length, 'data:', JSON.stringify(data.helpers));
        helperList = (data.helpers || []).slice();
        (data.helpers || []).forEach(function (h) { helpers.set(h.agent_id, h); });
        Object.assign(queues, data.queues || {});
        renderAll();
        pushLog('State awal diterima — ' + helpers.size + ' helper aktif');
        break;
      }
 
      case 'HELPER_LOGIN': {
        const h = pkt.data.helper;
        helpers.set(h.agent_id, h);
        renderHelpers();
        updateStats();
        pushLog('[+] Helper login: ' + h.name + ' (' + h.department + ')');
        break;
      }
 
      case 'HELPER_LOGOUT': {
        const id = pkt.data.agent_id;
        const name = helpers.has(id) ? helpers.get(id).name : id;
        helpers.delete(id);
        renderHelpers();
        updateStats();
        pushLog('[-] Helper logout: ' + name);
        break;
      }
 
      case 'HELPER_STATUS_CHANGE': {
        const { agent_id, status, session_id } = pkt.data;
        if (!helpers.has(agent_id)) {
          send({ type: 'GET_STATE' });
          break;
        }
        const h = helpers.get(agent_id);
        h.status     = status;
        h.session_id = session_id;
        renderHelpers();
        updateStats();
        pushLog('● ' + h.name + ' → ' + status + (session_id ? ' [' + session_id + ']' : ''));
        break;
      }
 
      case 'QUEUE_UPDATE': {
        console.log('[QUEUE_UPDATE] helperList.length before render:', helperList.length);
        Object.assign(queues, pkt.data.queues);
        renderQueues();
        renderHelpers();
        updateStats();
        const total = (queues.FIRE||[]).length + (queues.MEDICAL||[]).length + (queues.POLICE||[]).length;
        pushLog('Queue diperbarui — total ' + total + ' antrean');
        break;
      }
 
      case 'CHAT_WATCHING': {
        pushLog('Memantau chat sesi ' + pkt.data.session_id);
        break;
      }
 
      case 'CHAT_MESSAGE': {
        if (pkt.data.session_id === watchingSid) {
          appendChatMsg(pkt.data.message);
        }
        break;
      }
 
      case 'CHAT_SESSION_ENDED': {
        if (pkt.data.session_id === watchingSid) {
          appendEndedNotice('Sesi telah berakhir');
          document.getElementById('chat-footer-text').textContent = 'Sesi ini telah selesai.';
          pushLog('Sesi yang dipantau telah berakhir: ' + pkt.data.session_id);
        }
        break;
      }
    }
  }
 
  // ── RENDER HELPERS ─────────────────────────────────────────────────────────
  function renderHelpers() {
    const list  = document.getElementById('helpers-list');
    const empty = document.getElementById('helpers-empty');
    const hArr  = [...helpers.values()];

    while (list.firstChild) list.removeChild(list.firstChild);

    if (hArr.length === 0) {
      list.appendChild(empty);
      empty.style.display = 'flex';
      return;
    }

    empty.style.display = 'none';

    hArr.forEach(function (h) {
      const card = document.createElement('div');
      card.className = 'helper-card dept-' + h.department + ' status-' + h.status;
      card.dataset.agentId = h.agent_id;

      const deptIcons = { FIRE: '🔥', MEDICAL: '🏥', POLICE: '🚔' };
      const icon = deptIcons[h.department] || '👤';

      // card-top
      const cardTop = document.createElement('div');
      cardTop.className = 'card-top';

      const infoDiv = document.createElement('div');
      const nameDiv = document.createElement('div');
      nameDiv.className = 'card-name';
      nameDiv.textContent = h.name;
      const idDiv = document.createElement('div');
      idDiv.className = 'card-id';
      idDiv.textContent = h.agent_id;
      infoDiv.appendChild(nameDiv);
      infoDiv.appendChild(idDiv);

      const badge = document.createElement('span');
      badge.className = 'status-badge ' + h.status;
      badge.textContent = h.status;

      cardTop.appendChild(infoDiv);
      cardTop.appendChild(badge);

      // card-dept
      const deptDiv = document.createElement('div');
      deptDiv.className = 'card-dept ' + h.department;
      const iconSpan = document.createElement('span');
      iconSpan.className = 'dept-icon';
      iconSpan.textContent = icon;
      deptDiv.appendChild(iconSpan);
      deptDiv.appendChild(document.createTextNode(h.department));

      card.appendChild(cardTop);
      card.appendChild(deptDiv);

      if (h.session_id) {
        const sessionDiv = document.createElement('div');
        sessionDiv.className = 'card-session';
        sessionDiv.textContent = 'SESSION: ' + h.session_id;
        card.appendChild(sessionDiv);
      }

      if (h.status === 'MISI' && h.session_id) {
        card.addEventListener('click', function () { openChat(h); });
      }

      list.appendChild(card);
    });

    document.getElementById('helper-badge').textContent = hArr.length;
  }
 
  // ── RENDER QUEUES ──────────────────────────────────────────────────────────
  function renderQueues() {
    ['FIRE', 'MEDICAL', 'POLICE'].forEach(function (dept) {
      const items = queues[dept] || [];
      document.getElementById('cnt-' + dept).textContent = items.length;
 
      const listEl = document.getElementById('list-' + dept);
      listEl.innerHTML = '';
 
      if (items.length === 0) {
        const em = document.createElement('div');
        em.className = 'empty-queue';
        em.textContent = 'KOSONG';
        listEl.appendChild(em);
        return;
      }
 
      const statusLabels = ['● PENDING', '✔ ONGOING', '✔ DONE', '✕ BANNED'];
      const statusClass  = ['s0', 's1', 's1', 's1'];
 
      items.forEach(function (s) {
        const el  = document.createElement('div');
        el.className = 'queue-item';
        const st = typeof s.status === 'number' ? s.status : 0;
        el.innerHTML =
          '<div class="qi-sid">' + (s.session_id || '-') + '</div>' +
          '<div class="qi-client">CLIENT: ' + (s.client_id || '-') + '</div>' +
          '<div class="qi-msg">' + (s.initial_message || '-') + '</div>' +
          '<div class="qi-status ' + (statusClass[st] || 's0') + '">' +
            (statusLabels[st] || '● PENDING') +
          '</div>';
        listEl.appendChild(el);
      });
    });
  }
 
  // ── RENDER ALL ─────────────────────────────────────────────────────────────
  function renderAll() {
    renderHelpers();
    renderQueues();
    updateStats();
  }
 
  function updateStats() {
    const hArr = [...helpers.values()];
    const misi  = hArr.filter(function (h) { return h.status === 'MISI'; }).length;
    const total = (queues.FIRE||[]).length + (queues.MEDICAL||[]).length + (queues.POLICE||[]).length;
    document.getElementById('stat-helpers').textContent = helpers.size;
    document.getElementById('stat-misi').textContent    = misi;
    document.getElementById('stat-queue').textContent   = total;
  }
 
  // ── CHAT MODAL ─────────────────────────────────────────────────────────────
  function openChat(helper) {
    watchingSid = helper.session_id;
 
    document.getElementById('chat-modal-title-text').textContent =
      'MEMANTAU CHAT — ' + helper.name.toUpperCase();
    document.getElementById('chat-modal-sub').textContent =
      helper.agent_id + '  ↔  SESSION: ' + helper.session_id;
    document.getElementById('chat-messages').innerHTML = '';
    document.getElementById('chat-footer-text').textContent =
      'Menonton secara real-time — pesan baru akan muncul otomatis';
 
    // Tambahkan pesan info awal
    appendSystemMsg('Mulai memantau sesi ini. Hanya pesan baru yang ditampilkan.');
 
    document.getElementById('chat-overlay').classList.add('open');
 
    // Beritahu server bahwa kita ingin watch session ini
    send({ type: 'WATCH_CHAT', session_id: helper.session_id });
  }
 
  function closeChat() {
    if (watchingSid) {
      send({ type: 'UNWATCH_CHAT', session_id: watchingSid });
    }
    watchingSid = null;
    document.getElementById('chat-overlay').classList.remove('open');
  }
 
  function appendChatMsg(msg) {
    const container = document.getElementById('chat-messages');
    const row = document.createElement('div');
    row.className = 'msg-row';
 
    const isHelper = msg.sender_id && msg.sender_id.startsWith('H');
    const isSys    = msg.sender_id === 'SYSTEM';
 
    const senderClass  = isSys ? 'system' : (isHelper ? 'helper' : 'client');
    const senderLabel  = isSys ? 'SYS' : (isHelper ? msg.sender_id : 'CLIENT');
    const bubbleExtra  = isSys ? ' system-bubble' : '';
 
    row.innerHTML =
      '<div class="msg-sender ' + senderClass + '">' + senderLabel + '</div>' +
      '<div class="msg-bubble' + bubbleExtra + '">' + escHtml(msg.content || '') + '</div>';
 
    container.appendChild(row);
    container.scrollTop = container.scrollHeight;
  }
 
  function appendSystemMsg(text) {
    appendChatMsg({ sender_id: 'SYSTEM', content: text });
  }
 
  function appendEndedNotice(text) {
    const container = document.getElementById('chat-messages');
    const el = document.createElement('div');
    el.className = 'ended-notice';
    el.textContent = '✕ ' + text.toUpperCase();
    container.appendChild(el);
    container.scrollTop = container.scrollHeight;
  }
 
  document.getElementById('chat-close').addEventListener('click', closeChat);
  document.getElementById('chat-overlay').addEventListener('click', function (e) {
    if (e.target === this) closeChat();
  });
 
  // ── LOG TICKER ─────────────────────────────────────────────────────────────
  const logQueue = [];
  function pushLog(msg) {
    const ts   = new Date().toLocaleTimeString('id-ID', { hour12: false });
    logQueue.push('[' + ts + '] ' + msg);
    if (logQueue.length > 12) logQueue.shift();
    document.getElementById('log-content').textContent = logQueue.slice().reverse().join('   ·   ');
  }
 
  // ── CONN STATUS ─────────────────────────────────────────────────────────────
  function setConnected(ok) {
    const dot  = document.getElementById('conn-dot');
    const text = document.getElementById('conn-text');
    if (ok) {
      dot.classList.add('live');
      text.textContent = 'LIVE';
    } else {
      dot.classList.remove('live');
      text.textContent = 'DISCONNECTED';
    }
  }
 
  // ── CLOCK ──────────────────────────────────────────────────────────────────
  function tick() {
    document.getElementById('time').textContent =
      new Date().toLocaleTimeString('id-ID', { hour12: false });
  }
  tick();
  setInterval(tick, 1000);
 
  // ── UTILS ──────────────────────────────────────────────────────────────────
  function escHtml(str) {
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }
 
  // ── START ──────────────────────────────────────────────────────────────────
  connect();
 
}());
</script>
</body>
</html>`;
 
