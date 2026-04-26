(function () {
  // ── STATE ──────────────────────────────────────────────────────────────────
  const helpers   = new Map();   // agentId → helper object
  const queues    = { FIRE: [], MEDICAL: [], POLICE: [] };
  let helperList  = [];
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
        console.log('[INIT] helpers count:', (data.helpers||[]).length);
        // Full Sync
        helpers.clear();
        (data.helpers || []).forEach(function (h) { 
          const id = (h.agent_id || '').trim();
          h.agent_id = id;
          helpers.set(id, h); 
        });
        Object.assign(queues, data.queues || {});
        renderAll();
        pushLog('State awal diterima — ' + helpers.size + ' helper aktif');
        break;
      }
 
      case 'HELPER_LOGIN': {
        const h = pkt.data.helper;
        const id = (h.agent_id || '').trim();
        h.agent_id = id;
        helpers.set(id, h);
        renderHelpers();
        updateStats();
        pushLog('[+] Helper login: ' + h.name + ' (' + h.department + ')');
        break;
      }
 
      case 'HELPER_LOGOUT': {
        const id = (pkt.data.agent_id || '').trim();
        const name = helpers.has(id) ? helpers.get(id).name : id;
        helpers.delete(id);
        renderHelpers();
        updateStats();
        pushLog('[-] Helper logout: ' + name);
        break;
      }
 
      case 'HELPER_STATUS_CHANGE': {
        let { agent_id, status, session_id } = pkt.data;
        agent_id = (agent_id || '').trim();
        console.log('[STATUS_CHANGE]', agent_id, status);
        
        if (!helpers.has(agent_id)) {
          console.warn('[STATUS_CHANGE] Agent missing from local Map:', agent_id, '. Fetching full state...');
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
        console.log('[QUEUE_UPDATE] helpers in pkt:', (pkt.data.helpers || []).length);
        if (pkt.data.helpers) {
          // Normalize remote IDs
          const remoteHelpers = (pkt.data.helpers || []).map(function(h) {
             const id = (h.agent_id || '').trim();
             h.agent_id = id;
             return h;
          });
          const remoteIds = remoteHelpers.map(function(h) { return h.agent_id; });

          // Sync helpers Map: Remove those not in snapshot
          for (let id of helpers.keys()) {
            if (!remoteIds.includes(id)) {
              console.log('[SYNC] Deleting missing helper:', id);
              helpers.delete(id);
            }
          }
          // Update/Add from snapshot
          remoteHelpers.forEach(function(h) { 
            helpers.set(h.agent_id, h); 
          });
        }
        Object.assign(queues, pkt.data.queues || {});
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
    try {
      const list  = document.getElementById('helpers-list');
      const empty = document.getElementById('helpers-empty');
      if (!list || !empty) return;

      const hArr  = [...helpers.values()];
      
      // Debugging state
      console.log('[RENDER] Current Helpers Map:', hArr.length);
      if (hArr.length > 0) console.table(hArr);

      // Clear the actual list only
      while (list.firstChild) list.removeChild(list.firstChild);

      if (hArr.length === 0) {
        empty.style.display = 'flex';
        document.getElementById('helper-badge').textContent = '0';
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
    } catch (err) {
      console.error('[RENDER_ERROR] Failed to render helpers:', err);
    }
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
