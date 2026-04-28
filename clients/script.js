(function() {
  // ── STATE ──────────────────────────────────────────────────────────────────
  let clientId  = localStorage.getItem('911_client_id');
  let phoneNum  = localStorage.getItem('911_phone_num');
  let sessionId = null;
  let status    = 0; // 0: PENDING, 1: ONGOING, 2: DONE, 3: BANNED

  // ── WEBSOCKET ──────────────────────────────────────────────────────────────
  let ws;
  let reconnectTimer;

  function connect() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(protocol + '//' + location.host);

    ws.onopen = function() {
      setConnected(true);
      if (clientId) {
        // Re-register if already have ID
        send({ type: 'REGISTER', phone: phoneNum });
      }
    };

    ws.onclose = function() {
      setConnected(false);
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connect, 3000);
    };

    ws.onmessage = function(e) {
      try {
        const pkt = JSON.parse(e.data);
        handlePacket(pkt);
      } catch (_) {}
    };
  }

  function send(obj) {
    if (ws && ws.readyState === 1) {
      obj.role = 'client';
      ws.send(JSON.stringify(obj));
    }
  }

  // ── PACKET HANDLER ─────────────────────────────────────────────────────────
  function handlePacket(pkt) {
    switch (pkt.event) {
      case 'REGISTER_RES': {
        setLoading(false);
        const { err, res } = pkt.data;
        if (err || !res.success) {
          showError('reg', err ? err.message : res.message);
          return;
        }
        clientId = res.client.client_id;
        phoneNum = res.client.phone_num;
        localStorage.setItem('911_client_id', clientId);
        localStorage.setItem('911_phone_num', phoneNum);
        showView('view-request');
        document.getElementById('user-id').textContent = 'ID: ' + clientId;
        document.getElementById('user-phone').textContent = 'PH: ' + phoneNum;
        break;
      }

      case 'REQUEST_RES': {
        setLoading(false);
        const { err, res } = pkt.data;
        if (err) {
          showError('req', err.message);
          return;
        }
        if (res.status === 3) {
          showError('req', res.info);
          return;
        }
        sessionId = res.session_id;
        showView('view-status');
        document.getElementById('sh-sid').textContent = 'SESSION: ' + sessionId;
        updateStatus(0, res.info);
        break;
      }

      case 'STATUS_UPDATE': {
        const update = pkt.data;
        sessionId = update.session.session_id;
        updateStatus(update.session.status, update.notification);
        break;
      }

      case 'CHAT_MSG': {
        appendChatMsg(pkt.data);
        break;
      }

      case 'CANCEL_RES': {
        setLoading(false);
        const { err, res } = pkt.data;
        if (!err && res.success) {
          sessionId = null;
          showView('view-request');
        } else {
          alert('Gagal membatalkan: ' + (err ? err.message : res.message));
        }
        break;
      }
    }
  }

  // ── UI ACTIONS ─────────────────────────────────────────────────────────────
  document.getElementById('btn-register').addEventListener('click', function() {
    const phone = document.getElementById('reg-phone').value.trim();
    if (!phone) return showError('reg', 'Masukkan nomor telepon!');
    setLoading(true);
    send({ type: 'REGISTER', phone: phone });
  });

  document.getElementById('btn-request').addEventListener('click', function() {
    const msg = document.getElementById('req-msg').value.trim();
    if (!msg) return showError('req', 'Jelaskan kondisi darurat Anda!');
    setLoading(true);
    send({ type: 'REQUEST_EMERGENCY', message: msg });
  });

  document.getElementById('btn-cancel').addEventListener('click', function() {
    if (confirm('Batalkan permintaan bantuan ini?')) {
      setLoading(true);
      send({ type: 'CANCEL', session_id: sessionId });
    }
  });

  document.getElementById('btn-send').addEventListener('click', sendChat);
  document.getElementById('chat-input').addEventListener('keypress', function(e) {
    if (e.key === 'Enter') sendChat();
  });

  document.getElementById('btn-return').addEventListener('click', function() {
    sessionId = null;
    status = 0;
    document.getElementById('chat-messages').innerHTML = '';
    document.getElementById('req-msg').value = '';
    showView('view-request');
  });

  function sendChat() {
    const input = document.getElementById('chat-input');
    const msg = input.value.trim();
    if (!msg || !sessionId) return;
    send({ type: 'SEND_CHAT', session_id: sessionId, content: msg });
    input.value = '';
    // Optimistic append or wait for server message? The server broadcasts it back.
  }

  // ── UI UTILS ───────────────────────────────────────────────────────────────
  function showView(id) {
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    document.getElementById(id).classList.add('active');
  }

  function setLoading(on) {
    document.getElementById('overlay').className = on ? 'active' : '';
  }

  function showError(prefix, msg) {
    const el = document.getElementById(prefix + '-error');
    el.textContent = msg;
    setTimeout(() => { el.textContent = ''; }, 5000);
  }

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

  function updateStatus(code, info) {
    status = code;
    const badge = document.getElementById('sh-badge');
    const labels = ['PENDING', 'ONGOING', 'DONE', 'BANNED'];
    badge.textContent = labels[code] || 'UNKNOWN';
    badge.className = 'status-badge s' + (code === 1 ? '1' : '0');
    
    document.getElementById('status-notification').textContent = info;

    const chatBox = document.getElementById('chat-box');
    const input = document.getElementById('chat-input');
    const btn   = document.getElementById('btn-send');
    const btnCancel = document.getElementById('btn-cancel');
    const btnReturn = document.getElementById('btn-return');

    if (code === 1) {
      chatBox.classList.remove('chat-disabled');
      input.disabled = false;
      btn.disabled   = false;
      btnCancel.style.display = 'none';
      btnReturn.style.display = 'none';
    } else {
      chatBox.classList.add('chat-disabled');
      input.disabled = true;
      btn.disabled   = true;
      
      if (code === 0) {
        btnCancel.style.display = 'block';
        btnReturn.style.display = 'none';
      } else if (code === 2 || code === 3) {
        btnCancel.style.display = 'none';
        btnReturn.style.display = 'block';
      }
    }
  }

  function appendChatMsg(msg) {
    const container = document.getElementById('chat-messages');
    const div = document.createElement('div');
    
    const isYou = msg.sender_id === clientId;
    const isSys = msg.sender_id === 'SYSTEM';
    
    div.className = 'msg ' + (isSys ? 'sys' : (isYou ? 'you' : 'helper'));
    
    let content = '';
    if (!isSys) {
      content += '<div class="msg-sender">' + (isYou ? 'ANDA' : 'PETUGAS') + '</div>';
    }
    content += '<div class="msg-bubble">' + escHtml(msg.content) + '</div>';
    
    div.innerHTML = content;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
    
    if (msg.status === 'done' || msg.status === 'banned') {
      // Sesi ditutup
    }
  }

  function escHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  // ── START ──────────────────────────────────────────────────────────────────
  connect();

})();
