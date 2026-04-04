// helper/terminal.js
'use strict';

const readline = require('readline');
const { login, watchQueue, assignClient, chatStream, endSession } = require('./index');

// ─── Terminal helpers ────────────────────────────────────────────────────────
const C = {
  reset:  '\x1b[0m',
  bold:   '\x1b[1m',
  red:    '\x1b[31m',
  green:  '\x1b[32m',
  yellow: '\x1b[33m',
  blue:   '\x1b[34m',
  magenta:'\x1b[35m',
  cyan:   '\x1b[36m',
  gray:   '\x1b[90m',
};

const fmt  = (color, msg) => `${color}${msg}${C.reset}`;
const info = (msg) => console.log(`${C.cyan}[INFO]${C.reset} ${msg}`);
const ok   = (msg) => console.log(`${C.green}[✓]${C.reset} ${msg}`);
const err  = (msg) => console.log(`${C.red}[✗]${C.reset} ${msg}`);
const warn = (msg) => console.log(`${C.yellow}[!]${C.reset} ${msg}`);
const sep  = ()    => console.log(fmt(C.gray, '─'.repeat(44)));

const rl  = readline.createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise(resolve => rl.question(q, resolve));

// ─── App state ───────────────────────────────────────────────────────────────
const state = {
  agent_id:   null,
  name:       null,
  department: null,
  token:      null,
  session_id: null,   // currently active / assigned session
};

const DEPARTMENTS = ['FIRE', 'MEDICAL', 'POLICE'];

// ─── Banner ──────────────────────────────────────────────────────────────────
function drawBanner() {
  console.clear();
  console.log(fmt(C.blue, '╔══════════════════════════════════════════╗'));
  console.log(fmt(C.blue, '║') + fmt(C.bold, '       🚑  EMERGENCY CALL CENTER         ') + fmt(C.blue, '║'));
  console.log(fmt(C.blue, '║') + fmt(C.cyan, '            HELPER TERMINAL              ') + fmt(C.blue, '║'));
  console.log(fmt(C.blue, '╚══════════════════════════════════════════╝'));
  console.log('');
}

function printStatus() {
  if (state.agent_id) {
    info(`Agent  : ${fmt(C.yellow, state.name)} (${fmt(C.yellow, state.agent_id)})`);
    info(`Dept   : ${fmt(C.magenta, state.department)}`);
  }
  if (state.session_id)
    info(`Session: ${fmt(C.green, state.session_id.slice(0, 8) + '...')}`);
}

// ─── 1. Login ────────────────────────────────────────────────────────────────
async function doLogin() {
  sep();
  console.log(fmt(C.bold, ' HELPER LOGIN'));
  sep();
  console.log(' Pre-seeded agents:');
  console.log('   agent001 / pass123  →  FIRE');
  console.log('   agent002 / pass123  →  MEDICAL');
  console.log('   agent003 / pass123  →  POLICE');
  console.log('');

  const agentId  = (await ask(' Agent ID   : ')).trim();
  const password = (await ask(' Password   : ')).trim();

  console.log(' Departments: 1=FIRE  2=MEDICAL  3=POLICE');
  const deptChoice = (await ask(' Department : ')).trim();
  const dept = DEPARTMENTS[parseInt(deptChoice, 10) - 1] || deptChoice.toUpperCase();

  try {
    const res = await login(agentId, password, dept);
    if (res.success) {
      state.agent_id   = res.agent.agent_id;
      state.name       = res.agent.name;
      state.department = res.agent.department;
      state.token      = res.token;
      ok(`Welcome, ${fmt(C.yellow, state.name)}! Department: ${fmt(C.magenta, state.department)}`);
    } else {
      err(res.message);
    }
  } catch (e) {
    err(`Connection error: ${e.message}`);
  }
}

// ─── 2. Watch Queue ──────────────────────────────────────────────────────────
async function doWatchQueue() {
  if (!state.agent_id) return warn('Please login first (option 1).');
  sep();
  console.log(fmt(C.bold, ` QUEUE — ${state.department}`));
  sep();
  info('Incoming sessions will appear below. Press Enter to return.\n');

  const stream = watchQueue(state.agent_id, state.department);

  stream.on('data', update => {
    console.log('');
    console.log(fmt(C.blue, `── Queue Update [${update.event || 'UPDATE'}] ${'─'.repeat(18)}`));

    if (update.new_session?.session_id) {
      const s = update.new_session;
      console.log(fmt(C.green, `  🆕 New session:`));
      console.log(`     ID      : ${fmt(C.yellow, s.session_id)}`);
      console.log(`     Client  : ${fmt(C.cyan, s.client?.phone_num || s.client?.client_id)}`);
      console.log(`     Message : "${s.initial_message}"`);
    }

    if (Array.isArray(update.PENDING) && update.PENDING.length > 0) {
      console.log(fmt(C.yellow, `  📋 Pending (${update.PENDING.length}):`));
      update.PENDING.forEach((s, i) => {
        console.log(`     ${i + 1}. ${s.session_id.slice(0, 8)}... — "${s.initial_message?.slice(0, 40)}"`);
      });
    } else {
      console.log('  (no pending sessions)');
    }

    console.log(fmt(C.blue, '─'.repeat(44)));
  });

  stream.on('error', e => err(`Stream error: ${e.message}`));
  stream.on('end',   () => info('Queue stream closed.'));

  await ask('');
  stream.cancel();
}

// ─── 3. Assign Client ────────────────────────────────────────────────────────
async function doAssignClient() {
  if (!state.agent_id) return warn('Please login first.');
  sep();
  console.log(fmt(C.bold, ' ASSIGN SESSION'));
  sep();

  const sessionId = (await ask(' Session ID (full or partial): ')).trim();
  if (!sessionId) return warn('Session ID cannot be empty.');

  try {
    const res = await assignClient(state.agent_id, sessionId);
    if (res.success) {
      state.session_id = res.session.session_id;
      ok(`Assigned to session ${fmt(C.yellow, state.session_id)}`);
      info(`Client  : ${res.session.client?.phone_num}`);
      info(`Status  : ${res.session.status}`);
      info(`Message : "${res.session.initial_message}"`);
    } else {
      err(res.message);
    }
  } catch (e) {
    err(`Failed: ${e.message}`);
  }
}

// ─── 4. Chat ─────────────────────────────────────────────────────────────────
async function doChat() {
  if (!state.agent_id)   return warn('Please login first.');
  if (!state.session_id) return warn('No assigned session — assign a client first (option 3).');
  sep();
  console.log(fmt(C.bold, ' LIVE CHAT WITH CLIENT'));
  sep();
  info(`Session: ${state.session_id}`);
  info('Type a message and press Enter.  Type "exit" to leave.\n');

  const stream = chatStream();

  stream.on('data', msg => {
    if (msg.sender_role !== 'HELPER' && msg.content !== '__CONNECT__') {
      process.stdout.clearLine?.(0);
      process.stdout.cursorTo?.(0);
      console.log(`${fmt(C.red, '[Client]')} ${msg.content}`);
      process.stdout.write('You: ');
    }
  });
  stream.on('error', e => err(`Chat error: ${e.message}`));
  stream.on('end',   () => info('\nChat stream ended.'));

  // Announce ourselves
  stream.write({
    session_id:  state.session_id,
    sender_id:   state.agent_id,
    sender_role: 'HELPER',
    content:     '__CONNECT__',
    timestamp:   Date.now().toString(),
  });

  while (true) {
    const line = (await ask('You: ')).trim();
    if (line.toLowerCase() === 'exit') break;
    if (!line) continue;
    stream.write({
      session_id:  state.session_id,
      sender_id:   state.agent_id,
      sender_role: 'HELPER',
      content:     line,
      timestamp:   Date.now().toString(),
    });
  }
  stream.end();
  info('Left the chat.');
}

// ─── 5. End Session ──────────────────────────────────────────────────────────
async function doEndSession() {
  if (!state.agent_id)   return warn('Please login first.');
  if (!state.session_id) return warn('No assigned session — assign a client first (option 3).');
  sep();
  console.log(fmt(C.bold, ' END SESSION'));
  sep();
  info(`Session: ${state.session_id}`);

  console.log(' Reason:  1=DONE  2=BAN client');
  const reasonChoice = (await ask(' Reason: ')).trim();
  const reason = reasonChoice === '2' ? 'BANNED' : 'DONE';

  const note = (await ask(' Note (optional): ')).trim();

  if (reason === 'BANNED') {
    const confirm = (await ask(fmt(C.red, ' ⚠  This will ban the client. Confirm? (yes/no): '))).trim();
    if (confirm.toLowerCase() !== 'yes') {
      warn('Ban cancelled.');
      return;
    }
  }

  try {
    const res = await endSession(state.agent_id, state.session_id, reason, note);
    if (res.success) {
      ok(`Session closed. Final status: ${fmt(C.yellow, res.final_status)}`);
      state.session_id = null;
    } else {
      err(res.message);
    }
  } catch (e) {
    err(`Failed: ${e.message}`);
  }
}

// ─── Main menu ───────────────────────────────────────────────────────────────
async function menu() {
  while (true) {
    drawBanner();
    printStatus();
    console.log('');
    console.log(fmt(C.bold, ' MENU'));
    sep();

    if (!state.agent_id) {
      console.log('  1.  Login');
    } else {
      console.log('  1.  Login (re-login)');
      console.log('  2.  Watch Queue');
      console.log('  3.  Assign Session');
      console.log('  4.  Chat with Client');
      console.log('  5.  End Session');
    }
    console.log('  6.  Exit');
    sep();

    const choice = (await ask(' Choice: ')).trim();
    console.log('');

    switch (choice) {
      case '1': await doLogin();       break;
      case '2': await doWatchQueue();  break;
      case '3': await doAssignClient(); break;
      case '4': await doChat();        break;
      case '5': await doEndSession();  break;
      case '6':
        info('Goodbye!');
        rl.close();
        process.exit(0);
      default:
        warn('Invalid choice.');
    }

    await ask('\nPress Enter to continue...');
  }
}

// ─── Entry point ─────────────────────────────────────────────────────────────
menu().catch(e => {
  err(`Fatal error: ${e.message}`);
  rl.close();
  process.exit(1);
});