// clients/terminal.js
'use strict';

const readline = require('readline');
const { register, requestEmergency, watchSessionStatus, chatStream } = require('./index');

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

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise(resolve => rl.question(q, resolve));

// ─── App state ───────────────────────────────────────────────────────────────
const state = {
  client_id:  null,
  phone_num:  null,
  session_id: null,
};

// ─── Screens ─────────────────────────────────────────────────────────────────
function drawBanner() {
  console.clear();
  console.log(fmt(C.red, '╔══════════════════════════════════════════╗'));
  console.log(fmt(C.red, '║') + fmt(C.bold, '       🚨  EMERGENCY CALL CENTER         ') + fmt(C.red, '║'));
  console.log(fmt(C.red, '║') + fmt(C.cyan, '            CLIENT TERMINAL              ') + fmt(C.red, '║'));
  console.log(fmt(C.red, '╚══════════════════════════════════════════╝'));
  console.log('');
}

function printStatus() {
  if (state.client_id) {
    info(`Phone    : ${fmt(C.yellow, state.phone_num)}`);
    info(`Client ID: ${fmt(C.yellow, state.client_id.slice(0, 8) + '...')}`);
  }
  if (state.session_id) {
    info(`Session  : ${fmt(C.magenta, state.session_id.slice(0, 8) + '...')}`);
  }
}

// ─── 1. Register ─────────────────────────────────────────────────────────────
async function doRegister() {
  sep();
  console.log(fmt(C.bold, ' REGISTER NEW CLIENT'));
  sep();
  const phone = (await ask(' Phone number : ')).trim();
  if (!phone) return warn('Phone number cannot be empty.');

  try {
    const res = await register(phone);
    if (res.success) {
      state.client_id = res.client.client_id;
      state.phone_num = res.client.phone_num;
      ok(`Registered!  ID: ${fmt(C.yellow, state.client_id)}`);
    } else {
      err(res.message);
    }
  } catch (e) {
    err(`Connection error: ${e.message}`);
  }
}

// ─── 2. Request Emergency ────────────────────────────────────────────────────
async function doRequestEmergency() {
  if (!state.client_id) return warn('Please register first (option 1).');
  sep();
  console.log(fmt(C.bold, ' REQUEST EMERGENCY'));
  sep();
  console.log(' Auto-routing keywords:');
  console.log('   🔥  fire  / burn  / smoke');
  console.log('   🏥  medical / ambulance / hurt / injured');
  console.log('   👮  police / crime / robbery / attack');
  console.log('');

  const msg = (await ask(' Describe your emergency: ')).trim();
  if (!msg) return warn('Message cannot be empty.');

  try {
    const res = await requestEmergency(state.client_id, msg);
    state.session_id = res.session_id;

    ok(`Emergency submitted!`);
    info(`Session ID : ${fmt(C.yellow, res.session_id)}`);
    info(`Routed to  : ${fmt(C.magenta, res.routed_to)}`);
    info(`Status     : ${fmt(C.cyan, res.status)}`);
  } catch (e) {
    err(`Failed: ${e.message}`);
  }
}

// ─── 3. Watch Session ────────────────────────────────────────────────────────
async function doWatchSession() {
  if (!state.client_id)  return warn('Please register first.');
  if (!state.session_id) return warn('No active session — request an emergency first (option 2).');
  sep();
  console.log(fmt(C.bold, ' WATCHING SESSION STATUS'));
  sep();
  info(`Session: ${state.session_id}`);
  info('Updates will appear below. Press Enter to stop.\n');

  const stream = watchSessionStatus(state.client_id, state.session_id);

  stream.on('data', upd => {
    console.log('');
    console.log(fmt(C.cyan, '── Session Update ──────────────────────────'));
    if (upd.session) {
      const s = upd.session;
      console.log(`  Status : ${fmt(C.yellow, s.status)}`);
      console.log(`  Type   : ${fmt(C.magenta, s.type)}`);
      if (s.assigned_agent?.agent_id)
        console.log(`  Agent  : ${fmt(C.green, `${s.assigned_agent.name} (${s.assigned_agent.agent_id})`)}`);
    }
    if (upd.notification)
      console.log(`  Notice : ${upd.notification}`);
    console.log(fmt(C.cyan, '────────────────────────────────────────────'));
  });

  stream.on('error', e => err(`Stream error: ${e.message}`));
  stream.on('end',   () => info('Session stream closed by server.'));

  await ask('');
  stream.cancel();
}

// ─── 4. Chat ─────────────────────────────────────────────────────────────────
async function doChat() {
  if (!state.client_id)  return warn('Please register first.');
  if (!state.session_id) return warn('No active session — request an emergency first (option 2).');
  sep();
  console.log(fmt(C.bold, ' LIVE CHAT WITH HELPER'));
  sep();
  info(`Session: ${state.session_id}`);
  info('Type a message and press Enter.  Type "exit" to leave.\n');

  const stream = chatStream();

  stream.on('data', msg => {
    if (msg.sender_role !== 'CLIENT' && msg.content !== '__CONNECT__') {
      // Clear the prompt line and print the incoming message above it
      process.stdout.clearLine?.(0);
      process.stdout.cursorTo?.(0);
      console.log(`${fmt(C.blue, '[Helper]')} ${msg.content}`);
      process.stdout.write('You: ');
    }
  });
  stream.on('error', e => err(`Chat error: ${e.message}`));
  stream.on('end',   () => info('\nChat stream ended.'));

  // Announce ourselves so the server maps role → stream
  stream.write({
    session_id:  state.session_id,
    sender_id:   state.client_id,
    sender_role: 'CLIENT',
    content:     '__CONNECT__',
    timestamp:   Date.now().toString(),
  });

  // Message loop
  while (true) {
    const line = (await ask('You: ')).trim();
    if (line.toLowerCase() === 'exit') break;
    if (!line) continue;
    stream.write({
      session_id:  state.session_id,
      sender_id:   state.client_id,
      sender_role: 'CLIENT',
      content:     line,
      timestamp:   Date.now().toString(),
    });
  }
  stream.end();
  info('Left the chat.');
}

// ─── Main menu ───────────────────────────────────────────────────────────────
async function menu() {
  while (true) {
    drawBanner();
    printStatus();
    console.log('');
    console.log(fmt(C.bold, ' MENU'));
    sep();
    console.log('  1.  Register');
    console.log('  2.  Request Emergency');
    console.log('  3.  Watch Session Status');
    console.log('  4.  Chat with Helper');
    console.log('  5.  Exit');
    sep();

    const choice = (await ask(' Choice: ')).trim();
    console.log('');

    switch (choice) {
      case '1': await doRegister();         break;
      case '2': await doRequestEmergency(); break;
      case '3': await doWatchSession();     break;
      case '4': await doChat();             break;
      case '5':
        info('Goodbye. Stay safe!');
        rl.close();
        process.exit(0);
      default:
        warn('Invalid choice — please enter 1–5.');
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