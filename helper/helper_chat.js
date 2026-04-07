export default function startChat(helper, agentId, sessionId, rl, onExit) {
    console.log("\x1b[35m=== CHAT HELPER (Ketik 'exit' untuk kembali) ===\x1b[0m");
    const call = helper.ChatStream();
    let isChatActive = true;

    // Register stream logic
    call.write({
        sender_id: agentId,
        session_id: sessionId,
        content: ""
    });

    const loop = () => {
        rl.question("\x1b[36mHelper:\x1b[0m ", (text) => {
            if (!isChatActive) {
                if(onExit) onExit();
                return;
            }

            // Hapus baris input sebelumnya agar tidak double di terminal
            process.stdout.moveCursor(0, -1);
            process.stdout.clearLine(0);

            if (text.toLowerCase() === "exit") {
                console.log("\x1b[33mKeluar chat...\x1b[0m");
                isChatActive = false;
                call.end();
                if(onExit) onExit();
                return;
            }
            call.write({
                sender_id: agentId,
                session_id: sessionId,
                content: text,
            });
            loop();
        });
    };

    call.on("data", (msg) => {
        if (!msg.content && !msg.status) return;

        process.stdout.clearLine(0);
        process.stdout.cursorTo(0);
        
        let formattedMsg;
        if (msg.sender_id === agentId) {
            formattedMsg = `\x1b[36m[You]\x1b[0m \x1b[97m${msg.content}\x1b[0m`;
        } else if (msg.sender_id.startsWith("H")) {
            formattedMsg = `\x1b[36m[Helper]\x1b[0m \x1b[97m${msg.content}\x1b[0m`;
        } else if (msg.sender_id === "SYSTEM") {
            formattedMsg = `\x1b[33m[SYSTEM]\x1b[0m \x1b[93m${msg.content}\x1b[0m`;
        } else {
            formattedMsg = `\x1b[32m[Client]\x1b[0m \x1b[97m${msg.content}\x1b[0m`;
        }
        
        if (msg.content) {
            console.log(formattedMsg);
        }

        if (msg.status === "done" || msg.status === "banned") {
            console.log("\x1b[33mSession selesai dari Server.\x1b[0m");
            isChatActive = false;
            call.end();
            // Force exit the blocked prompt
            rl.write(null, {ctrl: true, name: 'u'});
            rl.write('\n');
        } else {
            // Tampilkan ulang prompt karena ditimpa log
            if (isChatActive) {
                process.stdout.write("\x1b[36mHelper:\x1b[0m ");
            }
        }
    });

    loop();
};