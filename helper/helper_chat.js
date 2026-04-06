export default function startChat(helper, agentId, sessionId, rl, onExit) {
    console.log("=== CHAT HELPER (Ketik 'exit' untuk kembali) ===");
    const call = helper.ChatStream();

    const loop = () => {
        rl.question("Helper: ", (text) => {
            if (text.toLowerCase() === "exit") {
                console.log("Keluar chat...");
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
        process.stdout.clearLine(0);
        process.stdout.cursorTo(0);
        console.log(`[${msg.sender_id}] ${msg.content}`);

        if (msg.status === "done" || msg.status === "banned") {
            console.log("Session selesai dari Server.");
            call.end();
            if(onExit) onExit();
        } else {
            // Tampilkan ulang prompt karena ditimpa log
            process.stdout.write("Helper: ");
        }
    });

    loop();
};