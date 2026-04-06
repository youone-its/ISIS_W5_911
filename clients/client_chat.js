export default function startChat(client, clientId, sessionId, rl, onExit) {
    console.log("=== CHAT USER (Ketik 'exit' untuk kembali) ===");
    const call = client.ChatStream();

    const loop = () => {
        rl.question("You: ", (text) => {
            if (text.toLowerCase() === "exit") {
                console.log("Keluar chat...");
                call.end();
                if(onExit) onExit();
                return;
            }
            call.write({
                sender_id: clientId,
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
            process.stdout.write("You: ");
        }
    });

    loop();
};