# 911 Dispatch System: Logic Walkthrough

This document explains the integration between **gRPC Services** (Emergency handling) and the **WebSocket Dashboard** (Real-time monitoring).

## Architecture Overview

1.  **gRPC (server/index.js)**: Acts as the core backend, handling client/helper registration, emergency requests, and chat streams.
2.  **WebSocket (helper/web_socket.js)**: Acts as the "Broadcast Station". It doesn't process business logic but receives updates from gRPC and pushes them to the Dashboard clients.
3.  **Dashboard (dashboard/)**: A static web application that connects to the WebSocket server to display the current state of the system.

---

## 1. Flow: Helper Lifecycle

### Login
-   **gRPC**: A helper calls `WatchQueue`. The server validates the login and stores the helper in `loggedInHelpers`.
-   **Bridge**: Server calls `wsDashboard.onHelperLogin(agent_id, helperData)`.
-   **WebSocket**: `helperCards` is updated. Event `HELPER_LOGIN` is broadcasted.
-   **UI**: A new card appears in the "Active Helpers" panel.

### Logout
-   **gRPC**: The gRPC stream is cancelled or a logout is requested.
-   **Bridge**: Server calls `wsDashboard.onHelperLogout(agent_id)`.
-   **WebSocket**: Agent is removed from `helperCards`. Event `HELPER_LOGOUT` is broadcasted.
-   **UI**: The helper card is removed.

---

## 2. Flow: Emergency Request & Queueing

### New Request
-   **gRPC**: A client calls `RequestEmergency`. The server adds a record to the corresponding queue (FIRE/MEDICAL/POLICE).
-   **Bridge**: Server calls `wsDashboard.onQueueUpdate(queues)`.
-   **WebSocket**: `queueSnap` is updated. Event `QUEUE_UPDATE` (full snapshot) is broadcasted.
-   **UI**: The count in the "Queue Monitor" increases, and a new ticker item appears in the column.

---

## 3. Flow: Mission Assignment (The Handshake)

This is the most complex part where gRPC and WebSocket link a session to a helper.

1.  **Helper Action**: Helper calls `UpdateSessionStatus` with `status: 1` (ONGOING).
2.  **gRPC**: Server updates the session status in memory (`queues`).
3.  **Bridge**: Server calls `wsDashboard.onSessionAssigned(agent_id, session_id)`.
4.  **WebSocket**: 
    -   Maps `session_id` to `agent_id` in `sessionAgent`.
    -   Updates the helper's status to `MISI` in `helperCards`.
    -   Broadcasts `HELPER_STATUS_CHANGE`.
5.  **UI**: The helper card turns **Orange/Amber**, pulses, and gains a "PANTAU CHAT" clickable overlay.

---

## 4. Flow: Chat Monitoring

-   **Dashboard Action**: Admin clicks a helper card in `MISI` status.
-   **WebSocket**: Client sends `{ type: 'WATCH_CHAT', session_id: ... }`. WebSocket server adds this client to a `chatWatchers` Set for that session.
-   **gRPC Message**: In `chatStreamHandler`, whenever a message is broadcasted to participants:
    -   **Bridge**: Server calls `wsDashboard.onChatMessage(currentSessionId, msg)`.
    -   **WebSocket**: Checks if any dashboard is watching this `sessionId`. If yes, sends `CHAT_MESSAGE`.
-   **UI**: Message appears in the Chat Modal in real-time.

---

## 5. Flow: Mission Completion

-   **Helper/Admin Action**: Session status updated to `2` (DONE).
-   **Bridge**: Server calls `wsDashboard.onSessionEnded(session_id)`.
-   **WebSocket**:
    -   Clears mission status for the helper.
    -   Broadcasts `HELPER_STATUS_CHANGE` (back to `LUANG`).
    -   Broadcasts `CHAT_SESSION_ENDED` to any watchers.
-   **UI**: Helper card turns Green again. The chat modal shows a "Sesi Berakhir" notice.

---

## Directory Reference

-   `dashboard/index.html`: UI Structure.
-   `dashboard/style.css`: Visual "Dispatch" theme.
-   `dashboard/script.js`: Frontend logic (reconnect, packet handling, DOM rendering).
-   `helper/web_socket.js`: The WebSocket/HTTP server & event handlers.
-   `server/index.js`: The gRPC server that triggers these events.
