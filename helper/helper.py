import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import threading

import helper_pb2
import helper_pb2_grpc
import common_pb2

def listen_chat(stub, agent_id, session_id):
    def request_generator():
        while True:
            msg = input("Helper: ")
            yield common_pb2.ChatMessage(
                sender_id=agent_id,
                session_id=session_id,
                content=msg
            )

    responses = stub.ChatStream(request_generator())

    for res in responses:
        print(f"\n[{res.sender_id}] {res.content}")

def main():
    channel = grpc.insecure_channel('localhost:50051')
    stub = helper_pb2_grpc.HelperServiceStub(channel)

    agent_id = input("Agent ID: ")
    password = input("Password: ")

    # Login
    res = stub.Login(
        helper_pb2.HelperLoginRequest(
            agent_id=agent_id,
            password=password
        )
    )

    if not res.success:
        print("Login gagal")
        return

    print("Login berhasil!")

    # Watch Queue
    print("Menunggu request...")
    queue = stub.WatchQueue(
        helper_pb2.WatchQueueRequest(agent_id=agent_id)
    )

    for update in queue:
        if update.new_session.session_id:
            session_id = update.new_session.session_id
            print("Ada request baru:", session_id)

            # Assign
            assign = stub.AssignClient(
                helper_pb2.AssignRequest(
                    agent_id=agent_id,
                    session_id=session_id
                )
            )

            if assign.success:
                print("Ambil session:", session_id)
                print("=== CHAT START ===")
                listen_chat(stub, agent_id, session_id)
                break

if __name__ == "__main__":
    main()