import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import threading
import time

import clients_pb2
import clients_pb2_grpc
import common_pb2

def listen_chat(stub, client_id, session_id):
    def request_generator():
        while True:
            msg = input("You: ")
            yield common_pb2.ChatMessage(
                sender_id=client_id,
                session_id=session_id,
                content=msg
            )

    responses = stub.ChatStream(request_generator())

    for res in responses:
        print(f"\n[{res.sender_id}] {res.content}")

def main():
    channel = grpc.insecure_channel('localhost:50051')
    stub = clients_pb2_grpc.ClientServiceStub(channel)

    phone = input("Masukkan nomor HP: ")

    # Register
    res = stub.Register(clients_pb2.RegisterRequest(phone_num=phone))
    client_id = res.client.id
    print("Registered:", client_id)

    # Request Emergency
    msg = input("Pesan darurat: ")
    emer = stub.RequestEmergency(
        clients_pb2.EmergencyRequest(
            client_id=client_id,
            initial_message=msg
        )
    )

    session_id = emer.session_id
    print("Session ID:", session_id)

    print("=== CHAT START ===")
    listen_chat(stub, client_id, session_id)

if __name__ == "__main__":
    main()