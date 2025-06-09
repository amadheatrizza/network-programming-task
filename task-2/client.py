import socket

def send_request(sock, command):
    if command == "TIME":
        sock.sendall(b"TIME\r\n")
        reply = sock.recv(32).decode('utf-8')
        print("RECEIVED: ", reply.strip())
        print()
    elif command == "QUIT":
        sock.sendall(b"QUIT\r\n")
        print("Connection closed by client.")
        return False
    else:
        print("Command not recognized. Use TIME or QUIT.")
    return True

def run_client():
    address = ('127.0.0.1', 45000)
    try:
        with socket.create_connection(address) as sock:
            print("Connected to server.\n")
            print("Enter command (TIME or QUIT):")
            active = True
            while active:
                user_input = input("Command > ").strip().upper()
                active = send_request(sock, user_input)
    except ConnectionRefusedError:
        print("Failed to connect. Is the server running?")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    run_client()
