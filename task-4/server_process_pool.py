import os
import socket
from multiprocessing import Pool
from httpserver import FileHandler

HOST = "127.0.0.1"
PORT = 9977
WORKERS = 4
file_handler = FileHandler()

def process_request(connection):
    try:
        raw = connection.recv(65535).strip()
        if not raw:
            return
            
        pid = os.getpid()
        print(f":: Process-{pid}: Handling request")
        response = file_handler.process(raw)
        connection.sendall(response)
    except Exception as e:
        print(f"!! Process-{pid} error: {e}")
    finally:
        connection.close()

def run_server():
    print(f":: Starting on {HOST}:{PORT} with {WORKERS} workers")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((HOST, PORT))
        sock.listen(5)
        
        with Pool(processes=WORKERS) as workers:
            print(f"++ Server ready")
            try:
                while True:
                    conn, _ = sock.accept()
                    workers.apply_async(process_request, (conn,))
            except KeyboardInterrupt:
                print("\n!! Shutting down workers...")
            finally:
                sock.close()

if __name__ == "__main__":
    run_server()
