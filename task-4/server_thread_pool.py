import socketserver
from httpserver import FileHandler

HOST = "127.0.0.1"
PORT = 9977
file_handler = FileHandler()

class ConnectionHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            raw = self.request.recv(65535).strip()
            if not raw:
                return
            
            print(f":: Thread-{self.request.fileno()}: New request")
            response = file_handler.process(raw)
            self.request.sendall(response)
        except Exception as e:
            print(f"!! Thread error: {e}")

class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

def run():
    print(f":: Starting on {HOST}:{PORT}")
    with ThreadedServer((HOST, PORT), ConnectionHandler) as s:
        try:
            s.serve_forever()
        except KeyboardInterrupt:
            print("\n!! Shutting down")

if __name__ == "__main__":
    run()
