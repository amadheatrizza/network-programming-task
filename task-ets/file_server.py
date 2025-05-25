from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import logging
from file_protocol import FileProtocol
import concurrent.futures
import multiprocessing
import argparse
  
class ServerPool:
  def __init__(self, host='0.0.0.0', port=6667, pool_size=1, executor_type='thread'):
    self.protocol = FileProtocol()
    self.pool_size = pool_size
    self.executor_type = executor_type
    self.socket = self.create_socket(host, port)

  def create_socket(self, host, port):
      sock = socket(AF_INET, SOCK_STREAM)
      sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
      if self.executor_type == 'thread':
        sock.settimeout(1800)
      sock.bind((host, port))
      return sock
  
  def handle_client(self, conn, addr):
      logging.warning(f"Handling connection from {addr}")
      buffer = ""
      try:
          while data := conn.recv(128 * 1024 * 1024):
              buffer += data.decode()
              while "\r\n\r\n" in buffer:
                  command, buffer = buffer.split("\r\n\r\n", 1)
                  response = self.protocol.proses_string(command) + "\r\n\r\n"
                  conn.sendall(response.encode())
      except Exception as e:
          logging.warning(f"Connection error from {addr}: {str(e)}")
      finally:
          conn.close()
          logging.warning(f"Closed connection from {addr}")

  def run_server(self): 
    logging.warning(f"Server started on port {self.socket.getsockname()[1]} with {self.pool_size} pool size")
    
    listen_count = 5 if self.executor_type == 'thread' else 1
    self.socket.listen(listen_count)
 
    executor_class = concurrent.futures.ThreadPoolExecutor if self.executor_type == 'thread' else concurrent.futures.ProcessPoolExecutor
    
    with executor_class(max_workers=self.pool_size) as executor:
        try:
            while True:
                conn, addr = self.socket.accept()
                executor.submit(self.handle_client, conn, addr)
        except KeyboardInterrupt:
            logging.warning("Server shutdown initiated")
        finally:
            self.socket.close()

def setup_logging():
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def parse_args():
    parser = argparse.ArgumentParser(description='Threaded File Server')
    parser.add_argument('--port', type=int, default=6667)
    parser.add_argument('--pool-size', type=int, default=1)
    parser.add_argument('--executor', choices=['thread', 'process'], default='thread', 
                        help='Executor type (default: thread)')
    return parser.parse_args()

def main():
    args = parse_args()
    
    if args.executor == 'process':
      multiprocessing.freeze_support()
    
    server = ServerPool(port=args.port, pool_size=args.pool_size, executor_type=args.executor)
    server.run_server()

if __name__ == "__main__":
    main()
