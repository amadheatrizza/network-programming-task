from socket import *
import socket
import threading
import logging
from datetime import datetime

class ProcessTheClient(threading.Thread):
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        threading.Thread.__init__(self)

    def run(self):
        logging.warning(f"Client connected: {self.address}")
        try:
            while True:
                data = self.connection.recv(32)
                if not data:
                    break

                message = data.decode('utf-8').strip()

                if message == "TIME":
                    now = datetime.now()
                    jam = now.strftime("JAM %H:%M:%S\r\n")
                    self.connection.sendall(jam.encode('utf-8'))

                elif message == "QUIT":
                    logging.warning(f"Client {self.address} requested QUIT")
                    break
                else:
                    # Bisa abaikan request yang tidak dikenal
                    pass
        except Exception as e:
            logging.error(f"Error handling client {self.address}: {e}")
        finally:
            self.connection.close()
            logging.warning(f"Connection closed: {self.address}")

class Server(threading.Thread):
    def __init__(self):
        self.the_clients = []
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        threading.Thread.__init__(self)

    def run(self):
        self.my_socket.bind(('0.0.0.0', 45000))  # Port diubah ke 45000
        self.my_socket.listen(5)
        logging.warning("Server started on port 45000")

        try:
            while True:
                self.connection, self.client_address = self.my_socket.accept()
                logging.warning(f"Connection from {self.client_address}")

                clt = ProcessTheClient(self.connection, self.client_address)
                clt.start()
                self.the_clients.append(clt)
        except KeyboardInterrupt:
            logging.warning("Server shutting down.")
        finally:
            self.my_socket.close()

def main():
    logging.basicConfig(level=logging.WARNING)
    svr = Server()
    svr.start()

if __name__ == "__main__":
    main()
