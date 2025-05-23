# file_server.py (ProcessPoolExecutor version)
from socket import *
import socket
import logging
import json
from concurrent.futures import ProcessPoolExecutor

from file_protocol import FileProtocol

def process_request(string_data):
    fp = FileProtocol()  # Harus dibuat ulang di tiap proses
    return fp.proses_string(string_data)

def main():
    ip = '0.0.0.0'
    port = 7777
    max_workers = 5
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((ip, port))
    server_socket.listen(5)

    logging.warning(f"Server berjalan di {ip}:{port} dengan ProcessPoolExecutor")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        while True:
            connection, address = server_socket.accept()
            logging.warning(f"Connection from {address}")

            buffer = ""
            while True:
                data = connection.recv(1024)
                if not data:
                    break
                buffer += data.decode()
                if "\r\n\r\n" in buffer:
                    request, buffer = buffer.split("\r\n\r\n", 1)
                    break

            future = executor.submit(process_request, request)
            hasil = future.result()
            hasil = hasil + "\r\n\r\n"
            connection.sendall(hasil.encode())
            connection.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()