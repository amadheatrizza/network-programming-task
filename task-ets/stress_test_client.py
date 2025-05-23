import os
import time
import base64
import socket
import json
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from tabulate import tabulate 

server_address = ('172.16.16.101', 6666)  # Sesuaikan IP server

def send_command(command_dict):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    try:
        message = json.dumps(command_dict) + "\r\n\r\n"
        # Print hanya info ringkas
        if command_dict['cmd'] == 'upload':
            print(f"[UPLOAD] File: {command_dict['params'][0]}")
        else:
            print(f"[SEND] Command: {command_dict['cmd']} - Params: {command_dict['params']}")
        sock.sendall(message.encode())

        data_received = ""
        while True:
            data = sock.recv(1024)
            if data:
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    break
            else:
                break

        hasil = json.loads(data_received.strip().replace("\r\n\r\n", ""))
        return hasil
    finally:
        sock.close()

def upload_file(filepath):
    try:
        with open(filepath, 'rb') as f:
            file_content = base64.b64encode(f.read()).decode()
        filename = os.path.basename(filepath)
        command = {
            "cmd": "upload",
            "params": [filename, file_content]
        }
        hasil = send_command(command)
        if hasil['status'] == 'OK':
            return True, filename
        else:
            return False, hasil['data']
    except Exception as e:
        return False, str(e)

def download_file(filename):
    try:
        command = {
            "cmd": "get",
            "params": [filename]
        }
        hasil = send_command(command)
        if hasil['status'] == 'OK':
            return True, filename
        else:
            return False, hasil['data']
    except Exception as e:
        return False, str(e)

def client_worker(file_to_upload, file_to_download):
    start_time = time.time()
    upload_status, upload_msg = upload_file(file_to_upload)
    download_status, download_msg = download_file(file_to_download)
    end_time = time.time()
    duration = end_time - start_time

    total_bytes = os.path.getsize(file_to_upload) + os.path.getsize(file_to_download)
    throughput = total_bytes / duration if duration > 0 else 0

    return {
        "upload_status": upload_status,
        "upload_msg": upload_msg,
        "download_status": download_status,
        "download_msg": download_msg,
        "duration": duration,
        "throughput": throughput
    }

def stress_test(pool_type='thread', workers=5, file_upload='file10MB.bin', file_download='file10MB.bin'):
    results = []
    if pool_type == 'thread':
        executor_class = ThreadPoolExecutor
    elif pool_type == 'process':
        executor_class = ProcessPoolExecutor
    else:
        raise ValueError("pool_type must be 'thread' or 'process'")

    with executor_class(max_workers=workers) as executor:
        futures = [executor.submit(client_worker, file_upload, file_download) for _ in range(workers)]
        for future in as_completed(futures):
            results.append(future.result())

    sukses_client = sum(1 for r in results if r['upload_status'] and r['download_status'])
    gagal_client = workers - sukses_client
    total_duration = sum(r['duration'] for r in results)
    avg_throughput = sum(r['throughput'] for r in results) / workers

    print("\n===== STRESS TEST SUMMARY =====")
    print(f"Pool Type      : {pool_type}")
    print(f"Total Clients  : {workers}")
    print(f"Success        : {sukses_client}")
    print(f"Failed         : {gagal_client}")
    print(f"Avg Duration   : {total_duration / workers:.2f} sec")
    print(f"Avg Throughput : {avg_throughput / (1024 * 1024):.2f} MB/s")

    # Tampilkan tabel detail
    headers = ["#", "Upload", "Download", "Duration (s)", "Throughput (MB/s)"]
    table = []
    for idx, res in enumerate(results, 1):
        table.append([
            idx,
            "OK" if res['upload_status'] else "FAIL",
            "OK" if res['download_status'] else "FAIL",
            f"{res['duration']:.2f}",
            f"{res['throughput'] / (1024 * 1024):.2f}"
        ])
    print("\nDetail per Client:")
    print(tabulate(table, headers=headers, tablefmt="grid"))

    return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
   


    # Contoh eksekusi:
    # untuk file 10MB
    stress_test(pool_type='thread', workers=1, file_upload='file10MB.bin', file_download='file10MB.bin')
    stress_test(pool_type='thread', workers=5, file_upload='file10MB.bin', file_download='file10MB.bin')
    stress_test(pool_type='thread', workers=50, file_upload='file10MB.bin', file_download='file10MB.bin')

    # # Untuk file 50MB
    # stress_test(pool_type='thread', workers=1, file_upload='file50MB.bin', file_download='file50MB.bin')
    # stress_test(pool_type='thread', workers=5, file_upload='file50MB.bin', file_download='file50MB.bin')
    # stress_test(pool_type='thread', workers=50, file_upload='file50MB.bin', file_download='file50MB.bin')
    
    # # Untuk file 100MB
    # stress_test(pool_type='thread', workers=1, file_upload='file100MB.bin', file_download='file100MB.bin')
    # stress_test(pool_type='thread', workers=5, file_upload='file100MB.bin', file_download='file100MB.bin')
    # stress_test(pool_type='thread', workers=50, file_upload='file100MB.bin', file_download='file100MB.bin')