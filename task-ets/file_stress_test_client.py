import socket
import json
import base64
import logging
import os
import time
import concurrent.futures
import argparse
import statistics
import csv
import psutil

DEFAULT_SERVER_ADDRESS = ('localhost', 6667)
DEFAULT_CHUNK_SIZE = 128 * 1024 * 1024
MEMORY_THRESHOLD = 0.9
RESULT_DIRECTORIES = ['test_files', 'downloads']
OPERATION_TYPES = ['upload', 'download', 'list']
EXECUTOR_TYPES = ['thread', 'process']

def configure_logging(debug=False):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("stress_test.log"),
            logging.StreamHandler()
        ]
    )

def ensure_directories_exist():
    for directory in RESULT_DIRECTORIES:
        os.makedirs(directory, exist_ok=True)

def check_memory_usage(threshold=MEMORY_THRESHOLD):
    memory = psutil.virtual_memory()
    if memory.percent / 100 > threshold:
        logging.warning(f"High memory usage: {memory.percent}%")
        return True
    return False

def generate_test_file(file_size_mb, directory='test_files'):
    filename = f"test_file_{file_size_mb}MB.bin"
    filepath = os.path.join(directory, filename)
    
    if os.path.exists(filepath) and os.path.getsize(filepath) == file_size_mb * DEFAULT_CHUNK_SIZE:
        return filepath
    
    with open(filepath, 'wb') as file:
        for _ in range(file_size_mb):
            if check_memory_usage():
                raise MemoryError("Memory threshold exceeded")
            file.write(os.urandom(DEFAULT_CHUNK_SIZE))
    return filepath

class FileServerClient:
    def __init__(self, server_address=DEFAULT_SERVER_ADDRESS):
        self.server_address = server_address
        self.reset_counters()
        ensure_directories_exist()

    def reset_counters(self):
        self.results = {op: [] for op in OPERATION_TYPES}
        self.success_count = {op: 0 for op in OPERATION_TYPES}
        self.fail_count = {op: 0 for op in OPERATION_TYPES}

    def send_command(self, command_str=""):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(600)
        
        try:
            sock.connect(self.server_address)
            chunks = [command_str[i:i+65536] for i in range(0, len(command_str), 65536)]
            for chunk in chunks:
                sock.sendall(chunk.encode())
            sock.sendall("\r\n\r\n".encode())
            
            data_received = ""
            while True:
                data = sock.recv(DEFAULT_CHUNK_SIZE)
                if data:
                    data_received += data.decode()
                    if "\r\n\r\n" in data_received:
                        break
                else:
                    break
            
            return json.loads(data_received.split("\r\n\r\n")[0])
        except socket.timeout:
            return {'status': 'ERROR', 'data': 'Socket timeout'}
        except ConnectionRefusedError:
            return {'status': 'ERROR', 'data': 'Connection refused'}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}
        finally:
            sock.close()

    def perform_upload(self, file_path, worker_id):
        start_time = time.time()
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        try:
            encoded_chunks = []
            with open(file_path, 'rb') as file:
                while True:
                    if check_memory_usage():
                        raise MemoryError("Memory threshold exceeded")
                    chunk = file.read(DEFAULT_CHUNK_SIZE)
                    if not chunk:
                        break
                    encoded_chunks.append(base64.b64encode(chunk).decode())
            
            command_str = f"UPLOAD {filename} {' '.join(encoded_chunks)}"
            result = self.send_command(command_str)
            duration = time.time() - start_time
            
            if result['status'] == 'OK':
                self.success_count['upload'] += 1
                logging.info(f"Worker {worker_id}: UPLOAD successful in {duration:.2f}s")
            else:
                self.fail_count['upload'] += 1
                logging.error(f"Worker {worker_id}: UPLOAD failed: {result['data']}")
            return self._create_result('upload', worker_id, file_size, duration, result)
        except Exception as e:
            logging.error(f"Worker {worker_id}: UPLOAD exception! {str(e)}")
            return self._create_error_result('upload', worker_id, file_size, start_time, str(e))

    def perform_download(self, filename, worker_id):
        start_time = time.time()
        
        try:
            result = self.send_command(f"GET {filename}")
            if result['status'] != 'OK':
                return self._create_error_result('download', worker_id, 0, start_time, result['data'])
            
            download_path = os.path.join('downloads', f"worker{worker_id}_{filename}")
            file_content = base64.b64decode(result['data_file'])
            file_size = len(file_content)
            
            with open(download_path, 'wb') as file:
                for i in range(0, file_size, DEFAULT_CHUNK_SIZE):
                    if check_memory_usage():
                        raise MemoryError("Memory threshold exceeded")
                    file.write(file_content[i:i+DEFAULT_CHUNK_SIZE])
            
            self.success_count['download'] += 1
            duration = time.time() - start_time
            logging.info(f"Worker {worker_id}: DOWNLOAD successful in {duration:.2f}s")
            return self._create_result('download', worker_id, file_size, duration, result)
        except Exception as e:
            logging.error(f"Worker {worker_id}: DOWNLOAD exception! {str(e)}")
            return self._create_error_result('download', worker_id, 0, start_time, str(e))

    def perform_list(self, worker_id):
        start_time = time.time()
        
        try:
            result = self.send_command("LIST")
            duration = time.time() - start_time
            
            if result['status'] == 'OK':
                self.success_count['list'] += 1
                logging.info(f"Worker {worker_id}: LIST successful in {duration:.2f}s")
            else:
                self.fail_count['list'] += 1
                logging.error(f"Worker {worker_id}: LIST failed: {result['data']}")
            
            return {
                'worker_id': worker_id,
                'operation': 'list',
                'duration': duration,
                'status': result['status']
            }
        except Exception as e:
            logging.error(f"Worker {worker_id}: LIST exception! {str(e)}")
            return {
                'worker_id': worker_id,
                'operation': 'list',
                'duration': time.time() - start_time,
                'status': 'ERROR',
                'error': str(e)
            }

    def _create_result(self, operation, worker_id, file_size, duration, result):
        return {
            'worker_id': worker_id,
            'operation': operation,
            'file_size': file_size,
            'duration': duration,
            'throughput': file_size / duration if duration > 0 else 0,
            'status': result['status']
        }

    def _create_error_result(self, operation, worker_id, file_size, start_time, error):
        duration = time.time() - start_time
        self.fail_count[operation] += 1
        result = {
            'worker_id': worker_id,
            'operation': operation,
            'file_size': file_size,
            'duration': duration,
            'throughput': 0,
            'status': 'ERROR',
            'error': error
        }
        return result

    def run_stress_test(self, operation, file_size_mb, client_pool_size, executor_type='thread'):
        self.reset_counters()
        
        if operation not in OPERATION_TYPES:
            logging.error(f"Invalid operation: {operation}")
            return
            
        test_file = None
        if operation in ['upload', 'download']:
            test_file = generate_test_file(file_size_mb)
            if operation == 'download':
                upload_result = self.perform_upload(test_file, 0)
                if upload_result['status'] != 'OK':
                    return None
        
        effective_pool_size = min(client_pool_size, 10) if file_size_mb >= 50 and client_pool_size >= 50 else client_pool_size
        executor_class = concurrent.futures.ThreadPoolExecutor if executor_type == 'thread' else concurrent.futures.ProcessPoolExecutor
        
        logging.info(f"{operation.upper()} test_file_{file_size_mb}MB starting...")
        
        all_results = []
        batch_size = effective_pool_size
        
        with executor_class(max_workers=batch_size) as executor:
            for batch_start in range(0, client_pool_size, batch_size):
                batch_end = min(batch_start + batch_size, client_pool_size)
                futures = []
                
                for i in range(batch_start, batch_end):
                    if operation == 'upload':
                        futures.append(executor.submit(self.perform_upload, test_file, i))
                    elif operation == 'download':
                        futures.append(executor.submit(self.perform_download, os.path.basename(test_file), i))
                    else:
                        futures.append(executor.submit(self.perform_list, i))
                
                for future in concurrent.futures.as_completed(futures):
                    all_results.append(future.result())
                    if check_memory_usage():
                        time.sleep(1)
        
        return self._calculate_statistics(operation, file_size_mb, client_pool_size, executor_type, all_results)

    def _calculate_statistics(self, operation, file_size_mb, client_pool_size, executor_type, results):
        durations = [r['duration'] for r in results if r['status'] == 'OK']
        throughputs = [r['throughput'] for r in results if r.get('throughput', 0) > 0]
        
        if not durations:
            return {
                'operation': operation,
                'file_size_mb': file_size_mb,
                'client_pool_size': client_pool_size,
                'executor_type': executor_type,
                'success_count': self.success_count[operation],
                'fail_count': self.fail_count[operation]
            }
        
        stats = {
            'operation': operation,
            'file_size_mb': file_size_mb,
            'client_pool_size': client_pool_size,
            'executor_type': executor_type,
            'avg_duration': statistics.mean(durations),
            'median_duration': statistics.median(durations),
            'min_duration': min(durations),
            'max_duration': max(durations),
            'avg_throughput': statistics.mean(throughputs) if throughputs else 0,
            'median_throughput': statistics.median(throughputs) if throughputs else 0,
            'min_throughput': min(throughputs) if throughputs else 0,
            'max_throughput': max(throughputs) if throughputs else 0,
            'success_count': self.success_count[operation],
            'fail_count': self.fail_count[operation]
        }
        
        logging.info(f"{operation.upper()} test_file_{file_size_mb}MB complete: {stats['success_count']} succeeded, {stats['fail_count']} failed")
        return stats

    def run_all_tests(self, file_sizes, client_pool_sizes, server_pool_sizes, executor_types, operations):
        all_stats = []
        
        for server_pool_size in server_pool_sizes:
            logging.info(f"Server pool size: {server_pool_size}")
            logging.info("Please restart the server with the appropriate pool size!")
            input("Press Enter when ready...")
          
            for executor_type in executor_types:
                for operation in operations:
                    for file_size in file_sizes:
                        for client_pool_size in client_pool_sizes:
                            try:
                                stats = self.run_stress_test(operation, file_size, client_pool_size, executor_type)
                                if stats:
                                    stats['server_pool_size'] = server_pool_size
                                    all_stats.append(stats)
                            except Exception as e:
                                all_stats.append(self._create_error_stats(operation, file_size, client_pool_size, server_pool_size, executor_type, str(e)))
        
        self._save_results_to_csv(all_stats)

    def _create_error_stats(self, operation, file_size, client_pool_size, server_pool_size, executor_type, error):
        return {
            'operation': operation,
            'file_size_mb': file_size,
            'client_pool_size': client_pool_size,
            'server_pool_size': server_pool_size,
            'executor_type': executor_type,
            'avg_duration': 0,
            'median_duration': 0,
            'min_duration': 0,
            'max_duration': 0,
            'avg_throughput': 0,
            'median_throughput': 0,
            'min_throughput': 0,
            'max_throughput': 0,
            'success_count': 0,
            'fail_count': client_pool_size,
            'error': error
        }

    def _save_results_to_csv(self, all_stats):
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        csv_filename = f"stress_test_results_{timestamp}.csv"
        
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = [
                'operation', 'file_size_mb', 'client_pool_size', 'server_pool_size', 'executor_type',
                'avg_duration', 'median_duration', 'min_duration', 'max_duration',
                'avg_throughput', 'median_throughput', 'min_throughput', 'max_throughput',
                'success_count', 'fail_count'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_stats)
        
        logging.info(f"Results saved to {csv_filename}")

def parse_arguments():
    parser = argparse.ArgumentParser(description='File Server Stress Test Client')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=6667)
    parser.add_argument('--operation', choices=['upload', 'download', 'list', 'all'], default='all')
    parser.add_argument('--file-sizes', type=int, nargs='+', default=[10, 50, 100])
    parser.add_argument('--client-pools', type=int, nargs='+', default=[1, 5, 50])
    parser.add_argument('--server-pools', type=int, nargs='+', default=[1, 5, 50])
    parser.add_argument('--executor', choices=['thread', 'process', 'both'], default='thread')
    parser.add_argument('--debug', action='store_true')
    return parser.parse_args()
  
def run_single_test(args):
  return (len(args.operation) == 1 and 
          len(args.file_sizes) == 1 and 
          len(args.client_pools) == 1 and 
          len(args.server_pools) == 1)

def run_tests(args):
    configure_logging(args.debug)
    client = FileServerClient((args.host, args.port))
    
    executor_types = EXECUTOR_TYPES if args.executor == 'both' else [args.executor]
    operations = OPERATION_TYPES if args.operation == 'all' else [args.operation]
    
    if run_single_test(args):
        stats = client.run_stress_test(operations[0], args.file_sizes[0], args.client_pools[0], executor_types[0])
        if stats:
            stats['server_pool_size'] = args.server_pools[0]
            client._save_results_to_csv([stats])
    else:
        client.run_all_tests(args.file_sizes, args.client_pools, args.server_pools, executor_types, operations)

if __name__ == "__main__":
    run_tests(parse_arguments())