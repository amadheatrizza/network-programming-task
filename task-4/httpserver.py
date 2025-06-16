import os
from datetime import datetime
import urllib.parse
from http import HTTPStatus

class FileHandler:
    def __init__(self, storage_dir='./storage'):
        self.storage = storage_dir
        self._make_storage()
        self.file_types = {
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg',
            '.png': 'image/png',
            '.txt': 'text/plain',
            '.html': 'text/html',
            '.json': 'application/json'
        }

    def _make_storage(self):
        os.makedirs(self.storage, exist_ok=True)

    def process(self, raw_data):
        cmd, path, meta, content = self._breakdown(raw_data)
        if not cmd:
            return self._fail(HTTPStatus.BAD_REQUEST, "Bad request")
        
        path = urllib.parse.unquote(path)
        print(f":: {cmd} {path}")

        try:
            if cmd == 'GET':
                return self._get(path)
            elif cmd == 'POST':
                return self._store(path, meta, content)
            elif cmd == 'DELETE':
                return self._erase(path)
            return self._fail(HTTPStatus.METHOD_NOT_ALLOWED, "Not allowed")
        except Exception as e:
            print(f"!! Server error: {e}")
            return self._fail(HTTPStatus.INTERNAL_SERVER_ERROR, "Server broke")

    def _breakdown(self, raw):
        parts = raw.split(b'\r\n\r\n', 1)
        head = parts[0].decode('utf-8').split('\r\n')
        body = parts[1] if len(parts) > 1 else b''

        try:
            method, path, _ = head[0].split()
            headers = dict(
                line.split(':', 1) 
                for line in head[1:] 
                if ':' in line
            )
            return method.upper(), path, headers, body
        except:
            return None, None, None, None

    def _get(self, path):
        if path == '/':
            return self._ok("Ready")
        elif path == '/list':
            return self._show_files()
        return self._send_file(path)

    def _store(self, path, meta, content):
        if path != '/upload':
            return self._fail(HTTPStatus.BAD_REQUEST, "Wrong path")
        
        fname = meta.get('X-File-Name', '').strip()
        if not fname:
            return self._fail(HTTPStatus.BAD_REQUEST, "No filename")

        if not self._valid_name(fname):
            return self._fail(HTTPStatus.BAD_REQUEST, "Bad filename")

        try:
            full_path = os.path.join(self.storage, fname)
            with open(full_path, 'wb') as f:
                f.write(content)
            print(f"++ Stored {fname}")
            return self._ok(f"Saved {fname}", HTTPStatus.CREATED)
        except Exception as e:
            print(f"!! Store failed: {e}")
            return self._fail(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed: {e}")

    def _erase(self, path):
        fname = path.lstrip('/')
        if not fname or not self._valid_name(fname):
            return self._fail(HTTPStatus.BAD_REQUEST, "Invalid")

        full_path = os.path.join(self.storage, fname)
        if not os.path.exists(full_path):
            return self._fail(HTTPStatus.NOT_FOUND, "Not found")

        try:
            os.remove(full_path)
            print(f"++ Erased {fname}")
            return self._ok(f"Gone {fname}")
        except Exception as e:
            print(f"!! Erase failed: {e}")
            return self._fail(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed: {e}")

    def _show_files(self):
        try:
            files = os.listdir(self.storage)
            page = "<html><body><h1>Files:</h1><ul>"
            page += "".join(f"<li>{f}</li>" for f in files)
            page += "</ul></body></html>"
            print(f":: Listed {len(files)} files")
            return self._ok(page, headers={'Content-Type': 'text/html'})
        except Exception as e:
            print(f"!! List failed: {e}")
            return self._fail(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed: {e}")

    def _send_file(self, path):
        safe_path = self._clean_path(path)
        if not safe_path or not os.path.isfile(safe_path):
            return self._fail(HTTPStatus.NOT_FOUND, "Not found")

        try:
            with open(safe_path, 'rb') as f:
                data = f.read()
            ext = os.path.splitext(safe_path)[1].lower()
            content_type = self.file_types.get(ext, 'application/octet-stream')
            print(f":: Sent {safe_path}")
            return self._ok(data, headers={'Content-Type': content_type})
        except Exception as e:
            print(f"!! Send failed: {e}")
            return self._fail(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed: {e}")

    def _clean_path(self, path):
        rel_path = path.lstrip('/')
        if not rel_path or '..' in rel_path:
            return None
        abs_path = os.path.abspath(os.path.join(self.storage, rel_path))
        return abs_path if abs_path.startswith(os.path.abspath(self.storage)) else None

    def _valid_name(self, name):
        return name and not any(c in name for c in '/\\') and '..' not in name

    def _ok(self, data, status=HTTPStatus.OK, headers=None):
        if not isinstance(data, bytes):
            data = str(data).encode('utf-8')
        return self._build(status, data, headers)

    def _fail(self, status, msg):
        return self._build(status, str(msg).encode('utf-8'))

    def _build(self, status, data, headers=None):
        if headers is None:
            headers = {}
        headers.setdefault('Content-Type', 'text/plain')
        
        response = [
            f"HTTP/1.1 {status.value} {status.phrase}\r\n",
            f"Date: {datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n",
            "Server: PyServ/1.0\r\n",
            f"Content-Length: {len(data)}\r\n"
        ]
        response.extend(f"{k}: {v}\r\n" for k, v in headers.items())
        response.append("\r\n")
        return b"".join(line.encode('utf-8') for line in response) + data
