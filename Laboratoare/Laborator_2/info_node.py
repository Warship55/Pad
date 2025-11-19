#!/usr/bin/env python3
"""
InfoNode HTTP server with Cassandra persistence (with in-memory fallback).
Run: python -u info_node.py [port]

Behavior:
- Attempts to connect to Cassandra on startup.
- Creates keyspace/table if they do not exist.
- On POST/PUT: writes to Cassandra if available, else writes to in-memory store.
- On GET: reads from Cassandra if available, else from in-memory store.
Configuration via environment variables:
- CASS_CONTACT_POINTS (comma separated, default "127.0.0.1")
- CASS_KEYSPACE (default "warehouse")
"""
import os
import sys
import json
import logging
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from dicttoxml import dicttoxml
from typing import Dict, Any, Optional

# Cassandra driver
try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
except Exception:
    Cluster = None

# configure logging to stdout
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='[InfoNode] %(message)s')
logger = logging.getLogger("info_node")

# in-memory fallback store
_store: Dict[str, Dict[str, Any]] = {}
_store_lock = threading.RLock()

# Cassandra configuration
CASS_CONTACT_POINTS = os.environ.get('CASS_CONTACT_POINTS', '127.0.0.1').split(',')
CASS_KEYSPACE = os.environ.get('CASS_KEYSPACE', 'warehouse')

class CassandraClient:
    def __init__(self, contact_points, keyspace):
        self.cluster = None
        self.session = None
        self.keyspace = keyspace
        if Cluster is None:
            logger.warning("cassandra-driver not installed; Cassandra disabled")
            return
        try:
            logger.info("Connecting to Cassandra at %s (keyspace=%s)", contact_points, keyspace)
            self.cluster = Cluster(contact_points)
            self.session = self.cluster.connect()
            # create keyspace and table if needed
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}}
            """)
            self.session.set_keyspace(keyspace)
            # create table employees
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id text PRIMARY KEY,
                    name text,
                    title text
                )
            """)
            # prepare basic statements (we'll use simple execute for brevity)
            logger.info("Connected to Cassandra and ensured schema exists")
        except Exception as e:
            logger.warning("Cassandra connection/setup failed: %s", e)
            self.close()
            self.session = None

    def insert_employee(self, emp: Dict[str, Any]):
        if not self.session:
            raise RuntimeError("No Cassandra session")
        # safe insert - uses simple statement
        try:
            self.session.execute(
                "INSERT INTO employees (id, name, title) VALUES (%s, %s, %s)",
                (str(emp.get('id')), emp.get('name'), emp.get('title'))
            )
        except Exception as e:
            raise

    def get_employee(self, emp_id: str) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("No Cassandra session")
        row = self.session.execute("SELECT id, name, title FROM employees WHERE id=%s", (str(emp_id),)).one()
        if row:
            return {'id': row.id, 'name': row.name, 'title': row.title}
        return None

    def list_employees(self):
        if not self.session:
            raise RuntimeError("No Cassandra session")
        stmt = SimpleStatement("SELECT id, name, title FROM employees")
        rows = self.session.execute(stmt)
        result = []
        for r in rows:
            result.append({'id': r.id, 'name': r.name, 'title': r.title})
        return result

    def close(self):
        try:
            if self.cluster:
                self.cluster.shutdown()
        except Exception:
            pass

# instantiate Cassandra client
cass_client = CassandraClient(contact_points=CASS_CONTACT_POINTS, keyspace=CASS_KEYSPACE)

def to_json(obj):
    return json.dumps(obj, ensure_ascii=False)

def to_xml(obj):
    b = dicttoxml(obj, custom_root='response', attr_type=False)
    return b.decode('utf-8')

class InfoHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def _parse_format(self, query, headers):
        q = parse_qs(query)
        if 'format' in q:
            return q['format'][0].lower()
        accept = headers.get('Accept', '')
        if 'xml' in accept:
            return 'xml'
        return 'json'

    def _send(self, status: int, body: str, content_type: str = 'application/json; charset=utf-8'):
        body_bytes = body.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parsed.query
        fmt = self._parse_format(query, self.headers)

        if path != '/employees' and not path.startswith('/employees/'):
            self._send(404, 'Not Found', 'text/plain')
            return

        params = parse_qs(query)
        id_list = params.get('id')
        try:
            if id_list:
                emp_id = id_list[0]
                if cass_client.session:
                    emp = cass_client.get_employee(emp_id)
                    if emp is None:
                        self._send(404, 'Not Found', 'text/plain')
                        return
                    out = emp
                else:
                    with _store_lock:
                        item = _store.get(emp_id)
                    if item is None:
                        self._send(404, 'Not Found', 'text/plain')
                        return
                    out = item
            else:
                if cass_client.session:
                    out = cass_client.list_employees()
                else:
                    with _store_lock:
                        out = list(_store.values())
        except Exception as e:
            logger.error("Error reading data: %s", e)
            self._send(500, 'Internal Server Error', 'text/plain')
            return

        logger.info("Served GET %s (items returned: %s)", self.path, (len(out) if isinstance(out, list) else 1))
        if fmt == 'xml':
            self._send(200, to_xml(out), 'application/xml; charset=utf-8')
        else:
            self._send(200, to_json(out), 'application/json; charset=utf-8')

    def _read_body_json(self):
        length = int(self.headers.get('Content-Length', 0))
        if length == 0:
            return {}
        data = self.rfile.read(length)
        text = data.decode('utf-8', errors='ignore')
        return json.loads(text)

    def do_PUT(self):
        self._handle_put_post()

    def do_POST(self):
        self._handle_put_post()

    def _handle_put_post(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path != '/employees' and not path.startswith('/employees/'):
            self._send(404, 'Not Found', 'text/plain')
            return

        try:
            payload = self._read_body_json()
        except Exception as e:
            logger.warning("JSON parse error on %s: %s", self.path, e)
            self._send(400, 'Bad Request', 'text/plain')
            return

        id_val = str(payload.get('id', ''))
        if not id_val:
            import uuid
            id_val = str(uuid.uuid4())
            payload['id'] = id_val

        # write to Cassandra if available, else in-memory
        try:
            if cass_client.session:
                cass_client.insert_employee(payload)
            else:
                with _store_lock:
                    _store[id_val] = payload
        except Exception as e:
            logger.error("Write error (Cassandra): %s. Falling back to memory.", e)
            with _store_lock:
                _store[id_val] = payload

        logger.info("Received %s %s id=%s", self.command, self.path, id_val)
        resp = {'result': 'ok', 'id': id_val}
        self._send(200, to_json(resp), 'application/json; charset=utf-8')

    def log_message(self, format, *args):
        logger.info("%s - - [%s] %s", self.client_address[0], self.log_date_time_string(), format % args)

def run(port=8001):
    server = ThreadingHTTPServer(('0.0.0.0', port), InfoHandler)
    logger.info('InfoNode running on 0.0.0.0:%s', port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down...')
    finally:
        cass_client.close()
        server.server_close()

if __name__ == '__main__':
    p = 8001
    if len(sys.argv) > 1:
        p = int(sys.argv[1])
    run(p)