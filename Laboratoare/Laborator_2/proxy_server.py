#!/usr/bin/env python3
"""
Reverse proxy (clean) with caching + deterministic routing by id + aggregation for GET /employees.
Additional behavior: invalidate cache for aggregated and per-id GET keys after successful write.
Run: python -u proxy_server.py
"""
import sys
import logging
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import requests
from cache_layer import CacheLayer
from load_balancer import LoadBalancer
import json

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='[Proxy] %(message)s')
logger = logging.getLogger("proxy")

# configure backends
BACKENDS = ['http://localhost:8001', 'http://localhost:8002']

class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    session = requests.Session()
    cache = CacheLayer()
    lb = LoadBalancer(BACKENDS)

    def _make_cache_key(self):
        parsed = urlparse(self.path)
        key = f"{self.command}:{parsed.geturl()}"
        self._saved_body = b''
        self._saved_id = None

        if self.command in ('POST', 'PUT'):
            length = int(self.headers.get('Content-Length', 0))
            if length:
                body = self.rfile.read(length)
                self._saved_body = body
                try:
                    j = json.loads(body.decode('utf-8'))
                    if isinstance(j, dict) and 'id' in j:
                        self._saved_id = str(j['id'])
                except Exception:
                    pass
                key += f":{self._saved_body.decode('utf-8', errors='ignore')}"
        else:
            parsed_qs = parse_qs(parsed.query)
            if 'id' in parsed_qs:
                self._saved_id = parsed_qs['id'][0]

        return key

    def _send_raw(self, status, headers, body_bytes):
        self.send_response(status)
        for k, v in headers.items():
            if k.lower() in ('transfer-encoding', 'connection', 'keep-alive', 'proxy-authenticate',
                             'proxy-authorization', 'te', 'trailers', 'upgrade'):
                continue
            self.send_header(k, str(v))
        self.send_header('Content-Length', str(len(body_bytes)))
        self.end_headers()
        if body_bytes:
            self.wfile.write(body_bytes)

    def do_GET(self):
        return self._handle_forward()

    def do_POST(self):
        return self._handle_forward()

    def do_PUT(self):
        return self._handle_forward()

    def _aggregate_get_from_backends(self, path_with_query, headers):
        aggregated = []
        errors = []
        for backend in self.lb.backends:
            target = backend + path_with_query
            try:
                resp = self.session.get(target, headers=headers, timeout=5)
                if resp.status_code != 200:
                    errors.append((backend, resp.status_code))
                    continue
                try:
                    j = resp.json()
                except Exception:
                    continue
                if isinstance(j, list):
                    aggregated.extend(j)
                elif isinstance(j, dict):
                    aggregated.append(j)
            except requests.RequestException as e:
                errors.append((backend, str(e)))
        return aggregated, errors

    def _handle_forward(self):
        cache_key = self._make_cache_key()
        cached = self.cache.get(cache_key)
        if cached:
            try:
                obj = json.loads(cached)
                status = obj.get('status', 200)
                headers = obj.get('headers', {})
                body = obj.get('body', '').encode('utf-8')
                headers.setdefault('X-Proxy-Cache', 'HIT')
                headers.setdefault('X-Backend', 'cached')
                self._send_raw(status, headers, body)
                logger.info("Cache HIT for %s", cache_key)
                return
            except Exception:
                pass

        method = self.command
        resource_id = getattr(self, '_saved_id', None)
        parsed = urlparse(self.path)
        headers = {k: v for k, v in self.headers.items() if k.lower() != 'host'}

        if method == 'GET' and not resource_id:
            path_with_query = parsed.path
            if parsed.query:
                path_with_query += '?' + parsed.query
            aggregated, errors = self._aggregate_get_from_backends(path_with_query, headers)
            body_text = json.dumps(aggregated, ensure_ascii=False)
            resp_headers = {'Content-Type': 'application/json; charset=utf-8',
                            'X-Proxy-Cache': 'MISS', 'X-Backend': 'aggregated'}
            self.cache.put(cache_key, json.dumps({'status': 200, 'headers': resp_headers, 'body': body_text}), ttl_seconds=30)
            logger.info("Aggregated GET %s -> total %s items (errors: %s)", self.path, len(aggregated), errors)
            self._send_raw(200, resp_headers, body_text.encode('utf-8'))
            return

        if method == 'GET' and resource_id:
            for backend in self.lb.backends:
                target = backend + parsed.path + f"?id={resource_id}"
                try:
                    resp = self.session.get(target, headers=headers, timeout=5)
                    if resp.status_code == 200:
                        resp_headers = {k: v for k, v in resp.headers.items()}
                        resp_headers['X-Proxy-Cache'] = 'MISS'
                        resp_headers['X-Backend'] = backend
                        body_bytes = resp.content
                        self.cache.put(cache_key, json.dumps({'status': 200, 'headers': resp_headers,
                                                              'body': body_bytes.decode('utf-8')}), ttl_seconds=30)
                        self._send_raw(resp.status_code, resp_headers, body_bytes)
                        logger.info("GET with id %s forwarded to %s", resource_id, backend)
                        return
                except requests.RequestException:
                    continue
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')
            return

        # POST/PUT -> replicate to all backends
        if method in ('POST', 'PUT'):
            body = getattr(self, '_saved_body', b'')
            success = []
            errors = []
            for backend in self.lb.backends:
                target = backend + self.path
                try:
                    resp = self.session.request(method, target, headers=headers, data=body, timeout=10)
                    if resp.status_code == 200:
                        success.append(backend)
                    else:
                        errors.append((backend, resp.status_code))
                except requests.RequestException as e:
                    errors.append((backend, str(e)))

            if success:
                # Invalidate relevant cache keys:
                # - aggregated GET list: "GET:/employees" (without query)
                # - per-id GET for the id just written: "GET:/employees/?id=<id>"
                # Determine id if available
                written_id = getattr(self, '_saved_id', None)
                try:
                    # aggregated key (no query)
                    self.cache.invalidate("GET:/employees")
                    if written_id:
                        self.cache.invalidate(f"GET:/employees/?id={written_id}")
                except Exception:
                    logger.warning("Cache invalidation failed (continuing)")

                resp_headers = {'Content-Type': 'application/json; charset=utf-8',
                                'X-Proxy-Cache': 'MISS', 'X-Backend': ','.join(success)}
                # echo last successful body as response body (or you could request read-back)
                self._send_raw(200, resp_headers, body)
                logger.info("%s replicated to %s (errors: %s)", method, success, errors)
            else:
                self.send_response(502)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Bad Gateway')
            return

    def log_message(self, format, *args):
        logger.info("%s - - [%s] %s", self.client_address[0], self.log_date_time_string(), format % args)

def run(port=8080):
    server = ThreadingHTTPServer(('0.0.0.0', port), ProxyHandler)
    logger.info('ProxyServer running on 0.0.0.0:%s', port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down proxy...')
    finally:
        server.server_close()
        ProxyHandler.cache.stop()

if __name__ == '__main__':
    run()