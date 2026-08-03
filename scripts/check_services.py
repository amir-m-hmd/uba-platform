import time
import socket
import urllib.request
import json

SERVICES = {
    "PostgreSQL": ("localhost", 5432),
    "Redpanda Kafka API": ("localhost", 9092),
    "ClickHouse Native": ("localhost", 9000),
}

HTTP_SERVICES = {
    "Redpanda Console": "http://localhost:8080",
    "ClickHouse HTTP": "http://localhost:8123/ping",
}

def check_tcp_port(host, port):
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except Exception:
        return False

def check_http_endpoint(url):
    try:
        response = urllib.request.urlopen(url, timeout=3)
        return response.getcode() in [200, 204]
    except Exception:
        return False

if __name__ == "__main__":
    print("=== System Health Check ===")
    
    for name, (host, port) in SERVICES.items():
        status = "ONLINE" if check_tcp_port(host, port) else "OFFLINE"
        print(f"[{status}] {name} ({host}:{port})")

    for name, url in HTTP_SERVICES.items():
        status = "ONLINE" if check_http_endpoint(url) else "OFFLINE"
        print(f"[{status}] {name} ({url})")
        