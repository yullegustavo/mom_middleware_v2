import socket
import json
from typing import Callable


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 5000) -> None:
        self._host = host
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self._host, self._port))

    def publish(self, topic: str, payload: dict) -> None:
        data = json.dumps({"payload": payload}).encode("utf-8")
        header = f"PUB {topic} {len(data)}\n".encode("utf-8")
        self._sock.sendall(header + data)

    def subscribe(self, topic: str) -> None:
        cmd = f"SUB {topic}\n".encode("utf-8")
        self._sock.sendall(cmd)

    def listen(self, on_message: Callable[[str, dict], None]) -> None:
        buf = b""
        while True:
            data = self._sock.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.decode("utf-8").strip()
                if not line:
                    continue
                parts = line.split()
                cmd = parts[0].upper()
                if cmd == "MSG" and len(parts) == 3:
                    topic = parts[1]
                    size = int(parts[2])
                    while len(buf) < size:
                        more = self._sock.recv(4096)
                        if not more:
                            return
                        buf += more
                    payload_bytes, buf = buf[:size], buf[size:]
                    obj = json.loads(payload_bytes.decode("utf-8"))
                    on_message(topic, obj.get("payload"))

    def close(self) -> None:
        self._sock.close()
