import socket
import threading
from typing import Dict

from core import (
    Message,
    Marshaller,
    SubscriptionManager,
    NotificationEngine,
    NotificationConsumer,
)


class Broker:
    def __init__(self, host: str = "127.0.0.1", port: int = 5000) -> None:
        self._host = host
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self._subs = SubscriptionManager()
        self._engine = NotificationEngine()

        self._consumer = NotificationConsumer(
            self._engine,
            self._subs.get,
            self._send_to_client,
        )
        self._consumer.start()

        self._clients: Dict[socket.socket, socket.socket] = {}

    def start(self) -> None:
        self._sock.bind((self._host, self._port))
        self._sock.listen()
        print(f"Broker escutando em {self._host}:{self._port}")

        while True:
            client_sock, addr = self._sock.accept()
            print("Nova conexão de", addr)
            self._clients[client_sock] = client_sock
            t = threading.Thread(
                target=self._handle_client,
                args=(client_sock,),
                daemon=True,
            )
            t.start()

    def _handle_client(self, client_sock: socket.socket) -> None:
        buf = b""
        try:
            while True:
                data = client_sock.recv(4096)
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

                    if cmd == "SUB" and len(parts) == 2:
                        topic = parts[1]
                        self._subs.add(topic, client_sock)

                    elif cmd == "PUB" and len(parts) == 3:
                        topic = parts[1]
                        size = int(parts[2])
                        while len(buf) < size:
                            more = client_sock.recv(4096)
                            if not more:
                                return
                            buf += more
                        payload_bytes, buf = buf[:size], buf[size:]
                        obj = Marshaller.decode(payload_bytes)
                        msg = Message(
                            topic=topic,
                            payload=obj.get("payload"),
                            headers=obj.get("headers", {}),
                        )
                        self._engine.publish(msg)

                    else:
                        # comando desconhecido
                        pass
        finally:
            for topic in list(self._subs._subs.keys()):
                self._subs.remove(topic, client_sock)
            client_sock.close()

    def _send_to_client(self, client_sock: socket.socket, msg: Message) -> None:
        try:
            data = Marshaller.encode(msg)
            header = f"MSG {msg.topic} {len(data)}\n".encode("utf-8")
            client_sock.sendall(header + data)
        except OSError:
            # conexão quebrada; vamos só ignorar por enquanto
            pass
