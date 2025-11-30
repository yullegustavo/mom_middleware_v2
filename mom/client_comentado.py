import socket
import json
from typing import Callable


class Client:
    """
    Cliente genérico do middleware MOM.
    Pode atuar como publisher, subscriber ou ambos, sobre uma conexão TCP.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 5000) -> None:
        # Endereço do broker ao qual este cliente irá se conectar
        self._host = host
        self._port = port

        # Socket TCP do lado cliente
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self._host, self._port))

    def publish(self, topic: str, payload: dict) -> None:
        """
        Envia uma publicação para o broker.
        Protocolo:
        - Linha de comando:  PUB <topic> <size>\n
        - Corpo:             JSON com {"payload": ...}
        """
        data = json.dumps({"payload": payload}).encode("utf-8")
        header = f"PUB {topic} {len(data)}\n".encode("utf-8")
        self._sock.sendall(header + data)

    def subscribe(self, topic: str) -> None:
        """
        Solicita inscrição em um tópico.
        Protocolo:
        - SUB <topic>\n
        """
        cmd = f"SUB {topic}\n".encode("utf-8")
        self._sock.sendall(cmd)

    def listen(self, on_message: Callable[[str, dict], None]) -> None:
        """
        Loop de recepção de mensagens vindas do broker.
        - Bloqueia lendo do socket.
        - Interpreta cabeçalho 'MSG <topic> <size>\\n'.
        - Lê 'size' bytes com o corpo JSON.
        - Chama o callback on_message(topic, payload).
        """
        buf = b""
        while True:
            data = self._sock.recv(4096)
            if not data:
                # Broker fechou a conexão ou ocorreu erro
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

                    # Garante que todo o corpo da mensagem foi recebido
                    while len(buf) < size:
                        more = self._sock.recv(4096)
                        if not more:
                            return
                        buf += more

                    payload_bytes, buf = buf[:size], buf[size:]
                    obj = json.loads(payload_bytes.decode("utf-8"))

                    # Passa apenas o 'payload' para o callback de aplicação
                    on_message(topic, obj.get("payload"))

    def close(self) -> None:
        """Fecha explicitamente a conexão com o broker."""
        self._sock.close()
