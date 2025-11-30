import socket
import threading
from typing import Dict

from core_comentado import (
    Message,
    Marshaller,
    SubscriptionManager,
    NotificationEngine,
    NotificationConsumer,
)


class Broker:
    """
    Broker do middleware MOM.
    Responsável por:
    - aceitar conexões TCP de clientes;
    - interpretar comandos de texto SUB/PUB;
    - gerenciar inscrições por tópico;
    - repassar publicações para o NotificationEngine.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 5000) -> None:
        # Endereço onde o broker vai escutar
        self._host = host
        self._port = port

        # Socket TCP principal (socket servidor)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Permite reutilizar a porta rapidamente após reiniciar o servidor
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Gerenciador de inscrições (tópico -> sockets inscritos)
        self._subs = SubscriptionManager()

        # Engine + consumer para entrega assíncrona
        self._engine = NotificationEngine()
        self._consumer = NotificationConsumer(
            self._engine,
            self._subs.get,         # função para obter inscritos de um tópico
            self._send_to_client,   # função que envia a mensagem em um socket
        )
        self._consumer.start()

        # Mapa simples de clientes conectados (não é estritamente necessário aqui,
        # mas pode ser útil para estatísticas ou gerenciamento futuro).
        self._clients: Dict[socket.socket, socket.socket] = {}

    def start(self) -> None:
        """
        Inicia o loop de aceitação de novas conexões.
        Para cada novo cliente, cria uma thread dedicada que executa _handle_client.
        """
        self._sock.bind((self._host, self._port))
        self._sock.listen()
        print(f"Broker escutando em {self._host}:{self._port}")

        while True:
            client_sock, addr = self._sock.accept()
            print("Nova conexão de", addr)
            self._clients[client_sock] = client_sock

            # Cada cliente é tratado em uma thread separada
            t = threading.Thread(
                target=self._handle_client,
                args=(client_sock,),
                daemon=True,
            )
            t.start()

    def _handle_client(self, client_sock: socket.socket) -> None:
        """
        Loop de tratamento de um cliente.
        Protocolo de linha:
        - SUB <topic>\n
        - PUB <topic> <size>\n<payload-json>
        """
        buf = b""
        try:
            while True:
                data = client_sock.recv(4096)
                if not data:
                    # Cliente fechou a conexão
                    break
                buf += data

                # Processa linha por linha (até encontrar '\n')
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.decode("utf-8").strip()
                    if not line:
                        continue

                    parts = line.split()
                    cmd = parts[0].upper()

                    if cmd == "SUB" and len(parts) == 2:
                        # Comando de inscrição em tópico
                        topic = parts[1]
                        self._subs.add(topic, client_sock)

                    elif cmd == "PUB" and len(parts) == 3:
                        # Comando de publicação: header + corpo em JSON
                        topic = parts[1]
                        size = int(parts[2])

                        # Garante que já recebemos todo o corpo (size bytes)
                        while len(buf) < size:
                            more = client_sock.recv(4096)
                            if not more:
                                return
                            buf += more

                        # Separa o corpo da mensagem do restante do buffer
                        payload_bytes, buf = buf[:size], buf[size:]

                        # Decodifica JSON em dict com 'payload' e 'headers'
                        obj = Marshaller.decode(payload_bytes)

                        # Reconstrói a Message com o tópico vindo do header textual
                        msg = Message(
                            topic=topic,
                            payload=obj.get("payload"),
                            headers=obj.get("headers", {}),
                        )

                        # Enfileira a mensagem na NotificationEngine
                        self._engine.publish(msg)

                    else:
                        # Comando não reconhecido: por enquanto é apenas ignorado
                        pass
        finally:
            # Em caso de erro ou desconexão, remove o cliente de todos os tópicos
            for topic in list(self._subs._subs.keys()):
                self._subs.remove(topic, client_sock)
            client_sock.close()

    def _send_to_client(self, client_sock: socket.socket, msg: Message) -> None:
        """
        Função usada pelo NotificationConsumer para enviar mensagens
        a cada subscriber. Implementa o lado 'downstream' do protocolo:

        MSG <topic> <size>\n<payload-json>
        """
        try:
            data = Marshaller.encode(msg)
            header = f"MSG {msg.topic} {len(data)}\n".encode("utf-8")
            client_sock.sendall(header + data)
        except OSError:
            # Se o socket estiver quebrado, apenas descartamos a tentativa de envio.
            pass
