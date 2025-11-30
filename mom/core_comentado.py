# Exclusão mútua para acessar o mapa de inscritos
from threading import Lock
from typing import Set, Any, Dict, Optional, Callable
# Facilita definição imutável de Message
from dataclasses import dataclass
import json                              # Serialização JSON de payload/headers
# Fila thread-safe para o mecanismo de notificação
from queue import Queue
from threading import Thread             # Thread que consome a fila de mensagens


@dataclass(frozen=True)
class Message:
    """
    Representa uma mensagem do middleware MOM.
    - topic: canal lógico de publicação/assinatura.
    - payload: conteúdo da mensagem (qualquer tipo serializável em JSON).
    - headers: metadados opcionais (ex.: timestamp, ID da mensagem, etc.).
    """
    topic: str
    payload: Any
    headers: Optional[Dict[str, str]] = None


class Marshaller:
    """
    Responsável por codificar/decodificar mensagens para envio em rede.
    Encapsula a política de serialização (aqui, JSON).
    """

    @staticmethod
    def encode(msg: Message) -> bytes:
        """
        Converte uma Message em bytes para transporte no socket.
        Formato JSON: {"payload": ..., "headers": {...}}.
        O tópico é enviado fora do corpo (no cabeçalho textual do protocolo).
        """
        data = {
            "payload": msg.payload,
            "headers": msg.headers or {},
        }
        return json.dumps(data).encode("utf-8")

    @staticmethod
    def decode(data: bytes) -> dict:
        """
        Faz o caminho inverso de encode: a partir de bytes em JSON,
        devolve um dicionário com as chaves 'payload' e 'headers'.
        """
        obj = json.loads(data.decode("utf-8"))
        return obj

# Parte II Step 3: SubscriptionManager
# Step IV: Broker (socket)
# Step V: Client e teste pub/sub


class SubscriptionManager:
    """
    Mantém o mapa tópico -> conjunto de clientes inscritos.
    Usa Lock para permitir acesso concorrente seguro entre threads.
    """

    def __init__(self) -> None:
        # topic -> set de 'clients' (sockets)
        self._subs: dict[str, Set[object]] = {}
        self._lock = Lock()

    def add(self, topic: str, client: object) -> None:
        """
        Adiciona um cliente ao conjunto de inscritos de um tópico.
        Cria o conjunto se o tópico ainda não existir.
        """
        with self._lock:
            self._subs.setdefault(topic, set()).add(client)

    def remove(self, topic: str, client: object) -> None:
        """
        Remove um cliente de um tópico.
        Se o tópico ficar vazio, remove o tópico do mapa.
        """
        with self._lock:
            if topic in self._subs:
                self._subs[topic].discard(client)
                if not self._subs[topic]:
                    del self._subs[topic]

    def get(self, topic: str) -> list[object]:
        """
        Retorna a lista de clientes inscritos em um tópico.
        Se não houver inscritos, devolve lista vazia.
        """
        with self._lock:
            return list(self._subs.get(topic, ()))


class NotificationEngine:
    """
    Núcleo de entrega assíncrona.
    Apenas expõe uma fila thread-safe onde o broker enfileira mensagens.
    """

    def __init__(self) -> None:
        # Fila de mensagens a serem entregues aos subscribers
        self.queue: "Queue[Message]" = Queue()

    def publish(self, message: Message) -> None:
        """
        Enfileira uma mensagem para entrega posterior pelo NotificationConsumer.
        """
        self.queue.put(message)


class NotificationConsumer(Thread):
    """
    Thread responsável por consumir a fila de NotificationEngine
    e despachar mensagens para todos os inscritos em cada tópico.
    """

    def __init__(
        self,
        engine: NotificationEngine,
        get_subscribers: Callable[[str], list[object]],
        send_fn: Callable[[object, Message], None],
        daemon: bool = True,
    ) -> None:
        """
        - engine: instância do NotificationEngine com a fila de mensagens.
        - get_subscribers: função que devolve a lista de clientes de um tópico.
        - send_fn: função usada para efetivamente enviar a mensagem a um cliente
                   (no nosso caso, escrever no socket).
        """
        super().__init__(daemon=daemon)
        self._engine = engine
        self._get_subscribers = get_subscribers
        self._send_fn = send_fn

    def run(self) -> None:
        """
        Loop infinito da thread consumidora:
        - bloqueia em queue.get();
        - obtém os inscritos do tópico;
        - envia a mensagem para cada cliente via send_fn.
        """
        while True:
            msg = self._engine.queue.get()
            subs = self._get_subscribers(msg.topic)
            for client in subs:
                self._send_fn(client, msg)
            # Não usamos task_done/join porque não há sincronização de término.
