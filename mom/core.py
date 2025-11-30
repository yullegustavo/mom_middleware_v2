from threading import Lock
from typing import Set
from dataclasses import dataclass
from typing import Any, Dict, Optional
import json
from queue import Queue
from threading import Thread
from typing import Callable


@dataclass(frozen=True)
class Message:
    topic: str
    payload: Any
    headers: Optional[Dict[str, str]] = None


class Marshaller:
    @staticmethod
    def encode(msg: Message) -> bytes:
        data = {
            "payload": msg.payload,
            "headers": msg.headers or {},
        }
        return json.dumps(data).encode("utf-8")

    @staticmethod
    def decode(data: bytes) -> dict:
        obj = json.loads(data.decode("utf-8"))
        return obj


# Parte II Step 3: SubscriptionManager Step IV: Broker (socket) Step V: Client e teste pub/sub


class SubscriptionManager:
    def __init__(self) -> None:
        self._subs: dict[str, Set[object]] = {}
        self._lock = Lock()

    def add(self, topic: str, client: object) -> None:
        with self._lock:
            self._subs.setdefault(topic, set()).add(client)

    def remove(self, topic: str, client: object) -> None:
        with self._lock:
            if topic in self._subs:
                self._subs[topic].discard(client)
                if not self._subs[topic]:
                    del self._subs[topic]

    def get(self, topic: str) -> list[object]:
        with self._lock:
            return list(self._subs.get(topic, ()))


class NotificationEngine:
    def __init__(self) -> None:
        self.queue: "Queue[Message]" = Queue()

    def publish(self, message: Message) -> None:
        self.queue.put(message)


class NotificationConsumer(Thread):
    def __init__(
        self,
        engine: NotificationEngine,
        get_subscribers: Callable[[str], list[object]],
        send_fn: Callable[[object, Message], None],
        daemon: bool = True,
    ) -> None:
        super().__init__(daemon=daemon)
        self._engine = engine
        self._get_subscribers = get_subscribers
        self._send_fn = send_fn

    def run(self) -> None:
        while True:
            msg = self._engine.queue.get()
            subs = self._get_subscribers(msg.topic)
            for client in subs:
                self._send_fn(client, msg)
                # self._engine.queue.task_done()
