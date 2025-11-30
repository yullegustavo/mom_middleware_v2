from broker import Broker
from client import Client
import threading
import time


def start_broker():
    broker = Broker()
    t = threading.Thread(target=broker.start, daemon=True)
    t.start()
    return broker


def start_control_center():
    sub = Client()
    sub.subscribe("vehicle.WasteManagement:1.telemetry")

    def on_message(topic, payload):
        print("[CONTROL] chegou mensagem:")
        print("  tópico:", topic)
        print("  payload:", payload)

    t = threading.Thread(target=sub.listen, args=(on_message,), daemon=True)
    t.start()
    return sub


def main():
    start_broker()
    time.sleep(0.5)
    start_control_center()

    print("Broker e subscriber rodando. Pressione Ctrl+C para sair.")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
