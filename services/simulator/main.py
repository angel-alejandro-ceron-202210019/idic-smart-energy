"""Simulador de medidores de energia (tipo ESP32/NodeMCU) - Smart Energy IDIC.

Publica por MQTT para cada zona:
  campus/energia/<zona>/voltaje | corriente | potencia | energia
  campus/energia/<zona>/alerta   (JSON) cuando la potencia supera el umbral

Para provocar un pico (demo):
  mosquitto_pub -h <broker> -t campus/control/pico -m laboratorio1     (o "todas")
"""
import json
import os
import random
import signal
import threading
import time

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
INTERVALO_S = float(os.getenv("INTERVALO_S", "2"))
FACTOR_PICO = float(os.getenv("FACTOR_PICO_SIM", "3.0"))
DURACION_PICO_S = float(os.getenv("DURACION_PICO_S", "25"))
VOLTAJE_NOMINAL = float(os.getenv("VOLTAJE_NOMINAL", "127"))
FACTOR_POTENCIA = 0.92
ENFRIAMIENTO_ALERTA_S = 30.0

# zona: (potencia_base_w, umbral_w)
ZONAS = {
    "aula1": (800, 1500),
    "aula2": (700, 1500),
    "laboratorio1": (1800, 3000),
    "laboratorio2": (1500, 2800),
    "biblioteca": (1200, 2200),
    "cafeteria": (2200, 3500),
}
SOLO = [z for z in os.getenv("ZONAS", "").split(",") if z.strip()]


class Medidor:
    def __init__(self, zona, base, umbral):
        self.zona, self.base, self.umbral = zona, base, umbral
        self.kwh = random.uniform(50, 400)
        self.pico_hasta = 0.0
        self.ult_alerta = 0.0

    def lectura(self, t):
        factor = FACTOR_PICO if t < self.pico_hasta else 1.0
        potencia = self.base * (0.85 + 0.30 * random.random()) * factor
        voltaje = random.gauss(VOLTAJE_NOMINAL, 1.2)
        corriente = potencia / (voltaje * FACTOR_POTENCIA)
        self.kwh += potencia * INTERVALO_S / 3_600_000
        return voltaje, corriente, potencia, self.kwh


MEDIDORES = {z: Medidor(z, b, u) for z, (b, u) in ZONAS.items() if not SOLO or z in SOLO}
PARAR = threading.Event()


def on_connect(client, _userdata, _flags, reason_code, _properties):
    if not reason_code.is_failure:
        print("Conectado a MQTT", flush=True)
        client.subscribe("campus/control/pico", qos=0)


def on_message(_client, _userdata, msg):
    destino = msg.payload.decode("utf-8", "ignore").strip()
    objetivo = list(MEDIDORES) if destino == "todas" else [destino]
    for zona in objetivo:
        if zona in MEDIDORES:
            MEDIDORES[zona].pico_hasta = time.time() + DURACION_PICO_S
            print(f"PICO provocado en {zona} por {DURACION_PICO_S:.0f} s", flush=True)


def publicar(client, zona, variable, valor, qos=1):
    client.publish(f"campus/energia/{zona}/{variable}", valor, qos=qos)


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="idic-simulador")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    signal.signal(signal.SIGTERM, lambda *_: PARAR.set())
    signal.signal(signal.SIGINT, lambda *_: PARAR.set())
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    print(f"Simulando {len(MEDIDORES)} zonas cada {INTERVALO_S} s", flush=True)
    while not PARAR.is_set():
        t = time.time()
        if client.is_connected():
            for zona, m in MEDIDORES.items():
                v, i, p, kwh = m.lectura(t)
                publicar(client, zona, "voltaje", f"{v:.1f}")
                publicar(client, zona, "corriente", f"{i:.2f}")
                publicar(client, zona, "potencia", f"{p:.1f}")
                publicar(client, zona, "energia", f"{kwh:.4f}")
                if p > m.umbral and t - m.ult_alerta >= ENFRIAMIENTO_ALERTA_S:
                    m.ult_alerta = t
                    evento = {"zona": zona, "potencia_w": round(p, 1), "umbral_w": m.umbral,
                              "mensaje": f"Consumo de {p:.0f} W sobre el umbral de {m.umbral} W"}
                    publicar(client, zona, "alerta", json.dumps(evento))
        PARAR.wait(INTERVALO_S)
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
