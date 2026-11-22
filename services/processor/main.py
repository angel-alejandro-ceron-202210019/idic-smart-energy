"""Servicio de procesamiento de Smart Energy (IDIC).

Flujo: MQTT (Mosquitto) -> cola en memoria -> calculos -> PostgreSQL.
- Guarda cada medicion (voltaje, corriente, potencia, energia).
- Calcula promedio movil y consumo acumulado (kWh) por zona.
- Detecta picos: potencia sobre el umbral de la zona o salto estadistico.
- Recibe eventos de alerta publicados por los medidores.
Si la BD o el broker caen, reintenta solo; los mensajes esperan en la cola.
"""
import json
import logging
import os
import queue
import re
import signal
import statistics
import threading
import time
from collections import deque

import paho.mqtt.client as mqtt
import psycopg
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("procesador")

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "idic-procesador")
TOPIC_FILTRO = "campus/energia/+/+"
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

VENTANA = int(os.getenv("VENTANA_PROMEDIO", "60"))
FACTOR_PICO = float(os.getenv("FACTOR_PICO", "1.5"))
ENFRIAMIENTO_S = float(os.getenv("ENFRIAMIENTO_ALERTA_S", "60"))
UMBRAL_DEFECTO_W = float(os.getenv("UMBRAL_DEFECTO_W", "2000"))
MAX_HUECO_S = float(os.getenv("MAX_HUECO_S", "300"))

VARIABLES = {"voltaje", "corriente", "potencia", "energia"}
ZONA_VALIDA = re.compile(r"^[a-z0-9_]{1,40}$")

MENSAJES = Counter("energia_mensajes_total", "Mensajes MQTT procesados", ["variable"])
POTENCIA = Gauge("energia_potencia_watts", "Ultima potencia por zona", ["zona"])
PROMEDIO = Gauge("energia_potencia_promedio_watts", "Promedio movil por zona", ["zona"])
CONSUMO = Gauge("energia_consumo_acumulado_kwh", "Consumo acumulado calculado", ["zona"])
ALERTAS = Counter("energia_alertas_total", "Alertas registradas", ["zona", "tipo"])
MQTT_UP = Gauge("procesador_mqtt_conectado", "1 si hay conexion con el broker")
DB_UP = Gauge("procesador_db_conectada", "1 si la ultima operacion en BD funciono")
COLA_TAM = Gauge("procesador_cola_mensajes", "Mensajes esperando en la cola")
ERRORES = Counter("procesador_errores_total", "Errores", ["origen"])


class BaseDatos:
    """Conexion unica con reconexion automatica."""

    def __init__(self):
        self.conn = None
        self.lock = threading.Lock()

    def _conectar(self):
        self.conn = psycopg.connect(
            host=os.getenv("DB_HOST", "10.10.20.12"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "smart_energy"),
            user=os.getenv("DB_USER", "energia_user"),
            password=os.getenv("DB_PASSWORD", ""),
            connect_timeout=5,
            autocommit=True,
        )

    def _ejecutar(self, sql, params, traer):
        with self.lock:
            for intento in (1, 2):
                try:
                    if self.conn is None or self.conn.closed:
                        self._conectar()
                    cur = self.conn.execute(sql, params)
                    resultado = cur.fetchone() if traer else None
                    DB_UP.set(1)
                    return resultado
                except psycopg.Error:
                    DB_UP.set(0)
                    self.conn = None
                    if intento == 2:
                        raise
                    time.sleep(1)

    def ejecutar(self, sql, params=()):
        self._ejecutar(sql, params, traer=False)

    def uno(self, sql, params=()):
        return self._ejecutar(sql, params, traer=True)


BD = BaseDatos()
COLA = queue.Queue(maxsize=int(os.getenv("COLA_MAX", "10000")))
UMBRALES = {}
ESTADO = {}


def cargar_umbrales():
    with BD.lock:
        if BD.conn is None or BD.conn.closed:
            BD._conectar()
        filas = BD.conn.execute("SELECT id, umbral_w FROM zonas").fetchall()
    for zona, umbral in filas:
        UMBRALES[zona] = float(umbral)


def umbral_de(zona):
    umbral = UMBRALES.get(zona)
    if umbral is None:
        BD.ejecutar(
            "INSERT INTO zonas (id, nombre, umbral_w) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO NOTHING", (zona, zona, UMBRAL_DEFECTO_W))
        fila = BD.uno("SELECT umbral_w FROM zonas WHERE id = %s", (zona,))
        umbral = float(fila[0])
        UMBRALES[zona] = umbral
    return umbral


def estado_zona(zona):
    e = ESTADO.get(zona)
    if e is None:
        fila = BD.uno("SELECT consumo_kwh, pico_w FROM resumen_zona WHERE zona = %s", (zona,))
        e = {
            "ventana": deque(maxlen=VENTANA),
            "t_prev": None,
            "p_prev": None,
            "kwh": float(fila[0]) if fila and fila[0] is not None else 0.0,
            "pico": float(fila[1]) if fila and fila[1] is not None else 0.0,
            "ult_alerta": 0.0,
        }
        ESTADO[zona] = e
    return e


def leer_valor(payload: bytes) -> float:
    texto = payload.decode("utf-8").strip()
    try:
        return float(texto)
    except ValueError:
        return float(json.loads(texto)["valor"])


def registrar_alerta(zona, tipo, potencia, umbral, origen, mensaje):
    BD.ejecutar(
        "INSERT INTO alertas (zona, tipo, potencia_w, umbral_w, origen, mensaje) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (zona, tipo, potencia, umbral, origen, mensaje))
    ALERTAS.labels(zona, tipo).inc()
    log.warning("ALERTA %s [%s/%s]: %s", zona, tipo, origen, mensaje)


def analizar_potencia(zona, p, t):
    e = estado_zona(zona)
    umbral = umbral_de(zona)
    prom_prev = statistics.fmean(e["ventana"]) if len(e["ventana"]) >= 10 else None

    if e["t_prev"] is not None:
        dt = t - e["t_prev"]
        if 0 < dt <= MAX_HUECO_S:  # trapecio: kW * horas
            e["kwh"] += ((p + e["p_prev"]) / 2.0) / 1000.0 * (dt / 3600.0)
    e["t_prev"], e["p_prev"] = t, p
    e["ventana"].append(p)
    e["pico"] = max(e["pico"], p)
    prom = statistics.fmean(e["ventana"])

    tipo = None
    if p > umbral:
        tipo = "umbral"
    elif prom_prev is not None and p > FACTOR_PICO * prom_prev and p > 0.5 * umbral:
        tipo = "pico_estadistico"
    if tipo and (t - e["ult_alerta"]) >= ENFRIAMIENTO_S:
        e["ult_alerta"] = t
        registrar_alerta(zona, tipo, p, umbral, "procesamiento",
                         f"Potencia {p:.0f} W (umbral {umbral:.0f} W, promedio {prom:.0f} W)")

    BD.ejecutar(
        """
        INSERT INTO resumen_zona (zona, ultima_potencia_w, promedio_w, pico_w, consumo_kwh, actualizado)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (zona) DO UPDATE SET
            ultima_potencia_w = EXCLUDED.ultima_potencia_w,
            promedio_w = EXCLUDED.promedio_w,
            pico_w = EXCLUDED.pico_w,
            consumo_kwh = EXCLUDED.consumo_kwh,
            actualizado = now()
        """, (zona, p, prom, e["pico"], e["kwh"]))
    POTENCIA.labels(zona).set(p)
    PROMEDIO.labels(zona).set(prom)
    CONSUMO.labels(zona).set(e["kwh"])


def procesar_alerta(zona, payload):
    datos = json.loads(payload.decode("utf-8"))
    umbral_de(zona)  # asegura que la zona exista
    registrar_alerta(zona, "umbral", datos.get("potencia_w"), datos.get("umbral_w"),
                     "medidor", datos.get("mensaje", "Evento de alerta del medidor"))


def procesar(zona, variable, payload, t):
    if variable == "alerta":
        procesar_alerta(zona, payload)
        MENSAJES.labels("alerta").inc()
        return
    valor = leer_valor(payload)
    umbral_de(zona)  # registra la zona si es nueva
    BD.ejecutar("INSERT INTO mediciones (zona, variable, valor) VALUES (%s, %s, %s)",
                (zona, variable, valor))
    MENSAJES.labels(variable).inc()
    if variable == "potencia":
        analizar_potencia(zona, valor, t)


def trabajador():
    ultima_carga = 0.0
    while True:
        item = COLA.get()
        COLA_TAM.set(COLA.qsize())
        while True:
            try:
                if time.time() - ultima_carga > 60:
                    cargar_umbrales()
                    ultima_carga = time.time()
                procesar(*item)
                break
            except psycopg.Error as exc:
                ERRORES.labels("bd").inc()
                DB_UP.set(0)
                log.error("BD no disponible, reintentando en 2 s: %s", exc)
                time.sleep(2)
            except Exception as exc:  # mensaje mal formado: se descarta
                ERRORES.labels("mensaje").inc()
                log.error("Mensaje descartado %s: %s", item[:2], exc)
                break


def on_connect(client, _userdata, _flags, reason_code, _properties):
    if reason_code.is_failure:
        log.error("Conexion MQTT rechazada: %s", reason_code)
        return
    MQTT_UP.set(1)
    log.info("Conectado a MQTT %s:%s", MQTT_HOST, MQTT_PORT)
    client.subscribe([(TOPIC_FILTRO, 1)])


def on_disconnect(_client, _userdata, _flags, reason_code, _properties):
    MQTT_UP.set(0)
    log.warning("Desconectado de MQTT (%s). Reintentando...", reason_code)


def on_message(_client, _userdata, msg):
    partes = msg.topic.split("/")
    if len(partes) != 4 or partes[0] != "campus" or partes[1] != "energia":
        return
    zona, variable = partes[2], partes[3]
    if not ZONA_VALIDA.match(zona) or (variable not in VARIABLES and variable != "alerta"):
        return
    try:
        COLA.put_nowait((zona, variable, msg.payload, time.time()))
        COLA_TAM.set(COLA.qsize())
    except queue.Full:
        ERRORES.labels("cola_llena").inc()


def main():
    start_http_server(METRICS_PORT)
    threading.Thread(target=trabajador, daemon=True).start()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID,
                         clean_session=False)  # sesion persistente: no pierde mensajes QoS 1
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    signal.signal(signal.SIGTERM, lambda *_: client.disconnect())
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    main()
