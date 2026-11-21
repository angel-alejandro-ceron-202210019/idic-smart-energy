"""Servicio de monitoreo de Smart Energy (IDIC).

Revisa cada INTERVALO_S segundos el estado de los componentes y registra
las caidas y recuperaciones. Expone:
  /estado   JSON con el estado actual y los ultimos eventos
  /metrics  formato Prometheus (componente_up, componente_latencia_ms)
  /health   comprobacion del propio monitor
"""
import os
import socket
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
import psycopg
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

INTERVALO = float(os.getenv("INTERVALO_S", "10"))
TIMEOUT = float(os.getenv("TIMEOUT_S", "3"))

UP = Gauge("componente_up", "1 si el componente responde", ["componente"])
LATENCIA = Gauge("componente_latencia_ms", "Latencia del ultimo chequeo", ["componente"])
CAMBIOS = Counter("componente_cambios_total", "Caidas y recuperaciones", ["componente", "evento"])


def chequeo_http(url):
    t0 = time.perf_counter()
    r = httpx.get(url, timeout=TIMEOUT)
    return r.status_code < 400, (time.perf_counter() - t0) * 1000


def chequeo_tcp(host, port):
    t0 = time.perf_counter()
    with socket.create_connection((host, port), timeout=TIMEOUT):
        pass
    return True, (time.perf_counter() - t0) * 1000


def chequeo_bd():
    t0 = time.perf_counter()
    with psycopg.connect(
        host=os.getenv("DB_HOST", "10.10.20.12"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "smart_energy"),
        user=os.getenv("DB_USER", "energia_user"),
        password=os.getenv("DB_PASSWORD", ""),
        connect_timeout=int(TIMEOUT),
    ) as conn:
        conn.execute("SELECT 1")
    return True, (time.perf_counter() - t0) * 1000


COMPONENTES = {
    "nginx": lambda: chequeo_http("http://nginx:80/health"),
    "web": lambda: chequeo_http("http://web:80/"),
    "api": lambda: chequeo_http("http://api:8000/health"),
    "mosquitto": lambda: chequeo_tcp("mosquitto", 1883),
    "procesador": lambda: chequeo_http("http://processor:8001/metrics"),
    "postgresql": chequeo_bd,
}

ESTADO = {n: {"up": None, "latencia_ms": None, "desde": None, "ultimo_chequeo": None}
          for n in COMPONENTES}
EVENTOS = deque(maxlen=50)


def ahora():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ciclo():
    while True:
        for nombre, fn in COMPONENTES.items():
            try:
                ok, ms = fn()
            except Exception:  # cualquier fallo cuenta como componente caido
                ok, ms = False, None
            e = ESTADO[nombre]
            if e["up"] is None or e["up"] != ok:
                if e["up"] is not None:
                    evento = "RECUPERADO" if ok else "CAIDO"
                    EVENTOS.appendleft({"ts": ahora(), "componente": nombre, "evento": evento})
                    CAMBIOS.labels(nombre, evento).inc()
                e["desde"] = ahora()
            e["up"], e["latencia_ms"], e["ultimo_chequeo"] = ok, (round(ms, 1) if ms else None), ahora()
            UP.labels(nombre).set(1 if ok else 0)
            if ms is not None:
                LATENCIA.labels(nombre).set(ms)
        time.sleep(INTERVALO)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    threading.Thread(target=ciclo, daemon=True).start()
    yield


app = FastAPI(title="IDIC Monitor", version="1.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"estado": "ok"}


@app.get("/estado")
def estado():
    return {"componentes": ESTADO, "eventos": list(EVENTOS)}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
