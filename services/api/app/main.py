"""API REST de Smart Energy (IDIC).

Comunica la interfaz web y otros componentes con la base de datos PostgreSQL.
Cada instancia responde con la cabecera X-Instancia para ver el balanceo de Nginx.
"""
import hmac
import os
import socket
import time
from contextlib import asynccontextmanager
from typing import Optional

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg.errors import ForeignKeyViolation
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout
from pydantic import BaseModel, Field

INSTANCIA = socket.gethostname()

POOL = ConnectionPool(
    conninfo="",
    kwargs=dict(
        host=os.getenv("DB_HOST", "10.10.20.12"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "smart_energy"),
        user=os.getenv("DB_USER", "energia_user"),
        password=os.getenv("DB_PASSWORD", ""),
        connect_timeout=5,
        row_factory=dict_row,
    ),
    min_size=1,
    max_size=int(os.getenv("DB_POOL_MAX", "5")),
    timeout=5,
    open=False,
    check=ConnectionPool.check_connection,
)

PETICIONES = Counter("api_peticiones_total", "Peticiones HTTP", ["metodo", "ruta", "codigo"])
LATENCIA = Histogram("api_latencia_segundos", "Latencia de las peticiones", ["ruta"])


@asynccontextmanager
async def lifespan(_app: FastAPI):
    POOL.open(wait=False)  # si la BD no esta lista, reintenta en segundo plano
    yield
    POOL.close()


app = FastAPI(title="IDIC Smart Energy API", version="1.0", lifespan=lifespan)


@app.middleware("http")
async def metricas(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    ruta = getattr(request.scope.get("route"), "path", "otra")
    PETICIONES.labels(request.method, ruta, str(response.status_code)).inc()
    LATENCIA.labels(ruta).observe(time.perf_counter() - t0)
    response.headers["X-Instancia"] = INSTANCIA
    return response


def consultar(sql: str, params: tuple = ()):
    try:
        with POOL.connection(timeout=3) as conn:
            return conn.execute(sql, params).fetchall()
    except (PoolTimeout, psycopg.OperationalError) as exc:
        raise HTTPException(status_code=503, detail="Base de datos no disponible") from exc


def verificar_clave(x_api_key: Optional[str] = Header(default=None)):
    esperada = os.getenv("API_KEY", "")
    if esperada and not hmac.compare_digest(x_api_key or "", esperada):
        raise HTTPException(status_code=401, detail="API key invalida")


class Medicion(BaseModel):
    zona: str = Field(min_length=1, max_length=40, pattern=r"^[a-z0-9_]+$")
    variable: str = Field(pattern=r"^(voltaje|corriente|potencia|energia)$")
    valor: float


@app.get("/health")
def health():
    db = False
    try:
        with POOL.connection(timeout=2) as conn:
            conn.execute("SELECT 1")
            db = True
    except Exception:  # noqa: BLE001 - el health nunca debe fallar
        pass
    return {"estado": "ok", "db": db, "instancia": INSTANCIA}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/zonas")
def zonas():
    return consultar("SELECT id, nombre, umbral_w FROM zonas ORDER BY id")


@app.get("/api/resumen")
def resumen():
    return consultar(
        """
        SELECT z.id AS zona, z.nombre, z.umbral_w,
               r.ultima_potencia_w, r.promedio_w, r.pico_w,
               COALESCE(r.consumo_kwh, 0) AS consumo_kwh, r.actualizado
        FROM zonas z LEFT JOIN resumen_zona r ON r.zona = z.id
        ORDER BY z.id
        """
    )


@app.get("/api/mediciones")
def mediciones(
    zona: Optional[str] = None,
    variable: Optional[str] = None,
    limite: int = Query(50, ge=1, le=500),
):
    return consultar(
        """
        SELECT zona, variable, valor, ts FROM mediciones
        WHERE (%s::text IS NULL OR zona = %s) AND (%s::text IS NULL OR variable = %s)
        ORDER BY ts DESC LIMIT %s
        """,
        (zona, zona, variable, variable, limite),
    )


@app.get("/api/alertas")
def alertas(limite: int = Query(20, ge=1, le=200)):
    return consultar(
        "SELECT id, zona, tipo, potencia_w, umbral_w, origen, mensaje, ts "
        "FROM alertas ORDER BY ts DESC LIMIT %s",
        (limite,),
    )


@app.get("/api/estadisticas")
def estadisticas():
    filas = consultar(
        "SELECT (SELECT count(*) FROM mediciones) AS mediciones, "
        "(SELECT count(*) FROM alertas) AS alertas"
    )
    return filas[0]


@app.post("/api/mediciones", status_code=201, dependencies=[Depends(verificar_clave)])
def crear_medicion(m: Medicion):
    try:
        with POOL.connection(timeout=3) as conn:
            conn.execute(
                "INSERT INTO mediciones (zona, variable, valor) VALUES (%s, %s, %s)",
                (m.zona, m.variable, m.valor),
            )
    except ForeignKeyViolation as exc:
        raise HTTPException(status_code=404, detail="Zona no registrada") from exc
    except (PoolTimeout, psycopg.OperationalError) as exc:
        raise HTTPException(status_code=503, detail="Base de datos no disponible") from exc
    return {"guardado": True}
