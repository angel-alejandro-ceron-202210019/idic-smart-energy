# IDIC - Smart Energy

Infraestructura Distribuida Inteligente en la Nube. Modulo Smart Energy del Smart Campus:
medidores IoT simulados -> MQTT (Mosquitto) -> procesamiento -> PostgreSQL -> API REST -> panel web.

## Estructura

| Ruta | Contenido |
|---|---|
| `docker-compose.apps.yml` | VM1 (srv-apps): Nginx, web, API, Mosquitto, procesamiento, monitoreo, simulador |
| `docker-compose.db.yml` | VM2 (srv-db): PostgreSQL |
| `db/init/` | Esquema SQL y zonas iniciales |
| `mosquitto/`, `nginx/` | Configuracion del broker y del proxy |
| `services/api` | API REST (FastAPI) |
| `services/processor` | Servicio de procesamiento (MQTT -> BD) |
| `services/monitor` | Servicio de monitoreo de componentes |
| `services/web` | Interfaz de administracion |
| `services/simulator` | Simulador de medidores (tipo ESP32) |
| `monitoring/`, `.github/workflows/` | Prometheus/Grafana y CI/CD (Parte 6) |

## Despliegue rapido

```bash
cp .env.example .env              # y cambia DB_PASSWORD
# en srv-db
docker compose -f docker-compose.db.yml up -d
# en srv-apps
docker compose -f docker-compose.apps.yml up -d --build
docker compose -f docker-compose.apps.yml --profile iot up -d simulator
```

Panel: `http://192.168.56.11/`
