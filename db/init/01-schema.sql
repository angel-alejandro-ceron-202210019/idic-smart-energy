-- Esquema del modulo Smart Energy (IDIC). Se ejecuta solo la primera vez que
-- arranca el contenedor de PostgreSQL con un volumen vacio.

CREATE TABLE IF NOT EXISTS zonas (
    id        TEXT PRIMARY KEY,
    nombre    TEXT NOT NULL,
    umbral_w  DOUBLE PRECISION NOT NULL CHECK (umbral_w > 0)
);

CREATE TABLE IF NOT EXISTS mediciones (
    id        BIGSERIAL PRIMARY KEY,
    zona      TEXT NOT NULL REFERENCES zonas(id),
    variable  TEXT NOT NULL CHECK (variable IN ('voltaje','corriente','potencia','energia')),
    valor     DOUBLE PRECISION NOT NULL,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_mediciones_zona_var_ts ON mediciones (zona, variable, ts DESC);
CREATE INDEX IF NOT EXISTS idx_mediciones_ts ON mediciones (ts DESC);

CREATE TABLE IF NOT EXISTS alertas (
    id          BIGSERIAL PRIMARY KEY,
    zona        TEXT NOT NULL REFERENCES zonas(id),
    tipo        TEXT NOT NULL,
    potencia_w  DOUBLE PRECISION,
    umbral_w    DOUBLE PRECISION,
    origen      TEXT NOT NULL,
    mensaje     TEXT,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alertas_ts ON alertas (ts DESC);

CREATE TABLE IF NOT EXISTS resumen_zona (
    zona               TEXT PRIMARY KEY REFERENCES zonas(id),
    ultima_potencia_w  DOUBLE PRECISION,
    promedio_w         DOUBLE PRECISION,
    pico_w             DOUBLE PRECISION,
    consumo_kwh        DOUBLE PRECISION NOT NULL DEFAULT 0,
    actualizado        TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO zonas (id, nombre, umbral_w) VALUES
    ('aula1',        'Aula 1',        1500),
    ('aula2',        'Aula 2',        1500),
    ('laboratorio1', 'Laboratorio 1', 3000),
    ('laboratorio2', 'Laboratorio 2', 2800),
    ('biblioteca',   'Biblioteca',    2200),
    ('cafeteria',    'Cafeteria',     3500)
ON CONFLICT (id) DO NOTHING;
