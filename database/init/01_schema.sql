-- Serving-layer schema for the ward database.
-- DRAFT from the project plan. Owner: Member B, to be finalised and signed off on Day 3.
-- Runs once, on the first start of an empty Postgres volume
-- (re-apply with: docker compose down -v && docker compose up -d).

-- Speed view: latest windowed state per patient (upserted by Spark Q1, Member A).
-- One row per patient: the 2-minute sliding window that ends with the patient's latest reading.
CREATE TABLE IF NOT EXISTS patient_current_status (
    patient_id          TEXT PRIMARY KEY,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    last_event_time     TIMESTAMPTZ NOT NULL,
    sim_day             INTEGER     NOT NULL,
    avg_heart_rate      DOUBLE PRECISION,
    min_heart_rate      DOUBLE PRECISION,
    max_heart_rate      DOUBLE PRECISION,
    avg_spo2            DOUBLE PRECISION,
    min_spo2            DOUBLE PRECISION,
    max_spo2            DOUBLE PRECISION,
    avg_systolic_bp     DOUBLE PRECISION,
    min_systolic_bp     DOUBLE PRECISION,
    max_systolic_bp     DOUBLE PRECISION,
    avg_diastolic_bp    DOUBLE PRECISION,
    min_diastolic_bp    DOUBLE PRECISION,
    max_diastolic_bp    DOUBLE PRECISION,
    avg_temperature     DOUBLE PRECISION,
    min_temperature     DOUBLE PRECISION,
    max_temperature     DOUBLE PRECISION,
    reading_count       INTEGER,
    abnormal_count      INTEGER,
    hr_trend            TEXT,          -- RISING / STABLE / FALLING
    spo2_trend          TEXT,
    vital_risk_score    INTEGER,
    vital_risk_category TEXT,          -- NORMAL / WATCH / CONCERNING
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Alerts raised by Spark Q3 and stored by the alert consumer (Member C).
CREATE TABLE IF NOT EXISTS vital_alerts (
    alert_id      TEXT PRIMARY KEY,
    patient_id    TEXT        NOT NULL,
    rule          TEXT        NOT NULL,
    severity      TEXT        NOT NULL,
    metric_value  DOUBLE PRECISION,
    event_time    TIMESTAMPTZ NOT NULL,
    sim_day       INTEGER     NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_vital_alerts_patient_time ON vital_alerts (patient_id, event_time DESC);

-- Daily lab feed, loaded by the daily_lab_consolidation DAG (Member B).
CREATE TABLE IF NOT EXISTS lab_results (
    sim_day         INTEGER     NOT NULL,
    patient_id      TEXT        NOT NULL,
    test_type       TEXT        NOT NULL,
    result_value    DOUBLE PRECISION,
    unit            TEXT,
    reference_low   DOUBLE PRECISION,
    reference_high  DOUBLE PRECISION,
    collected_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sim_day, patient_id, test_type, collected_at)
);

-- Batch view: per-patient vital summary for one sim day, recomputed from the Parquet lake (Member A).
CREATE TABLE IF NOT EXISTS vital_daily_summary (
    sim_day            INTEGER NOT NULL,
    patient_id         TEXT    NOT NULL,
    avg_heart_rate     DOUBLE PRECISION,
    max_heart_rate     DOUBLE PRECISION,
    min_spo2           DOUBLE PRECISION,
    max_temperature    DOUBLE PRECISION,
    max_systolic_bp    DOUBLE PRECISION,
    reading_count      INTEGER,
    abnormal_count     INTEGER,
    PRIMARY KEY (sim_day, patient_id)
);

-- Consolidated daily risk report: vitals + latest labs (Member B).
CREATE TABLE IF NOT EXISTS daily_patient_risk (
    sim_day              INTEGER NOT NULL,
    patient_id           TEXT    NOT NULL,
    vital_risk_score     INTEGER,
    lab_risk_score       INTEGER,
    total_risk_score     INTEGER,
    risk_category        TEXT,       -- NORMAL / WATCH / CONCERNING
    lab_status           TEXT,       -- AVAILABLE / LAB_UNAVAILABLE
    abnormal_labs        TEXT,       -- e.g. 'potassium:HIGH,crp:HIGH'
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, patient_id)
);

-- Pipeline health checks and speed-vs-batch reconciliation results (Member C).
CREATE TABLE IF NOT EXISTS pipeline_health (
    id          BIGSERIAL PRIMARY KEY,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    component   TEXT NOT NULL,
    check_name  TEXT NOT NULL,
    status      TEXT NOT NULL,       -- OK / WARN / FAIL
    value       DOUBLE PRECISION,
    details     JSONB
);
CREATE INDEX IF NOT EXISTS ix_pipeline_health_time ON pipeline_health (checked_at DESC);
