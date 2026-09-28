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
-- One row per result in labs_day=N.csv. A rerun of day N deletes that day's rows and
-- inserts them again in one transaction, so reruns never duplicate.
CREATE TABLE IF NOT EXISTS lab_results (
    sim_day         INTEGER     NOT NULL,
    patient_id      TEXT        NOT NULL,
    test_type       TEXT        NOT NULL,
    result_value    DOUBLE PRECISION NOT NULL,
    unit            TEXT        NOT NULL,
    reference_low   DOUBLE PRECISION NOT NULL,
    reference_high  DOUBLE PRECISION NOT NULL,
    collected_at    TIMESTAMPTZ NOT NULL,
    source_file     TEXT,                              -- lineage: which landing file
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, patient_id, test_type, collected_at),
    CHECK (reference_low <= reference_high)
);
CREATE INDEX IF NOT EXISTS ix_lab_results_patient_day ON lab_results (patient_id, sim_day DESC);

-- Batch view: per-patient vital summary for one sim day, recomputed from the Parquet lake (Member A).
-- Holds every aggregate common/risk_rules.vital_points() needs (incl. min_systolic_bp for BP < 90).
CREATE TABLE IF NOT EXISTS vital_daily_summary (
    sim_day            INTEGER NOT NULL,
    patient_id         TEXT    NOT NULL,
    avg_heart_rate     DOUBLE PRECISION,
    min_heart_rate     DOUBLE PRECISION,
    max_heart_rate     DOUBLE PRECISION,
    avg_spo2           DOUBLE PRECISION,
    min_spo2           DOUBLE PRECISION,
    avg_temperature    DOUBLE PRECISION,
    max_temperature    DOUBLE PRECISION,
    avg_systolic_bp    DOUBLE PRECISION,
    min_systolic_bp    DOUBLE PRECISION,
    max_systolic_bp    DOUBLE PRECISION,
    avg_diastolic_bp   DOUBLE PRECISION,
    reading_count      INTEGER,
    abnormal_count     INTEGER,
    computed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, patient_id)
);

-- Consolidated daily risk report: vitals + latest labs (Member B).
-- Scores come from common/risk_rules.py. lab_risk_score is NULL (never 0) when lab_status
-- is LAB_UNAVAILABLE; the total then counts vitals only.
CREATE TABLE IF NOT EXISTS daily_patient_risk (
    sim_day              INTEGER NOT NULL,
    patient_id           TEXT    NOT NULL,
    vital_risk_score     INTEGER,
    vital_reasons        TEXT,       -- e.g. 'spo2_low,heart_rate_high'
    lab_risk_score       INTEGER,
    lab_sim_day          INTEGER,    -- which day's labs were used (latest available)
    total_risk_score     INTEGER NOT NULL,
    risk_category        TEXT    NOT NULL,   -- NORMAL / WATCH / CONCERNING
    lab_status           TEXT    NOT NULL,   -- AVAILABLE / LAB_UNAVAILABLE
    abnormal_labs        TEXT,       -- e.g. 'potassium:HIGH,crp:HIGH'
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, patient_id),
    CHECK ((lab_status = 'LAB_UNAVAILABLE') = (lab_risk_score IS NULL))
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
