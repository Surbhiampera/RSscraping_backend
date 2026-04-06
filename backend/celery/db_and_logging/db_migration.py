import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
load_dotenv()  # loads .env file
 
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

 
# ===============================
# SCHEMA SQL
# ===============================
schema_sql = """
-- Extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
 
-- scrape_runs
CREATE TABLE IF NOT EXISTS scrape_runs (
    run_id UUID PRIMARY KEY,
    user_id UUID,

    status VARCHAR(100),

    total_rows INTEGER DEFAULT 0,
    valid_rows INTEGER DEFAULT 0,
    invalid_rows INTEGER DEFAULT 0,
    total_inputs INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    fail_count INTEGER DEFAULT 0,

    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    total_duration_ms INTEGER,

    notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- car_info
CREATE TABLE IF NOT EXISTS car_info (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    registration_number VARCHAR(50) NOT NULL,
    make_name VARCHAR(255),
    model_name VARCHAR(255),
    vehicle_variant VARCHAR(255),
    fuel_type VARCHAR(100),
    cubic_capacity INT,
    state_code VARCHAR(10),
    city_tier VARCHAR(20),
    car_age INT,
    registration_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_car_run FOREIGN KEY (run_id)
        REFERENCES scrape_runs(run_id)
        ON DELETE CASCADE
);
 
-- scrape_run_inputs
CREATE TABLE IF NOT EXISTS scrape_run_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL UNIQUE,
    car_number VARCHAR(50) NOT NULL,
    policy_expiry DATE,
    claim_status VARCHAR(100),
    phone VARCHAR(20),
    customer_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_run_input FOREIGN KEY (run_id)
        REFERENCES scrape_runs(run_id)
        ON DELETE CASCADE
);
 
-- quotes_details
CREATE TABLE IF NOT EXISTS quotes_details (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    insurer_name VARCHAR(255),
    plan_id BIGINT NOT NULL,
    plan_json JSONB NOT NULL,
    addon_combo_Id BIGINT NOT NULL,
    idv_type VARCHAR(100),
    idv_selected BIGINT, 
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_quotes_detail_run FOREIGN KEY (run_id)
        REFERENCES scrape_runs(run_id)
        ON DELETE CASCADE
);
 
-- quotes_responses
CREATE TABLE IF NOT EXISTS quotes_responses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    api_name VARCHAR(255) NOT NULL,
    api_url TEXT,
    idv_type VARCHAR(100),
    idv_selected BIGINT, 
    response_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_quotes_run FOREIGN KEY (run_id)
        REFERENCES scrape_runs(run_id)
        ON DELETE CASCADE
);
 
-- final_data
CREATE TABLE IF NOT EXISTS final_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL UNIQUE,
    final_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_final_run FOREIGN KEY (run_id)
        REFERENCES scrape_runs(run_id)
        ON DELETE CASCADE
);
 
-- final_flat_output
CREATE TABLE IF NOT EXISTS final_flat_output (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    flat_output JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_flat_run FOREIGN KEY (run_id)
        REFERENCES scrape_runs(run_id)
        ON DELETE CASCADE
);
 
-- run_logs
CREATE TABLE IF NOT EXISTS run_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES scrape_runs(run_id) ON DELETE CASCADE,
    step_number INTEGER NOT NULL,
    step_key VARCHAR(100) NOT NULL,
    status VARCHAR(20),
    start_ts TIMESTAMPTZ,
    end_ts TIMESTAMPTZ,
    duration_ms INTEGER,
    data JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
 
-- akamai_events
CREATE TABLE IF NOT EXISTS akamai_events (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES scrape_runs(run_id) ON DELETE CASCADE,
    log_id BIGINT REFERENCES run_logs(id) ON DELETE CASCADE,
    step_after INTEGER,
    step_key_after VARCHAR(100),
    event_timestamp TIMESTAMPTZ NOT NULL,
    data JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
-- data usage tracker
CREATE TABLE IF NOT EXISTS scrape_data_usage (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID         NOT NULL REFERENCES scrape_runs(run_id) ON DELETE CASCADE,
    phase           VARCHAR(50)  NOT NULL,
    category        VARCHAR(100) NOT NULL,
    call_count      INTEGER      NOT NULL DEFAULT 0,
    request_bytes   BIGINT       NOT NULL DEFAULT 0,
    response_bytes  BIGINT       NOT NULL DEFAULT 0,
    total_bytes     BIGINT       NOT NULL DEFAULT 0,
    request_size    VARCHAR(20)  NOT NULL DEFAULT '0 B',
    response_size   VARCHAR(20)  NOT NULL DEFAULT '0 B',
    total_size      VARCHAR(20)  NOT NULL DEFAULT '0 B',
    top_urls        JSONB                 DEFAULT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_data_usage_run_phase_cat UNIQUE (run_id, phase, category)
);
 
-- Alter columns if needed
ALTER TABLE scrape_run_inputs
    ALTER COLUMN policy_expiry TYPE VARCHAR(50) USING policy_expiry::text;
ALTER TABLE scrape_run_inputs
ADD CONSTRAINT unique_run_id UNIQUE (run_id);
"""
 
# ===============================
# FUNCTION TO CREATE DATABASE
# ===============================
def create_database_if_not_exists():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
            print(f"✅ Database '{DB_NAME}' created.")
        else:
            print(f"ℹ️ Database '{DB_NAME}' already exists.")
        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Error creating database:", e)
        exit(1)
 
# ===============================
# FUNCTION TO CREATE SCHEMA
# ===============================
def create_schema():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(schema_sql)
        print("✅ Database schema created successfully!")
        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Error creating schema:", e)
        exit(1)
 
# ===============================
# MAIN
# ===============================
if __name__ == "__main__":
    create_database_if_not_exists()
    create_schema()


 