#!/usr/bin/env python3
"""
Reference solution:
Bootstrap relational IoT context data into PostgreSQL from Kafka.

This script:
1) Creates the relational schema (iot_context) in PostgreSQL
2) Consumes a Kafka topic
3) Filters the JSON message with "info": true
4) Inserts contextual IoT data (homes, devices, generators, batteries, shadows)

This script is meant as a reference solution for the course.
"""

import os
import json
import sys
import psycopg2
from psycopg2 import sql
from kafka import KafkaConsumer

# ---------------------------------------------------------------------
# Configuration (environment variables)
# ---------------------------------------------------------------------

PGSQL_SERVER = os.getenv("PGSQL_SERVER", "localhost")
PGSQL_PORT = os.getenv("PGSQL_PORT", "5432")
PGSQL_DB = os.getenv("PGSQL_DB")
PGSQL_USER = os.getenv("PGSQL_USER")
PGSQL_PASSWORD = os.getenv("PGSQL_PASSWORD")

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "")
KAFKA_FULL_TOPIC = f"{KAFKA_TOPIC_PREFIX}{KAFKA_TOPIC}"

# ---------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------

def get_pg_connection():
    """
    Connect to PostgreSQL using psycopg2.
    psycopg2 is the most widely used PostgreSQL adapter for Python.
    """
    return psycopg2.connect(
        host=PGSQL_SERVER,
        port=PGSQL_PORT,
        dbname=PGSQL_DB,
        user=PGSQL_USER,
        password=PGSQL_PASSWORD
    )


def schema_exists(cur, schema_name):
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = %s
        );
    """, (schema_name,))
    return cur.fetchone()[0]


def drop_schema(cur, schema_name):
    cur.execute(sql.SQL("DROP SCHEMA {} CASCADE;").format(
        sql.Identifier(schema_name)
    ))


def create_schema_and_tables(cur):
    """
    Create schema and relational tables.
    """
    cur.execute("CREATE SCHEMA iot_context;")
    cur.execute("SET search_path TO iot_context;")

    # Tariffs (mandatory additional table)
    cur.execute("""
        CREATE TABLE tariffs (
            tariff_id SERIAL PRIMARY KEY,
            tariff_name TEXT NOT NULL UNIQUE,
            price_per_kwh FLOAT NOT NULL,
            description TEXT
        );
    """)

    # Homes
    cur.execute("""
        CREATE TABLE home (
            home_id TEXT PRIMARY KEY,
            tariff_id INTEGER NOT NULL REFERENCES tariffs(tariff_id)
        );
    """)

    # Devices (loads)
    cur.execute("""
        CREATE TABLE device (
            device_id TEXT PRIMARY KEY,
            home_id TEXT NOT NULL REFERENCES home(home_id),
            device_type TEXT NOT NULL,
            max_power FLOAT NOT NULL
        );
    """)

    # Generator (0 or 1 per home)
    cur.execute("""
        CREATE TABLE generator (
            generator_id TEXT PRIMARY KEY,
            home_id TEXT UNIQUE NOT NULL REFERENCES home(home_id),
            max_power FLOAT NOT NULL
        );
    """)

    # Battery (0 or 1 per home, only if generator exists)
    cur.execute("""
        CREATE TABLE battery (
            battery_id TEXT PRIMARY KEY,
            home_id TEXT UNIQUE NOT NULL REFERENCES home(home_id),
            capacity FLOAT NOT NULL
        );
    """)

    # Additional entity: shades affecting generator
    cur.execute("""
        CREATE TABLE generator_shade (
            shade_id SERIAL PRIMARY KEY,
            generator_id TEXT NOT NULL REFERENCES generator(generator_id) ON DELETE CASCADE,
            shade_index INTEGER NOT NULL,
            shade_label TEXT NOT NULL,
            UNIQUE(generator_id, shade_index)
        );
    """)


    # Abnormal Event
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            anomaly_id SERIAL PRIMARY KEY,
            home_id TEXT NOT NULL REFERENCES home(home_id) ON DELETE CASCADE,
            device_id TEXT REFERENCES device(device_id) ON DELETE SET NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            anomaly_type TEXT NOT NULL,
            description TEXT,
            severity INT CHECK (severity BETWEEN 1 AND 5),
            resolved BOOLEAN DEFAULT FALSE
        );
    """)

    # Energy Bills
    cur.execute("""
         CREATE TABLE IF NOT EXISTS energy_bills (
             bill_id SERIAL PRIMARY KEY,
             home_id TEXT NOT NULL REFERENCES home(home_id) ON DELETE CASCADE,
             tariff_id INT NOT NULL REFERENCES tariffs(tariff_id) ON DELETE RESTRICT,
             billing_period_start DATE NOT NULL,
             billing_period_end DATE NOT NULL,
             total_consumption_kwh FLOAT NOT NULL,
             total_cost FLOAT NOT NULL,
             generation_kwh FLOAT,
             grid_import_kwh FLOAT,
             grid_export_kwh FLOAT,
             CHECK (billing_period_start < billing_period_end)
         );
     """)


# ---------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------

def get_kafka_consumer():
    return KafkaConsumer(
        KAFKA_FULL_TOPIC,
        bootstrap_servers=f"{KAFKA_SERVER}:{KAFKA_PORT}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )


# ---------------------------------------------------------------------
# JSON validation
# ---------------------------------------------------------------------

def is_valid_info_message(msg):
    """
    Minimal validation:
    - Must be a dict
    - Must contain "info": true
    - Must contain "hogares"
    """
    return (
        isinstance(msg, dict) and
        msg.get("info") is True and
        "hogares" in msg
    )


# ---------------------------------------------------------------------
# Data insertion
# ---------------------------------------------------------------------

def insert_data(cur, data):
    cur.execute("SET search_path TO iot_context;")

    # Insert mandatory tariffs first
    tariffs = [
        ("flat_rate", 0.18, "Fixed price all day"),
        ("off_peak", 0.12, "Cheaper tariff for low-demand hours"),
        ("peak", 0.25, "Higher tariff for peak-demand hours")
    ]

    tariff_ids = {}
    for name, price, description in tariffs:
        cur.execute("""
            INSERT INTO tariffs (tariff_name, price_per_kwh, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (tariff_name) DO NOTHING;
        """, (name, price, description))

    # Read back tariff ids
    cur.execute("SELECT tariff_id, tariff_name FROM tariffs;")
    for tariff_id, tariff_name in cur.fetchall():
        tariff_ids[tariff_name] = tariff_id

    tariff_cycle = ["flat_rate", "off_peak", "peak"]

    for idx, hogar in enumerate(data["hogares"]):
        home_id = hogar["hogar"]

        # Deterministic tariff assignment
        assigned_tariff_name = tariff_cycle[idx % len(tariff_cycle)]
        assigned_tariff_id = tariff_ids[assigned_tariff_name]

        cur.execute("""
            INSERT INTO home (home_id, tariff_id)
            VALUES (%s, %s)
            ON CONFLICT (home_id) DO NOTHING;
        """, (home_id, assigned_tariff_id))

        # Loads / devices
        for carga in hogar.get("cargas", []):
            cur.execute("""
                INSERT INTO device (device_id, home_id, device_type, max_power)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (device_id) DO NOTHING;
            """, (
                carga["id"],
                home_id,
                carga["tipo"],
                carga["pot_max"]
            ))

        bateria = hogar.get("bateria", {})
        generador = hogar.get("generador", {})

        if generador.get("presente"):
            generator_id = generador["id"]

            cur.execute("""
                INSERT INTO generator (generator_id, home_id, max_power)
                VALUES (%s, %s, %s)
                ON CONFLICT (generator_id) DO NOTHING;
            """, (
                generator_id,
                home_id,
                generador["pot_max"]
            ))

            if bateria.get("presente"):
                cur.execute("""
                    INSERT INTO battery (battery_id, home_id, capacity)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (battery_id) DO NOTHING;
                """, (
                    bateria["id"],
                    home_id,
                    bateria["capacidad"]
                ))

            # Extra entity: generator shades
            sombras = hogar.get("sombras", [])
            for shade_idx, sombra in enumerate(sombras, start=1):
                if isinstance(sombra, dict):
                    shade_label = sombra.get("id") or sombra.get("tipo") or json.dumps(sombra, ensure_ascii=False)
                else:
                    shade_label = str(sombra)

                cur.execute("""
                    INSERT INTO generator_shade (generator_id, shade_index, shade_label)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (generator_id, shade_index) DO NOTHING;
                """, (
                    generator_id,
                    shade_idx,
                    shade_label
                ))

    # -----------------------------------------------------------------
    # Mock data for additional tables
    # -----------------------------------------------------------------
    import random
    from datetime import datetime, timedelta

    # Homes with their actual tariffs
    cur.execute("SELECT home_id, tariff_id FROM home;")
    homes_with_tariffs = cur.fetchall()
    home_ids = [home_id for home_id, _ in homes_with_tariffs]
    home_tariff_map = {home_id: tariff_id for home_id, tariff_id in homes_with_tariffs}

    # Tariff prices
    cur.execute("SELECT tariff_id, price_per_kwh FROM tariffs;")
    tariff_price_map = {tariff_id: price for tariff_id, price in cur.fetchall()}

    # Devices grouped by home
    cur.execute("SELECT home_id, device_id FROM device;")
    devices_by_home = {}
    for home_id, device_id in cur.fetchall():
        devices_by_home.setdefault(home_id, []).append(device_id)

    # Homes with generator
    cur.execute("SELECT home_id FROM generator;")
    homes_with_generators = {row[0] for row in cur.fetchall()}

    # -------------------------------------------------------------
    # Insert mock anomalies
    # -------------------------------------------------------------
    anomaly_types = [
        "power_spike",
        "low_generation",
        "battery_drain",
        "device_failure",
        "voltage_drop"
    ]

    for _ in range(50):
        home_id = random.choice(home_ids)

        # Choose device only from the same home
        home_devices = devices_by_home.get(home_id, [])
        device_id = random.choice(home_devices) if home_devices and random.random() < 0.7 else None

        anomaly_type = random.choice(anomaly_types)
        severity = random.randint(1, 5)  # matches INT CHECK (severity BETWEEN 1 AND 5)
        timestamp = datetime.now() - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        description = f"Detected {anomaly_type} with severity {severity}"
        resolved = random.choice([True, False])

        cur.execute("""
            INSERT INTO anomalies
                (home_id, device_id, timestamp, anomaly_type, description, severity, resolved)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            home_id,
            device_id,
            timestamp,
            anomaly_type,
            description,
            severity,
            resolved
        ))

    # -------------------------------------------------------------
    # Insert mock energy bills
    # -------------------------------------------------------------
    today = datetime.now().date()

    for home_id in home_ids:
        tariff_id = home_tariff_map[home_id]
        price_per_kwh = tariff_price_map[tariff_id]
        has_generator = home_id in homes_with_generators

        for month_offset in range(6):
            billing_period_end = today - timedelta(days=month_offset * 30)
            billing_period_start = billing_period_end - timedelta(days=30)

            total_consumption_kwh = round(random.uniform(150, 600), 2)
            generation_kwh = round(random.uniform(50, 250), 2) if has_generator else 0.0
            grid_import_kwh = round(max(total_consumption_kwh - generation_kwh, 0), 2)
            grid_export_kwh = round(random.uniform(0, 50), 2) if has_generator else 0.0
            total_cost = round(grid_import_kwh * price_per_kwh, 2)

            cur.execute("""
                INSERT INTO energy_bills
                    (home_id, tariff_id, billing_period_start, billing_period_end,
                     total_consumption_kwh, total_cost, generation_kwh,
                     grid_import_kwh, grid_export_kwh)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                home_id,
                tariff_id,
                billing_period_start,
                billing_period_end,
                total_consumption_kwh,
                total_cost,
                generation_kwh,
                grid_import_kwh,
                grid_export_kwh
            ))




# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    print("Connecting to PostgreSQL...")
    conn = get_pg_connection()
    conn.autocommit = False
    cur = conn.cursor()

    if schema_exists(cur, "iot_context"):
        print("Schema 'iot_context' already exists.")
        choice = input("Do you want to drop and recreate it? [y/N]: ").strip().lower()
        if choice == "y":
            print("Dropping schema...")
            drop_schema(cur, "iot_context")
            conn.commit()
        else:
            print("Keeping existing schema.")

    if not schema_exists(cur, "iot_context"):
        print("Creating schema and tables...")
        create_schema_and_tables(cur)
        conn.commit()

    print("Connecting to Kafka...")
    consumer = get_kafka_consumer()

    print("Waiting for configuration message (info=true)...")
    for msg in consumer:
        data = msg.value
        if is_valid_info_message(data):
            print("Valid configuration message received.")
            insert_data(cur, data)
            conn.commit()
            print("Data successfully inserted into PostgreSQL.")
            break
        else:
            print("Ignoring non-configuration message.")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
