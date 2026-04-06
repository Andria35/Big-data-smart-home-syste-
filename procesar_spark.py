from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------------
# Initialize Spark session
# -----------------------------
spark = SparkSession.builder \
    .appName("EnergyProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Kafka configuration
# -----------------------------
bootstrap_servers = f'{os.environ["KAFKA_SERVER"]}:{os.environ["KAFKA_PORT"]}'
topic_name = os.environ["KAFKA_TOPIC"]
topic_prefix = os.environ.get("KAFKA_TOPIC_PREFIX", "")
full_topic = f"{topic_prefix}{topic_name}"

print(f"Kafka bootstrap servers: {bootstrap_servers}")
print(f"Kafka topic: {full_topic}")

# -----------------------------
# Read stream from Kafka
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", full_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.group.id", "spark-energy-consumer") \
    .load()

# -----------------------------
# Schema
# -----------------------------
schema = StructType([
    StructField("hogar", StringType()),
    StructField("tipo_disp", StringType()),
    StructField("id_disp", StringType()),
    StructField("E", DoubleType()),
    StructField("Pmax", DoubleType()),
    StructField("V", DoubleType()),
    StructField("I", DoubleType()),
    StructField("tm", LongType())
])

# -----------------------------
# Parse JSON
# -----------------------------
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .withColumn("value", from_json("value", schema)) \
    .select("value.*") \
    .filter(col("hogar").isNotNull())

# -----------------------------
# Processing logic
# -----------------------------
# Keep old fields/logic, but make it safer and add a few extra derived metrics
df_processed = df_parsed \
    .filter(col("tm").isNotNull()) \
    .filter(col("id_disp").isNotNull()) \
    .withColumn("E_safe", coalesce(col("E"), lit(0.0))) \
    .withColumn("V_safe", coalesce(col("V"), lit(0.0))) \
    .withColumn("I_safe", coalesce(col("I"), lit(0.0))) \
    .withColumn("Pmax_safe", coalesce(col("Pmax"), lit(0.0))) \
    .withColumn("power", col("V_safe") * col("I_safe")) \
    .withColumn("energy_kwh", col("E_safe") / 1000.0) \
    .withColumn("abs_power", abs(col("power"))) \
    .withColumn("energy_abs_kwh", abs(col("energy_kwh"))) \
    .withColumn("has_measurements", when((col("V").isNotNull()) & (col("I").isNotNull()), lit(1)).otherwise(lit(0))) \
    .withColumn("low_voltage_flag", when((col("V").isNotNull()) & (col("V") < 210), lit(1)).otherwise(lit(0))) \
    .withColumn("high_power_flag", when(abs(col("power")) > 3000, lit(1)).otherwise(lit(0)))

# -----------------------------
# Write to InfluxDB
# -----------------------------
def write_to_influx(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty batch, skipping.")
        return

    print(f"Processing batch {batch_id}...")

    url = "http://localhost:8086"
    token = os.environ["INFLUX_TOKEN"]
    org = os.environ["INFLUX_ORG"]
    bucket = os.environ["INFLUX_BUCKET"]

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    rows = batch_df.collect()
    print(f"Batch {batch_id}: {len(rows)} rows")

    for row in rows:
        try:
            hogar = str(row["hogar"]) if row["hogar"] is not None else "unknown"
            tipo_disp = str(row["tipo_disp"]) if row["tipo_disp"] is not None else "unknown"
            id_disp = str(row["id_disp"]) if row["id_disp"] is not None else "unknown"

            energy_wh = float(row["E_safe"]) if row["E_safe"] is not None else 0.0
            energy_kwh = float(row["energy_kwh"]) if row["energy_kwh"] is not None else 0.0
            power_calc = float(row["power"]) if row["power"] is not None else 0.0
            pmax = float(row["Pmax_safe"]) if row["Pmax_safe"] is not None else 0.0
            voltage = float(row["V_safe"]) if row["V_safe"] is not None else 0.0
            current = float(row["I_safe"]) if row["I_safe"] is not None else 0.0

            # Extra fields, safe to add without breaking existing Grafana panels
            abs_power = float(row["abs_power"]) if row["abs_power"] is not None else 0.0
            energy_abs_kwh = float(row["energy_abs_kwh"]) if row["energy_abs_kwh"] is not None else 0.0
            has_measurements = int(row["has_measurements"]) if row["has_measurements"] is not None else 0
            low_voltage_flag = int(row["low_voltage_flag"]) if row["low_voltage_flag"] is not None else 0
            high_power_flag = int(row["high_power_flag"]) if row["high_power_flag"] is not None else 0

            point = Point("electricidad") \
                .tag("hogar", hogar) \
                .tag("tipo_disp", tipo_disp) \
                .tag("id_disp", id_disp) \
                .field("energy_wh", energy_wh) \
                .field("energy_kwh", energy_kwh) \
                .field("power_calc", power_calc) \
                .field("Pmax", pmax) \
                .field("voltage", voltage) \
                .field("current", current) \
                .field("abs_power", abs_power) \
                .field("energy_abs_kwh", energy_abs_kwh) \
                .field("has_measurements", has_measurements) \
                .field("low_voltage_flag", low_voltage_flag) \
                .field("high_power_flag", high_power_flag) \
                .time(int(row["tm"]), WritePrecision.MS)

            write_api.write(bucket=bucket, record=point)

            print(
                f"{hogar} | {id_disp} | "
                f"P={power_calc:.2f} W | "
                f"E={energy_kwh:.3f} kWh | "
                f"LV={low_voltage_flag} | HP={high_power_flag}"
            )

        except Exception as e:
            print(f"Error writing row in batch {batch_id}: {e}")

    client.close()
    print(f"Batch {batch_id}: write completed.")

# -----------------------------
# Streaming query
# -----------------------------
query = df_processed.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("append") \
    .start()

query.awaitTermination()