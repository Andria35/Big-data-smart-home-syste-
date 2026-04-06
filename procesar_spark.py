#!/usr/bin/env python3

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
df_processed = df_parsed \
    .withColumn("power", col("V") * col("I")) \
    .withColumn("energy_kwh", col("E") / 1000.0)

# -----------------------------
# Write to InfluxDB
# -----------------------------
def write_to_influx(batch_df, batch_id):
    url = "http://localhost:8086"
    token = os.environ["INFLUX_TOKEN"]
    org = os.environ["INFLUX_ORG"]
    bucket = os.environ["INFLUX_BUCKET"]

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    rows = batch_df.collect()

    for row in rows:
        try:
            point = Point("electricidad") \
                .tag("hogar", str(row["hogar"])) \
                .tag("tipo_disp", str(row["tipo_disp"])) \
                .tag("id_disp", str(row["id_disp"])) \
                .field("energy_wh", float(row["E"]) if row["E"] is not None else 0.0) \
                .field("energy_kwh", float(row["energy_kwh"]) if row["energy_kwh"] is not None else 0.0) \
                .field("power_calc", float(row["power"]) if row["power"] is not None else 0.0) \
                .field("Pmax", float(row["Pmax"]) if row["Pmax"] is not None else 0.0) \
                .field("voltage", float(row["V"]) if row["V"] is not None else 0.0) \
                .field("current", float(row["I"]) if row["I"] is not None else 0.0) \
                .time(int(row["tm"]), WritePrecision.MS)

            write_api.write(bucket=bucket, record=point)

            # Print for debug / verification
            v = row["V"] if row["V"] is not None else 0.0
            i = row["I"] if row["I"] is not None else 0.0
            p = v * i
            print(f"{row['hogar']} | {row['id_disp']} | P={p:.2f} W")

        except Exception as e:
            print(f"Error writing row: {e}")

    client.close()

# -----------------------------
# Streaming query
# -----------------------------
query = df_processed.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("append") \
    .start()

query.awaitTermination()