#!/usr/bin/env bash
# live-producer.sh — continuously publishes realistic measurement data
# to the "contemporary-measurements-data" topic every 3-5 seconds.
# Runs until the container is stopped.

set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka-demo:9092}"
KAFKA_BIN="/opt/kafka/bin"
TOPIC="contemporary-measurements-data"

echo "==> Live producer waiting for broker..."
until "$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BROKER" >/dev/null 2>&1; do
  sleep 2
done
echo "==> Live producer connected — publishing to $TOPIC"

# Sensor pool for variety
SENSOR_IDS=("TEMP-LAB-01" "TEMP-LAB-02" "HUM-LAB-01" "PRES-LAB-01" "TEMP-FLOOR-01" "TEMP-FLOOR-02" "HUM-FLOOR-01" "CO2-FLOOR-01" "LIGHT-OFFICE-01" "VIBR-MACH-01")
SENSOR_TYPES=("temperature"  "temperature"  "humidity"    "pressure"     "temperature"    "temperature"    "humidity"      "co2"           "luminosity"      "vibration")
SENSOR_UNITS=("celsius"      "celsius"      "percent"     "hPa"          "celsius"        "celsius"        "percent"       "ppm"           "lux"             "mm_s")
SENSOR_LOCS=("lab-alpha"     "lab-alpha"    "lab-alpha"   "lab-alpha"    "floor-3"        "floor-3"        "floor-3"       "floor-3"       "office-main"     "machine-room")

# Base values and ranges for each sensor (min, range)
BASE_VALS=(21.0 22.5 42.0 1013.0 23.0 24.0 38.0 420.0 350.0 0.5)
RANGE_VALS=(3.0  2.5  10.0 8.0    4.0  3.0  12.0 180.0 200.0 4.5)

SEQ=0

while true; do
  # Pick a random sensor
  IDX=$(( RANDOM % ${#SENSOR_IDS[@]} ))
  SENSOR="${SENSOR_IDS[$IDX]}"
  TYPE="${SENSOR_TYPES[$IDX]}"
  UNIT="${SENSOR_UNITS[$IDX]}"
  LOC="${SENSOR_LOCS[$IDX]}"
  BASE="${BASE_VALS[$IDX]}"
  RANGE="${RANGE_VALS[$IDX]}"

  # Generate a value: base + random fraction of range
  # bash doesn't do floats, so we use awk
  VALUE=$(awk "BEGIN { srand($RANDOM); printf \"%.2f\", $BASE + (rand() * $RANGE) }")

  # Occasionally flag an anomaly (~10% chance)
  ANOMALY="false"
  if (( RANDOM % 10 == 0 )); then
    ANOMALY="true"
    # Spike the value
    VALUE=$(awk "BEGIN { printf \"%.2f\", $VALUE * 1.5 }")
  fi

  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  SEQ=$((SEQ + 1))

  MSG="{\"seq\":${SEQ},\"sensor_id\":\"${SENSOR}\",\"type\":\"${TYPE}\",\"value\":${VALUE},\"unit\":\"${UNIT}\",\"location\":\"${LOC}\",\"anomaly\":${ANOMALY},\"ts\":\"${TIMESTAMP}\"}"

  echo "$MSG" | "$KAFKA_BIN/kafka-console-producer.sh" \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC" \
    >/dev/null 2>&1

  echo "  [#${SEQ}] ${SENSOR} (${TYPE}): ${VALUE} ${UNIT} $([ "$ANOMALY" = "true" ] && echo '⚠ ANOMALY' || true)"

  # Sleep 3-5 seconds (random)
  DELAY=$(( 3 + RANDOM % 3 ))
  sleep "$DELAY"
done
