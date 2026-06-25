#!/bin/bash
# run_all_partitions.sh
# Runs etl-tool for every partition of a given pipeline config.
# Usage: bash run_all_partitions.sh <config_yaml> <partition1> [partition2] ...

set -e

CONFIG=$1
shift
PARTITIONS=("$@")

# Resolve paths — works both in prod image (/app) and dev image (/build)
if [ -f "/app/etl-tool.jar" ]; then
    JAR="/app/etl-tool.jar"
    CERT="/app/yandex_s3.crt"
    CONFIGS_DIR="/app/configs"
    RUN_ETL="/app/run_etl.sh"
else
    JAR="/build/etl-tool/target/scala-3.4.2/etl-tool.jar"
    CERT="/build/etl-tool/yandex_s3.crt"
    CONFIGS_DIR="/build/etl-tool/configs"
    RUN_ETL="./run_etl.sh"
fi

echo "[INFO] Config: $CONFIG"
echo "[INFO] JAR: $JAR"
echo "[INFO] Partitions: ${#PARTITIONS[@]} total"
echo ""

# Import S3 cert once
$JAVA_HOME/bin/keytool -import -noprompt -trustcacerts \
  -alias yandex_s3 -file "$CERT" \
  -cacerts -storepass changeit 2>/dev/null && echo "[INFO] S3 cert imported" || true

OK=0
FAIL=0
for part in "${PARTITIONS[@]}"; do
    echo "[$(date '+%H:%M:%S')] Partition: $part"
    set +e
    $RUN_ETL \
      --config "$CONFIGS_DIR/pipelines/$CONFIG" \
      --partition "$part" \
      --run_id "manual_${part}"
    EXIT=$?
    set -e
    if [ $EXIT -eq 0 ]; then
      echo "[OK] $part"
      OK=$((OK+1))
    else
      echo "[FAIL] $part (exit=$EXIT)"
      FAIL=$((FAIL+1))
    fi
done

echo ""
echo "=== Done: $OK OK, $FAIL FAILED ==="
