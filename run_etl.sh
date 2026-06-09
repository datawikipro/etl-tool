#!/bin/bash
set -e

# Resolve JAR location — works both in prod image (/app) and dev image (/build)
if [ -f "/app/etl-tool.jar" ]; then
    JAR="/app/etl-tool.jar"
    CERT="/app/yandex_s3.crt"
else
    JAR="/build/etl-tool/target/scala-3.4.2/etl-tool.jar"
    CERT="/build/etl-tool/yandex_s3.crt"
fi

# Import S3 SSL cert into Java cacerts (using -cacerts flag for Corretto)
$JAVA_HOME/bin/keytool -import -noprompt -trustcacerts \
  -alias yandex_s3 \
  -file "$CERT" \
  -cacerts -storepass changeit 2>/dev/null && echo "[INFO] S3 cert imported" || echo "[INFO] S3 cert already exists, skipping"

exec java \
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  -Dfile.encoding=UTF-8 \
  -jar "$JAR" \
  "$@"
