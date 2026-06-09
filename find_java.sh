#!/bin/bash
echo "=== keytool ==="
ls -la $JAVA_HOME/bin/keytool 2>/dev/null || echo "no keytool in JAVA_HOME"
ls /usr/bin/keytool 2>/dev/null || echo "no /usr/bin/keytool"

echo "=== cacerts ==="
ls -la $JAVA_HOME/lib/security/cacerts 2>/dev/null || echo "no cacerts in JAVA_HOME/lib/security"
find $JAVA_HOME -name "cacerts" 2>/dev/null

echo "=== cert file ==="
ls -la /build/etl-tool/yandex_s3.crt

echo "=== import cert ==="
$JAVA_HOME/bin/keytool -import -noprompt -trustcacerts \
  -alias yandex_s3 \
  -file /build/etl-tool/yandex_s3.crt \
  -keystore $JAVA_HOME/lib/security/cacerts \
  -storepass changeit 2>&1 && echo "Import OK" || echo "Import FAILED"
