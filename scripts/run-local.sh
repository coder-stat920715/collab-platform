#!/usr/bin/env bash
# Starts Redis, then the services in the background. Requires Docker, JDK 21 and Maven 3.9+.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d redis
mvn -q -DskipTests install
for m in realtime-collaboration-service snapshot-persistence-service document-service notification-service api-gateway; do
  (cd "$m" && mvn -q spring-boot:run > "../logs-$m.txt" 2>&1 &)
done
echo "Open http://localhost:8081/index.html?doc=demo&user=alice and again with &user=bob"
