#!/usr/bin/env bash
set -euo pipefail

connector_url="http://localhost:8083/connectors/commerce-postgres-cdc/config"
config_file="infra/connect/register-postgres.json"

for attempt in {1..30}; do
  if curl -fsS http://localhost:8083/connectors >/dev/null; then
    break
  fi
  if [[ "$attempt" == "30" ]]; then
    echo "Kafka Connect did not become ready." >&2
    exit 1
  fi
  sleep 2
done

payload="$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$config_file")"
curl -fsS -X PUT -H 'Content-Type: application/json' --data "$payload" "$connector_url"
echo
curl -fsS http://localhost:8083/connectors/commerce-postgres-cdc/status
echo
