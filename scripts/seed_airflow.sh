#!/usr/bin/env bash
# Resolve the connection template from .env, then import connections + variables.
# Idempotent: re-running overwrites in place.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIG_DIR="apps/orchestration/config"
TEMPLATE="$CONFIG_DIR/connections.example.json"
RESOLVED="$CONFIG_DIR/connections.json"
VARIABLES="$CONFIG_DIR/variables.json"
COMPOSE=(docker compose --env-file .env -f infrastructure/docker/docker-compose.airflow.yml)

set -a
# shellcheck disable=SC1091
source .env
set +a

REQUIRED=(
  POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD
  SINK_KAFKA_SASL_USERNAME SINK_KAFKA_SASL_PASSWORD
  SOURCE_KAFKA_BROKERS SOURCE_KAFKA_SECURITY_PROTOCOL
  SOURCE_KAFKA_SASL_MECHANISM SOURCE_KAFKA_SASL_USERNAME SOURCE_KAFKA_SASL_PASSWORD
  DISCORD_WEBHOOK_URL
)
missing=()
for var in "${REQUIRED[@]}"; do
  [[ -n "${!var:-}" ]] || missing+=("$var")
done
if (( ${#missing[@]} )); then
  # An empty value here would import a connection that only fails at task runtime.
  echo "Empty or unset in .env: ${missing[*]}" >&2
  exit 1
fi

# Discord hands out one URL; the provider wants it split and validates the endpoint shape.
if [[ "$DISCORD_WEBHOOK_URL" != */webhooks/*/* ]]; then
  echo "DISCORD_WEBHOOK_URL does not look like .../webhooks/<id>/<token>: $DISCORD_WEBHOOK_URL" >&2
  exit 1
fi
DISCORD_API_BASE="${DISCORD_WEBHOOK_URL%%/webhooks/*}/"
DISCORD_WEBHOOK_ENDPOINT="webhooks/${DISCORD_WEBHOOK_URL#*/webhooks/}"
export DISCORD_API_BASE DISCORD_WEBHOOK_ENDPOINT

# Only the listed names are substituted, so a literal $ in a password survives.
placeholders=$(printf '${%s} ' "${REQUIRED[@]}" DISCORD_API_BASE DISCORD_WEBHOOK_ENDPOINT)
envsubst "$placeholders" < "$TEMPLATE" > "$RESOLVED"

# envsubst does not escape: a value carrying " or \ produces invalid JSON, and the import
# would then blame the generated file rather than the .env value behind it.
python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$RESOLVED" || {
  echo "$RESOLVED is not valid JSON — check .env for a value containing a quote or backslash" >&2
  exit 1
}

"${COMPOSE[@]}" exec -T airflow-scheduler \
  airflow connections import --overwrite /opt/airflow/config/connections.json
"${COMPOSE[@]}" exec -T airflow-scheduler \
  airflow variables import --action-on-existing-key overwrite /opt/airflow/config/variables.json

echo "Seeded $(python3 -c "import json,sys;print(len(json.load(open('$RESOLVED'))))") connections and \
$(python3 -c "import json,sys;print(len(json.load(open('$VARIABLES'))))") variables"
