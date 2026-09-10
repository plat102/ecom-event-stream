#!/usr/bin/env bash
#
# Submit the processing job to the Hadoop YARN cluster (client deploy-mode).
#
# Usage:
#   scripts/submit_yarn.sh                                  # run main.py
#   APP=apps/processing/src/smoke_test.py scripts/submit_yarn.sh
#
# Overridable: APP, HADOOP_CONF, NODEMANAGER.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="${APP:-apps/processing/src/main.py}"
HADOOP_CONF="${HADOOP_CONF:-$REPO/infrastructure/docker/hadoop/conf}"
NODEMANAGER="${NODEMANAGER:-hadoop-nodemanager1-1}"

IMAGE="$(docker inspect -f '{{.Config.Image}}' "$NODEMANAGER")"
SPARK_VERSION="$("$REPO/.venv/bin/python" -c 'import pyspark; print(pyspark.__version__)')"

mkdir -p "$REPO/build"
python3 -m zipfile -c "$REPO/build/deps.zip" "$REPO"/apps/processing/src/*.py "$REPO/shared" \
  "$REPO"/.venv/lib/python*/site-packages/{user_agents,ua_parser,ua_parser_builtins}

exec docker run --rm -i --name ecom-spark-yarn \
  --network=streaming-network -p 4040:4040 \
  -v "$REPO":"$REPO" -w "$REPO" \
  -v "$HADOOP_CONF":/hadoop-conf:ro \
  -v "$HOME/.ivy2":/root/.ivy2 \
  --env-file "$REPO/.env" \
  -e TZ=UTC \
  -e HADOOP_CONF_DIR=/hadoop-conf \
  -e HADOOP_USER_NAME=spark \
  -e RUN_PROFILE=yarn \
  -e CHECKPOINT_LOCATION="${CHECKPOINT_LOCATION:-hdfs://namenode/user/spark/checkpoints/ecom-stream-processor-yarn}" \
  -e PYSPARK_PYTHON=/usr/bin/python3.12 \
  -e PYSPARK_DRIVER_PYTHON="$REPO/.venv/bin/python" \
  -e SINK_KAFKA_BROKERS=kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  -e POSTGRES_HOST=postgresql \
  "$IMAGE" \
  .venv/bin/spark-submit \
    --master yarn --deploy-mode client \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},org.postgresql:postgresql:42.7.3" \
    --py-files build/deps.zip \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.yarn.am.memory=384m \
    --conf spark.executor.instances=1 --conf spark.executor.memory=1g \
    "$APP"
