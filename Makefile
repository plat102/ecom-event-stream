COMPOSE := docker compose --env-file .env \
	-f infrastructure/docker/docker-compose.kafka.yml \
	-f infrastructure/docker/docker-compose.db.yml \
	-f infrastructure/docker/docker-compose.dashboard.yml

COMPOSE_AIRFLOW := docker compose --env-file .env \
	-f infrastructure/docker/docker-compose.airflow.yml

# Read from .env so make and compose agree. Lazy `=`, so `make help` on a clone without a
# .env stays quiet; last value wins and surrounding quotes go, as compose's dotenv does.
AIRFLOW_META_PW = $(shell sed -n 's/^AIRFLOW_METADATA_PASSWORD=//p' .env 2>/dev/null \
	| tail -1 | sed -e 's/^"\(.*\)"$$/\1/' -e "s/^'\(.*\)'$$/\1/")

HADOOP_CONTAINERS := hadoop-namenode-1 hadoop-datanode1-1 \
	hadoop-resourcemanager-1 hadoop-nodemanager1-1

# Kept on one line: a backslash-newline inside a make variable collapses to a space,
# which would corrupt the comma-separated coordinate list.
SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.3

PSQL := docker exec -i postgresql psql -U $${POSTGRES_USER:-ecom} -d $${POSTGRES_DB:-ecom_analytics}

# The two run modes keep separate checkpoints, so Spark's own concurrent-query guard never
# fires between them — this lock is what keeps only one processing job alive.
STREAM_LOCK := .stream-job.lock
STREAM_BUSY := "A processing job already holds $(STREAM_LOCK) — only one may run at a time"

.PHONY: up down ps logs topics test psql views \
	yarn-up yarn-down yarn-ps hdfs-init run-local run-yarn smoke-yarn \
	airflow-build airflow-db airflow-init airflow-up airflow-down airflow-logs \
	airflow-ps airflow-cli airflow-seed airflow-test

up: ## Start infrastructure (Kafka cluster + MongoDB + PostgreSQL)
	$(COMPOSE) up -d

down: ## Stop infrastructure
	$(COMPOSE) down

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Tail logs from all containers
	$(COMPOSE) logs -f

topics: ## Create Kafka topics with correct partition counts
	docker exec -i kafka-0 bash < infrastructure/docker/kafka/create_topics.sh

test: ## Run all unit tests (ingestion + processing)
	poetry run pytest apps/ -v

psql: ## Open psql shell into the analytics warehouse
	docker exec -it postgresql psql -U $${POSTGRES_USER:-ecom} -d $${POSTGRES_DB:-ecom_analytics}

views: ## (Re)create the reporting views — safe to re-run, it drops them first
	$(PSQL) < infrastructure/docker/init-scripts/create_views.sql

yarn-up: ## Start the existing Hadoop/YARN cluster containers
	docker start $(HADOOP_CONTAINERS)

yarn-down: ## Stop the Hadoop/YARN cluster containers
	docker stop $(HADOOP_CONTAINERS)

yarn-ps: ## Show Hadoop/YARN container status
	docker ps -a --filter name=hadoop- --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

hdfs-init: ## Create the HDFS home directory Spark stages into (run once)
	docker exec -i hadoop-namenode-1 bash -c \
		'hdfs dfs -mkdir -p /user/spark && hdfs dfs -chown -R spark:spark /user/spark && hdfs dfs -ls /user/'

run-local: ## Run the processing job in local mode (exclusive)
	@TZ=UTC flock -n -E 99 $(STREAM_LOCK) poetry run spark-submit \
		--master "local[*]" \
		--packages "$(SPARK_PACKAGES)" \
		--conf spark.sql.session.timeZone=UTC \
		apps/processing/src/main.py; \
	s=$$?; [ $$s -eq 99 ] && { echo "$(STREAM_BUSY)" >&2; exit 1; }; exit $$s

run-yarn: ## Run the processing job on YARN (client deploy-mode, exclusive)
	@flock -n -E 99 $(STREAM_LOCK) scripts/submit_yarn.sh; \
	s=$$?; [ $$s -eq 99 ] && { echo "$(STREAM_BUSY)" >&2; exit 1; }; exit $$s

smoke-yarn: ## Run the connectivity smoke test on YARN
	APP=apps/processing/src/smoke_test.py scripts/submit_yarn.sh

airflow-build: ## Build the Airflow image
	$(COMPOSE_AIRFLOW) build

airflow-db: ## Create the airflow role + metadata database on the warehouse instance (run once)
	@test -n "$(AIRFLOW_META_PW)" || { echo "AIRFLOW_METADATA_PASSWORD is not set in .env" >&2; exit 1; }
	@$(PSQL) -tAc "SELECT 1 FROM pg_roles WHERE rolname='airflow'" | grep -q 1 || \
		$(PSQL) -c "CREATE ROLE airflow LOGIN"
	@$(PSQL) -c "ALTER ROLE airflow LOGIN PASSWORD '$(AIRFLOW_META_PW)'" >/dev/null
	@$(PSQL) -tAc "SELECT 1 FROM pg_database WHERE datname='airflow'" | grep -q 1 || \
		$(PSQL) -c "CREATE DATABASE airflow OWNER airflow"
	@echo "airflow role + database ready"

airflow-init: ## Apply metadata migrations and create the admin user (idempotent)
	@mkdir -p apps/orchestration/logs
	$(COMPOSE_AIRFLOW) run --rm airflow-init

airflow-up: ## Start Airflow (webserver on http://localhost:18080 + scheduler)
	@mkdir -p apps/orchestration/logs
	$(COMPOSE_AIRFLOW) up -d

airflow-down: ## Stop Airflow
	$(COMPOSE_AIRFLOW) down

airflow-logs: ## Tail the Airflow container logs
	$(COMPOSE_AIRFLOW) logs -f

airflow-ps: ## Show Airflow container status
	$(COMPOSE_AIRFLOW) ps

airflow-seed: ## Import DAG connections + variables from apps/orchestration/config
	scripts/seed_airflow.sh

airflow-test: ## Run the orchestration tests inside the Airflow image (they import airflow)
	$(COMPOSE_AIRFLOW) exec -T -e PYTHONPATH=/opt/repo:/opt/repo/apps/orchestration:/opt/airflow/dags airflow-scheduler \
		pytest /opt/repo/apps/orchestration/tests -q -p no:cacheprovider

airflow-cli: ## Run an airflow CLI command, e.g. make airflow-cli ARGS="dags list"
	$(COMPOSE_AIRFLOW) exec airflow-scheduler airflow $(ARGS)
