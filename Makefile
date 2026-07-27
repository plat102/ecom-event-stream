COMPOSE := docker compose --env-file .env \
	-f infrastructure/docker/docker-compose.kafka.yml \
	-f infrastructure/docker/docker-compose.db.yml \
	-f infrastructure/docker/docker-compose.dashboard.yml

HADOOP_CONTAINERS := hadoop-namenode-1 hadoop-datanode1-1 \
	hadoop-resourcemanager-1 hadoop-nodemanager1-1

# Kept on one line: a backslash-newline inside a make variable collapses to a space,
# which would corrupt the comma-separated coordinate list.
SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.3

PSQL := docker exec -i postgresql psql -U $${POSTGRES_USER:-ecom} -d $${POSTGRES_DB:-ecom_analytics}

.PHONY: up down ps logs topics test psql views \
	yarn-up yarn-down yarn-ps hdfs-init run-local run-yarn smoke-yarn

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

run-local: ## Run the processing job in local mode
	TZ=UTC poetry run spark-submit \
		--master "local[*]" \
		--packages "$(SPARK_PACKAGES)" \
		--conf spark.sql.session.timeZone=UTC \
		apps/processing/src/main.py

run-yarn: ## Run the processing job on YARN (client deploy-mode)
	scripts/submit_yarn.sh

smoke-yarn: ## Run the connectivity smoke test on YARN
	APP=apps/processing/src/smoke_test.py scripts/submit_yarn.sh
