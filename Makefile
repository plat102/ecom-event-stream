COMPOSE := docker compose --env-file .env \
	-f infrastructure/docker/docker-compose.kafka.yml \
	-f infrastructure/docker/docker-compose.db.yml

.PHONY: up down ps logs topics test psql

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
