.DEFAULT_GOAL := help

.PHONY: help demo stop kafka test backend-test web-test repository-traffic repository-traffic-test build java-check

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Foundry Stream Lab\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

demo: ## Build and run the complete simulated lab
	docker compose up --build

stop: ## Stop the lab and remove ephemeral Kafka data
	docker compose down --remove-orphans

kafka: ## Start only Kafka for host-based Foundry development
	docker compose up -d broker topic-init

java-check:
	@version=$$(java -version 2>&1 | awk -F '"' '/version/ {print $$2; exit}'); \
	major=$$(printf '%s\n' "$$version" | awk -F. '{if ($$1 == "1") print $$2; else print $$1}'); \
	if [ -z "$$major" ] || [ "$$major" -lt 21 ]; then \
		echo "Java 21 or newer is required (found $${version:-unknown})." >&2; \
		exit 1; \
	fi

backend-test: java-check ## Run Java tests
	cd app && mvn -B verify

web-test: ## Run frontend lint, tests, and production build
	cd web && npm run lint && npm run test -- --run && npm run build

test: backend-test web-test ## Run all checks

repository-traffic: ## Capture views, clones, and rename attribution
	mkdir -p tmp/repository-traffic
	python3 tools/github/traffic_attribution.py \
		--repository hyeonsangjeon/foundry-stream-lab \
		--legacy-repository hyeonsangjeon/kafka-metric-example \
		--renamed-at 2026-07-17T05:39:55Z \
		--archive-directory tmp/repository-traffic/archive \
		--output-json tmp/repository-traffic/snapshot.json \
		--output-markdown tmp/repository-traffic/summary.md
	@cat tmp/repository-traffic/summary.md

repository-traffic-test: ## Run offline GitHub traffic attribution tests
	python3 -m unittest discover -s tools/github/tests -p 'test_*.py'

build: ## Build the production container
	docker build -t foundry-stream-lab:local .
