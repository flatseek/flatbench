# Flatbench Makefile - Benchmark infrastructure management

.PHONY: help up down clean status logs es-create es-delete es-stats es-health

# Docker compose file
DC = $(word 1,$(wildcard docker-compose.yml docker-compose.yaml))
ifneq (,$(DC))
DC_CMD = docker-compose -f $(DC)
else
DC_CMD = docker-compose
endif

ES_HOST ?= http://localhost:9200
ES_INDEX ?= benchmark

## help - Show this help message
help:
	@echo "Flatbench Infrastructure"
	@echo "======================="
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up          Start all services (docker-compose up -d)"
	@echo "  make down        Stop all services (keep volumes)"
	@echo "  make clean       Stop and remove volumes"
	@echo "  make status      Show service status"
	@echo "  make logs        Show logs (all services)"
	@echo ""
	@echo "Elasticsearch:"
	@echo "  make es-create   Create benchmark index"
	@echo "  make es-delete   Delete benchmark index"
	@echo "  make es-stats    Show cluster stats"
	@echo "  make es-health   Show cluster health"
	@echo ""
	@echo "Benchmark:"
	@echo "  make benchmark   Run comparison benchmark (flatseek, es, sqlite)"
	@echo ""
	@echo "Configuration:"
	@echo "  ES_HOST=$(ES_HOST)"
	@echo "  ES_INDEX=$(ES_INDEX)"

## up - Start all services
up:
	@echo "Starting services..."
	$(DC_CMD) up -d
	@echo ""
	@echo "Waiting for Elasticsearch to be ready..."
	@for i in $$(seq 1 30); do \
		curl -sf $(ES_HOST) > /dev/null 2>&1 && break || { \
			echo -n "."; \
			sleep 2; \
		}; \
	done
	@echo ""
	@echo "Ready! Services:"
	@echo "  Elasticsearch: $(ES_HOST)"
	@echo "  Kibana:        http://localhost:5601"

## down - Stop all services
down:
	@echo "Stopping services..."
	$(DC_CMD) down

## clean - Stop and remove volumes
clean:
	@echo "Stopping services and removing volumes..."
	$(DC_CMD) down -v

## status - Show service status
status:
	@echo "Container status:"
	$(DC_CMD) ps
	@echo ""
	@echo "Elasticsearch health:"
	@curl -sf $(ES_HOST)/_cluster/health 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "Elasticsearch not responding"

## logs - Show logs
logs:
	$(DC_CMD) logs -f --tail=100

## es-create - Create benchmark index
es-create:
	@echo "Creating index '$(ES_INDEX)'..."
	@curl -s -X PUT "$(ES_HOST)/$(ES_INDEX)" -H 'Content-Type: application/json' \
		-d '{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"id":{"type":"keyword"},"name":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"email":{"type":"keyword"},"phone":{"type":"keyword"},"city":{"type":"keyword"},"country":{"type":"keyword"},"status":{"type":"keyword"},"balance":{"type":"float"},"created_at":{"type":"date"},"updated_at":{"type":"date"},"is_verified":{"type":"boolean"},"tags":{"type":"keyword"}}}}'
	@echo ""

## es-delete - Delete benchmark index
es-delete:
	@echo "Deleting index '$(ES_INDEX)'..."
	@curl -s -X DELETE "$(ES_HOST)/$(ES_INDEX)"
	@echo ""

## es-stats - Show cluster stats
es-stats:
	@echo "Cluster stats:"
	@curl -sf $(ES_HOST)/_cluster/stats 2>/dev/null | python3 -m json.tool | head -60

## es-health - Show cluster health
es-health:
	@curl -sf $(ES_HOST)/_cluster/health 2>/dev/null | python3 -m json.tool

FLATSEEK_PYTHON = python

## benchmark - Run benchmark (requires services up)
benchmark:
	@echo "Running benchmark (flatseek, elasticsearch, sqlite)..."
	@$(FLATSEEK_PYTHON) -m flatbench compare \
			--engines flatseek,elasticsearch,sqlite \
			--sizes 1000 10000 100000