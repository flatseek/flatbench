# Flatbench Makefile - Benchmark infrastructure management

.PHONY: help up down clean status logs es-create es-delete es-stats es-health \
		fs-create fs-delete fs-stats fs-health ts-create ts-delete ts-stats ts-health \
		zs-create zs-delete zs-stats zs-health \
		serve build deploy deploy-preview

# Resolve the docker-compose file: use DC_FILE env if set,
# otherwise prefer one in the current directory,
# otherwise fall back to the one shipped next to this Makefile (so
# `flatbench make up` works from any working directory).
MAKEFILE_DIR := $(dir $(abspath $(firstword $(MAKEFILE_LIST))))
_DC_FALLBACK := $(firstword $(wildcard \
            docker-compose.yml \
            docker-compose.yaml \
            $(MAKEFILE_DIR)docker-compose.yml \
            $(MAKEFILE_DIR)docker-compose.yaml))
ifeq ($(DC_FILE),)
DC_FILE := $(_DC_FALLBACK)
endif
ifneq (,$(DC_FILE))
DC_CMD = docker-compose -f $(DC_FILE)
else
DC_CMD = docker-compose
endif

ES_HOST ?= http://localhost:9200
FLATSEEK_HOST ?= http://localhost:8000
TYPESENSE_HOST ?= http://localhost:8108
TYPESENSE_API_KEY ?= xyz
ZINC_HOST ?= http://localhost:4080
ZINC_USER ?= admin
ZINC_PASSWORD ?= Complexpass#123
ES_INDEX ?= benchmark
FLATSEEK_INDEX ?= benchmark
TYPESENSE_INDEX ?= benchmark
ZINC_INDEX ?= benchmark
ENGINES ?= flatseek_cli,elasticsearch,typesense,whoosh,tantivy,zincsearch
NROWS ?= 10000

## help - Show this help message
help:
	@echo "Flatbench Infrastructure"
	@echo "======================="
	@echo ""
	@echo "  Output directory: ./output (benchmark JSON/MD reports land here)"
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
	@echo "  make es-delete  Delete benchmark index"
	@echo "  make es-stats   Show cluster stats"
	@echo "  make es-health  Show cluster health"
	@echo ""
	@echo "Flatseek:"
	@echo "  make fs-create  Create benchmark index"
	@echo "  make fs-delete Delete benchmark index"
	@echo "  make fs-stats   Show index stats"
	@echo "  make fs-health  Show API health"
	@echo ""
	@echo "Typesense:"
	@echo "  make ts-create  Create benchmark collection"
	@echo "  make ts-delete  Delete benchmark collection"
	@echo "  make ts-stats   Show collection stats"
	@echo "  make ts-health  Show API health"
	@echo ""
	@echo "ZincSearch:"
	@echo "  make zs-create  Create benchmark index"
	@echo "  make zs-delete  Delete benchmark index"
	@echo "  make zs-stats   Show index stats"
	@echo "  make zs-health  Show API health"
	@echo ""
	@echo "Benchmark:"
	@echo "  make benchmark      Run comparison benchmark (default: 10k rows)"
	@echo "  make bench-ts      Run Typesense benchmark (default: 1k rows)"
	@echo "  make serve         Serve report viewer at http://localhost:8080"
	@echo ""
	@echo "Configuration:"
	@echo "  NROWS=$(NROWS)          Rows per dataset (default: 10000)"
	@echo "  ENGINES=$(ENGINES)"
	@echo "  ES_HOST=$(ES_HOST)"
	@echo "  FLATSEEK_HOST=$(FLATSEEK_HOST)"
	@echo "  TYPESENSE_HOST=$(TYPESENSE_HOST)"
	@echo "  ZINC_HOST=$(ZINC_HOST)"
	@echo "  ES_INDEX=$(ES_INDEX)"
	@echo "  FLATSEEK_INDEX=$(FLATSEEK_INDEX)"
	@echo "  TYPESENSE_INDEX=$(TYPESENSE_INDEX)"
	@echo "  ZINC_INDEX=$(ZINC_INDEX)"

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
	@echo "Waiting for FlatseekAPI to be ready..."
	@for i in $$(seq 1 15); do \
		curl -sf $(FLATSEEK_HOST) > /dev/null 2>&1 && break || { \
			echo -n "."; \
			sleep 2; \
		}; \
	done
	@echo ""
	@echo "Waiting for Typesense to be ready..."
	@for i in $$(seq 1 15); do \
		curl -sf $(TYPESENSE_HOST)/health > /dev/null 2>&1 && break || { \
			echo -n "."; \
			sleep 2; \
		}; \
	done
	@echo ""
	@echo "Waiting for ZincSearch to be ready..."
	@for i in $$(seq 1 15); do \
		curl -sf $(ZINC_HOST)/health > /dev/null 2>&1 && break || { \
			echo -n "."; \
			sleep 2; \
		}; \
	done
	@echo ""
	@echo "Ready! Services:"
	@echo "  Elasticsearch: $(ES_HOST)"
	@echo "  FlatseekAPI:      $(FLATSEEK_HOST)"
	@echo "  Typesense:     $(TYPESENSE_HOST)"
	@echo "  ZincSearch:    $(ZINC_HOST)"
	@echo "  Kibana:         http://localhost:5601 (dev profile)"

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
	@echo ""
	@echo "Flatseek health:"
	@curl -sf $(FLATSEEK_HOST)/ 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "Flatseek not responding"
	@echo ""
	@echo "Typesense health:"
	@curl -sf $(TYPESENSE_HOST)/health 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "Typesense not responding"

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

## fs-create - Create flatseek benchmark index
fs-create:
	@echo "Creating index '$(FLATSEEK_INDEX)'..."
	@curl -s -X PUT "$(FLATSEEK_HOST)/$(FLATSEEK_INDEX)" -H 'Content-Type: application/json'
	@echo ""

## fs-delete - Delete flatseek index
fs-delete:
	@echo "Deleting index '$(FLATSEEK_INDEX)'..."
	@curl -s -X DELETE "$(FLATSEEK_HOST)/$(FLATSEEK_INDEX)"
	@echo ""

## fs-stats - Show flatseek index stats
fs-stats:
	@echo "Flatseek index stats:"
	@curl -sf "$(FLATSEEK_HOST)/$(FLATSEEK_INDEX)/_stats" 2>/dev/null | python3 -m json.tool || echo "Failed to get stats"

## fs-health - Show flatseek API health
fs-health:
	@curl -sf $(FLATSEEK_HOST)/ 2>/dev/null | python3 -m json.tool

## ts-create - Create typesense benchmark collection
ts-create:
	@echo "Creating collection '$(TYPESENSE_INDEX)'..."
	@curl -s -X POST "$(TYPESENSE_HOST)/collections" \
		-H "Content-Type: application/json" \
		-d '{"name":"$(TYPESENSE_INDEX)","fields":[{"name":"id","type":"string"},{"name":".*","type":"auto"}],"default_sorting_field":"id"}'
	@echo ""

## ts-delete - Delete typesense collection
ts-delete:
	@echo "Deleting collection '$(TYPESENSE_INDEX)'..."
	@curl -s -X DELETE "$(TYPESENSE_HOST)/collections/$(TYPESENSE_INDEX)"
	@echo ""

## ts-stats - Show typesense collection stats
ts-stats:
	@echo "Typesense collection stats:"
	@curl -sf "$(TYPESENSE_HOST)/collections/$(TYPESENSE_INDEX)" 2>/dev/null | python3 -m json.tool || echo "Failed to get stats"

## ts-health - Show typesense API health
ts-health:
	@curl -sf $(TYPESENSE_HOST)/health 2>/dev/null | python3 -m json.tool

## bench-ts - Run Typesense benchmark
bench-ts:
	@echo "Running Typesense benchmark..."
	@flatbench compare \
			--schema article \
			--engines typesense \
			--sizes $(or $(SIZES),1000)

## zs-create - Create zincsearch benchmark index
zs-create:
	@echo "Creating index '$(ZINC_INDEX)' in ZincSearch..."
	@curl -s -X PUT "$(ZINC_HOST)/api/v1/index" \
		-H "Content-Type: application/json" \
		-u "$(ZINC_USER):$(ZINC_PASSWORD)" \
		-d '{"name":"$(ZINC_INDEX)","mappings":{"properties":{"id":{"type":"keyword"},"title":{"type":"text"},"content":{"type":"text"},"tags":{"type":"keyword"},"views":{"type":"long"},"published_at":{"type":"date"},"author":{"type":"keyword"}}}}'
	@echo ""

## zs-delete - Delete zincsearch index
zs-delete:
	@echo "Deleting index '$(ZINC_INDEX)' in ZincSearch..."
	@curl -s -X DELETE "$(ZINC_HOST)/api/v1/index/$(ZINC_INDEX)" \
		-u "$(ZINC_USER):$(ZINC_PASSWORD)"
	@echo ""

## zs-stats - Show zincsearch index stats
zs-stats:
	@echo "ZincSearch index stats:"
	@curl -sf "$(ZINC_HOST)/api/v1/index/$(ZINC_INDEX)" \
		-u "$(ZINC_USER):$(ZINC_PASSWORD)" 2>/dev/null | python3 -m json.tool || echo "Failed to get stats"

## zs-health - Show zincsearch API health
zs-health:
	@curl -sf "$(ZINC_HOST)/health" 2>/dev/null | python3 -m json.tool || echo "ZincSearch not responding"

## benchmark - Run comparison benchmark
## Usage: make benchmark NROWS=10000 ENGINES="flatseek_cli,elasticsearch"
benchmark:
	@echo "Running benchmark with $(NROWS) rows..."
	@flatbench compare \
			--schema article \
			--engines $(ENGINES) \
			--sizes $(NROWS) \
			--workers 8

## serve - Start the report viewer locally on http://localhost:8080
serve:
	@flatbench serve --port 8080

## build - Build the static site into ./public (mirrors the Vercel build)
build:
	@bash build.sh

## deploy-preview - Deploy a preview build to Vercel
deploy-preview: build
	@vercel deploy --yes

## deploy - Deploy to production (bench.flatseek.io)
deploy: build
	@vercel deploy --prod --yes
