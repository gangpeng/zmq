# ZMQ Broker — Developer Makefile
# Wraps zig build commands for convenience.

ZIG ?= zig
ZIG_FLAGS ?=

.PHONY: build test run clean e2e s3-crash docker bench help codegen

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the broker binary (debug)
	$(ZIG) build $(ZIG_FLAGS)

release: ## Build optimized release binary
	$(ZIG) build -Doptimize=ReleaseFast $(ZIG_FLAGS)

test: ## Run all unit tests
	$(ZIG) build test --summary all $(ZIG_FLAGS)

run: build ## Run the broker on port 9092
	./zig-out/bin/zmq

run-s3: build ## Run with MinIO S3 backend
	./zig-out/bin/zmq 9092 --data-dir /tmp/automq-data \
		--s3-endpoint 127.0.0.1 --s3-port 9000

run-cluster: build ## Run 3-node cluster (requires 3 terminals)
	@echo "Terminal 1: ./zig-out/bin/zmq 9092 --node-id 0 --voters 0@localhost:9092,1@localhost:9093,2@localhost:9094"
	@echo "Terminal 2: ./zig-out/bin/zmq 9093 --node-id 1 --voters 0@localhost:9092,1@localhost:9093,2@localhost:9094"
	@echo "Terminal 3: ./zig-out/bin/zmq 9094 --node-id 2 --voters 0@localhost:9092,1@localhost:9093,2@localhost:9094"

codegen: ## Regenerate protocol structs from JSON schemas
	python3 src/protocol/codegen/codegen_v2.py src/protocol/schemas/ src/protocol/generated/

e2e: build ## Run E2E tests with MinIO (requires Docker)
	python3 tests/e2e_test.py

s3-crash: build ## Run gated S3 broker process crash/restart test (requires MinIO)
	ZMQ_BIN=./zig-out/bin/zmq python3 tests/s3_process_crash_test.py

bench: ## Run performance benchmarks
	$(ZIG) build bench $(ZIG_FLAGS)

docker: ## Build Docker image
	docker build -t zmq:latest .

docker-up: ## Start 3-node cluster with MinIO
	docker compose up -d --build

docker-down: ## Stop the cluster
	docker compose down -v

docker-logs: ## View node logs
	docker compose logs -f node0 node1 node2

clean: ## Clean build artifacts
	rm -rf .zig-cache zig-out

fmt: ## Format all Zig source files
	find src -name '*.zig' -exec $(ZIG) fmt {} +

loc: ## Count lines of code
	@echo "Hand-written: $$(find src -name '*.zig' -not -path '*/generated/*' -not -path '*/schemas/*' -exec cat {} + | wc -l) LOC"
	@echo "Generated:    $$(find src/protocol/generated -name '*.zig' -exec cat {} + | wc -l) LOC"
	@echo "Total:        $$(find src -name '*.zig' -exec cat {} + | wc -l) LOC"
	@echo "Files:        $$(find src -name '*.zig' | wc -l)"

status: ## Show project status
	@echo "═══════════════════════════════════════════════"
	@echo "  ZMQ Broker v0.8.0"
	@echo "═══════════════════════════════════════════════"
	@echo "Binary: $$(ls -lh zig-out/bin/zmq 2>/dev/null | awk '{print $$5}' || echo 'not built')"
	@echo "TODOs:  $$(grep -r 'TODO\|FIXME' src/ --include='*.zig' -l 2>/dev/null | wc -l)"
	@make loc 2>/dev/null
