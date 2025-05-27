.PHONY: setup start stop test test-prepare-data-blocks clean

# Docker services
setup:
	@echo "Setting up environment variables..."
	export POSTGRES_HOST=localhost
	export POSTGRES_PORT=5436
	export POSTGRES_USER=synctool
	export POSTGRES_PASSWORD=synctool
	export POSTGRES_DB=synctool

# Start the Docker containers
start:
	@echo "Starting Docker containers..."
	docker compose up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 5

# Stop the Docker containers
stop:
	@echo "Stopping Docker containers..."
	docker-compose down

# Run specific test for prepare_data_blocks
test-prepare-data-blocks: start
	@echo "Running prepare_data_blocks tests..."
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5436 \
	POSTGRES_USER=synctool \
	POSTGRES_PASSWORD=synctool \
	POSTGRES_DB=synctool \
	python -m pytest test/engine/reconcile/test_prepare_data_blocks.py -v
	@echo "Tests completed."

# Run all tests related to reconcile
test-reconcile: start
	@echo "Running all reconcile tests..."
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5436 \
	POSTGRES_USER=synctool \
	POSTGRES_PASSWORD=synctool \
	POSTGRES_DB=synctool \
	python -m pytest test/engine/reconcile/

# Run all tests
test: start
	@echo "Running all tests..."
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5436 \
	POSTGRES_USER=synctool \
	POSTGRES_PASSWORD=synctool \
	POSTGRES_DB=synctool \
	python -m pytest

# Clean up resources
clean: stop
	@echo "Cleaning up resources..."
	docker volume rm synctool_postgres_data || true
	@echo "Cleanup completed."

# Default target
all: test-prepare-data-blocks
