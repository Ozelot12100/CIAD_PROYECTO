# Load env variables from .env file into Makefile variables
include .env

export $(shell sed 's/=.*//' .env)

# Git submodule initialization
init-submodules:
	@echo "Initializing git submodules..."
	@git submodule update --init --recursive
	@echo "Git submodules initialized. ✅"

#region Production Environment
prod-up:
	@echo "Starting production environment..."
	@docker compose --project-directory . -f docker/compose/prod.yml up -d --remove-orphans $(ARGS)
	@echo "Production environment started. ✅"
prod-restart:
	@echo "Restarting production environment..."
	@docker compose --project-directory . -f docker/compose/prod.yml restart $(ARGS)
	@echo "Production environment restarted. ✅"
prod-pull-up:
	@if [ -z "$(ARGS)" ]; then \
		echo "No arguments provided. Pulling and starting all services."; \
	else \
		echo "Pulling and starting specified services: $(ARGS)"; \
	fi
	@docker compose --project-directory . -f docker/compose/prod.yml pull $(ARGS)
	@docker compose --project-directory . -f docker/compose/prod.yml up -d --remove-orphans $(ARGS)
	@echo "Production environment started with latest images. ✅"
prod-down-rm:
	@echo "Stopping and removing containers..."
	@docker compose --project-directory . -f docker/compose/prod.yml down --remove-orphans $(ARGS)
	@echo "Containers stopped and removed."
prod-logs-follow:
	@echo "Following logs for services: $(ARGS)..."
	@docker compose --project-directory . -f docker/compose/prod.yml logs --follow --tail 500 $(ARGS)
#endregion


#region Development Environment
dev-up:
	@echo "Starting development environment..."
	@docker compose --project-directory . -f docker/compose/dev.yml up --remove-orphans $(ARGS)
	@echo "Development environment started."
dev-down-rm:
	@echo "Stopping and removing containers..."
	@docker compose --project-directory . -f docker/compose/dev.yml down --remove-orphans $(ARGS)
	@echo "Containers stopped and removed."
dev-logs-follow:
	@echo "Following logs for services: $(ARGS)..."
	@docker compose --project-directory . -f docker/compose/dev.yml logs --follow --tail 500 $(ARGS)
	@echo "Logs are being followed. Press Ctrl+C to stop."
#endregion