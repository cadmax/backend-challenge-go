.PHONY: up up-multi down deps status logs build run test race vet check integration migrate-up migrate-down token smoke

up:
	docker compose up --build -d --wait

up-multi:
	docker compose --profile multi up --build -d --wait

down:
	docker compose --profile multi down

deps:
	docker compose up -d --wait postgres keycloak localstack

status:
	docker compose --profile multi ps -a

logs:
	docker compose --profile multi logs -f --tail=100

build:
	go build ./cmd/...

run:
	go run ./cmd/wager-api

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: test race vet

integration: deps
	./scripts/integration.sh

migrate-up:
	docker compose run --rm migrate /app/migrate up

migrate-down:
	docker compose run --rm migrate /app/migrate down

token:
	./scripts/token.sh $(CLIENT)

smoke:
	./scripts/smoke.sh
