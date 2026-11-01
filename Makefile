.PHONY: up down clean logs ps shell-db

up:
	docker compose up --build -d

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

shell-db:
	docker compose exec timescaledb psql -U postgres -d sensors
