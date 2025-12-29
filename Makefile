DC?=docker compose

.PHONY: up down logs heal netem netem_loss netem_delay partition_leader_minority partition_leader_majority blackhole

up:
	$(DC) up -d --build

down:
	$(DC) down -v

logs:
	$(DC) logs -f

