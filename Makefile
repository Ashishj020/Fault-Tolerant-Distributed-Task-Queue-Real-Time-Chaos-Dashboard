.PHONY: up down logs test submit-jobs chaos reset

up:
	docker compose up --build

down:
	docker compose down --remove-orphans

logs:
	docker compose logs -f --tail=200

test:
	docker compose exec -T api pytest -q tests

submit-jobs:
	curl -s -X POST http://localhost:8010/jobs/batch -H "Content-Type: application/json" -d "{\"count\": 12}"

chaos:
	docker compose exec -T api python -c "import httpx,json; c=httpx.Client(base_url='http://127.0.0.1:8000', timeout=30); print(json.dumps(c.post('/jobs/batch', json={'count':12}).json())); print(json.dumps(c.post('/chaos/start', json={'duration':120,'kill_interval':15,'workers':'random'}).json()))"

reset:
	curl -s -X POST http://localhost:8010/reset
	docker compose restart worker-1 worker-2 worker-3
