# Tests

## Unit tests
```bash
uv run pytest tests/unit
```

## Integration tests
Require Docker and a reachable Postgres instance. Disabled by default. 

```bash
uv run pytest tests/integration -m integration
```

## Docker smoke tests
Compose-based smoke tests for the analytics and bunny-wrapper containers.
```bash
docker compose -f docker/analytics-dev/test.compose.yml up -d
docker compose -f docker/bunny-wrapper/test.compose.yml up -d 
```

With the output written to the mount folder in `docker/bunny-wrapper/test-output` and `docker/analytics-dev/test-output`. 
