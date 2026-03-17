# Tests

## Unit tests
```bash
uv run pytest tests/unit
```

## Integration tests
The `tests/integration/test_docker_runner.py` require Docker and a reachable Postgres instance. 

The `tests/integration/test_submission_api_session_integration.py` require a deployed 5STES instance.

All integration tests are disabled by default. Run via: 

```bash
uv run pytest tests/integration -m integration
```

## Docker smoke tests
Compose-based entrypoint smoke tests for the analytics and bunny-wrapper containers.
```bash
docker compose -f docker/analytics-dev/test.compose.yml up -d
docker compose -f docker/bunny-wrapper/test.compose.yml up -d 
```

With the output written to the mount folder in `docker/bunny-wrapper/test-output` and `docker/analytics-dev/test-output`. 
