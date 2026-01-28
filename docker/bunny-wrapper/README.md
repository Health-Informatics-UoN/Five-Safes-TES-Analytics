# Bunny Wrapper

This container image is a wrapper for Bunny that maps the 5S-TES Default environment variables for database connection to the names Bunny expects.

It is intended for running Bunny in CLI mode only!

## Files

### `Dockerfile`

The Dockerfile is based on official Bunny images.

`// TODO: parameterise version and CI things so we automatically build for every official Bunny release?`

It does the following above and beyond the base image:

- Automatically provides default dummy values for required daemon mode settings, so that people running this CLI image don't have to provide config that won't be used.

- Adds an alternate entrypoint which maps the variables needed and executes Bunny in CLI mode explicitly

### `entrypoint.sh`

The custom entrypoint does the following:

- Executes Bunny in CLI mode via bash
  - with the correct environment variables exported, with their values set to the 5S-TES default environment variables.

That's all.

## Testing

A Bunny compose and sample query file are included for local testing of this image.

You can test using `docker compose run`:

```bash
docker compose -f test.compose.yml \
run --rm --build \
-v "$(pwd)"/availability.json:/var/queries/availability.json:ro \
bunny --body /var/queries/availability.json --o /tmp/output.json
```

or using `docker compose up`:

`docker compose -f test.compose.yml up`