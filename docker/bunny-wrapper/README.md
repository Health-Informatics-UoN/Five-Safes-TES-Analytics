# Bunny Wrapper

This container image is a wrapper for Bunny that maps the 5S-TES Default environment variables for database connection to the names Bunny expects.

It is intended for running Bunny in CLI mode only!

## Files

### `Dockerfile`

The Dockerfile is based on official Bunny images.

It does the following above and beyond the base image:

- Automatically provides default dummy values for required daemon mode settings, so that people running this CLI image don't have to provide config that won't be used.

- Adds an alternate entrypoint which maps the variables needed and executes Bunny in CLI mode explicitly

- Accepts `BUNNY_VERSION` as a build argument to select the base Bunny image version

### `entrypoint.sh`

The custom entrypoint does the following:

- Executes Bunny in CLI mode via bash
  - with the correct environment variables exported, with their values set to the 5S-TES default environment variables.

That's all.

## Continuous Integration (CI)

This repository includes a GitHub Actions workflow `release.bunny.yml` that automatically builds and publishes the Bunny CLI wrapper image for Five Safes TES. The workflow tracks the latest released version of Bunny and only creates a new image if one for that release does not already exist.

Each published image is labeled to match the corresponding upstream Bunny release. Because this wrapper directly tracks Bunny versions, the repository does not maintain an independent semantic versioning lifecycle.

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

### Testing an unreleased Bunny version

Bunny publishes an image for each pull request opened from a branch in its own repository, tagged `pr-<number>` and `sha-<short commit>`. To test the wrapper against one, set `BUNNY_VERSION` (if unset, the default in the `Dockerfile` is used):

```bash
BUNNY_VERSION=pr-331 docker compose -f test.compose.yml build bunny
docker compose -f test.compose.yml up -d --wait db
docker compose -f test.compose.yml up omop-lite
docker compose -f test.compose.yml run --rm --no-deps bunny
```

Results are written to `./test-output/output.json`. Don't use `--abort-on-container-exit`: `omop-lite` exits once it has loaded the data, and that flag would stop the database too.