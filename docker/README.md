# Docker: Node-side query and analysis

This folder contains the code that runs **inside the TES container** on each TRE node. It executes the user’s analysis (SQL plus optional Python) against the node’s database and writes the result (e.g. JSON) for the client to aggregate.

## Purpose

- The **user query** and **analysis type** are passed in as CLI arguments.
- The **analysis type** is looked up in the `LOCAL_PROCESSING_CLASSES` registry in `local_processing.py`.
- Each analysis class is responsible for:
  - Building the SQL query (from the user query + analysis-specific logic),
  - Running it against the node DB,
  - Optional Python-side analysis on the result.
- Results are written to file (e.g. JSON) and later collected and aggregated on the client side.

So this code does the **per-node, partial** work; aggregation across TREs happens elsewhere (orchestrator / client).

## Flow

1. **Entrypoint** — Container runs `python query_resolver.py` with CLI args (`--user-query`, `--analysis`, `--db-connection` or env, `--output-filename`, `--output-format`).
2. **query_resolver.py** — Parses the connection string (from env or `--db-connection`), then calls `process_query()`.
3. **process_query()** — Resolves the DB connection, looks up the analysis in `LOCAL_PROCESSING_CLASSES`, instantiates the processor, builds and runs the query, runs optional Python analysis, and writes the result to disk.
4. **local_processing.py** — Defines the registry and analysis classes (e.g. Mean, Variance, PMCC, ContingencyTable). Each class extends `BaseLocalProcessing` (from `local_processing_base.py`) and implements query building and optional Python analysis.

## Main modules

| File | Role |
|------|------|
| `query_resolver.py` | Click CLI, connection string parsing (`parse_connection_string`), and `process_query()` (orchestrates DB connection, registry lookup, execution, output). |
| `local_processing.py` | `LOCAL_PROCESSING_CLASSES` registry and concrete analysis classes (Mean, Variance, etc.). |
| `local_processing_base.py` | `BaseLocalProcessing` abstract base class (query building, optional Python analysis hook). |
| `Dockerfile` | Builds the image that runs this code (Python 3.12, dependencies, entrypoint `query_resolver.py`). |

## Database connection

- If `--db-connection` is **not** provided, the connection string is built from environment variables: `postgresUsername`, `postgresPassword`, `postgresServer`, `postgresPort`, `postgresDatabase` (see `validate_environment()` and `parse_connection_string(None)` in `query_resolver.py`). This is the normal case when the container is launched by TES with env set by the task.
- If `--db-connection` is provided, it can be a SQLAlchemy-style URL (`postgresql://...`) or a semicolon-separated key=value string (`Host=...;Username=...;...`).

## Building and running

From the repo root or this directory, build the image (see project docs or `tests/` for the exact image name and test usage). The container expects either postgres* env vars or `--db-connection`, plus `--user-query`, `--analysis`, and optional output options.

For the **bunny**-based workflow (different image and entrypoint), see `bunny-wrapper/`.
