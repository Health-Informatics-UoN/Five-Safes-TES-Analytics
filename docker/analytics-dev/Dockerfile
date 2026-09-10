FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY src ./src

RUN uv sync --locked --no-dev

ENTRYPOINT ["uv", "run", "--no-sync", "-m", "five_safes_tes_analytics.node.query_resolver"]
