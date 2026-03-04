FROM python:3.12

# Set working directory inside container
WORKDIR /app

# Copy only the node subpackage into the container
# This avoids installing the full package while preserving relative imports
# TODO: Consider using uv workspace packages for cleaner separation of concerns
COPY src/five_safes_tes_analytics/node /app/five_safes_tes_analytics/node

# Install dependencies
RUN pip install click sqlalchemy psycopg2-binary numpy tdigest

# Entry point
ENTRYPOINT ["python", "-m", "five_safes_tes_analytics.node.query_resolver"]
