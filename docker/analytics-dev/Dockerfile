FROM python:3.12

# Set working directory inside container
WORKDIR /app

# Copy files into /app
COPY src/five_safes_tes_analytics/node/* . 

# Install dependencies
RUN pip install click sqlalchemy psycopg2-binary numpy tdigest

# Entry point
ENTRYPOINT ["python", "query_resolver.py"]
