FROM python:3.12

# Set working directory inside container
WORKDIR /app

# Copy files into /app
COPY query_resolver.py .
COPY local_processing.py .
<<<<<<< HEAD:analysis demo container/Dockerfile
COPY local_processing_base.py .
=======
COPY local_processing_base.py . 
>>>>>>> origin/main:docker/Dockerfile

# Install dependencies
RUN pip install click sqlalchemy psycopg2-binary numpy tdigest

# Entry point
ENTRYPOINT ["python", "query_resolver.py"]
