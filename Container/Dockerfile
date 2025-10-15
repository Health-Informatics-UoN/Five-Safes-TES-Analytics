FROM python:3.12
# Or any preferred Python version.
ADD query_resolver.py .
COPY local_processing.py .

RUN pip install click sqlalchemy numpy tdigest

ENTRYPOINT ["python", "./query_resolver.py"] 
# Or enter the name of your unique directory and parameter set.