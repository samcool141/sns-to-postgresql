FROM public.ecr.aws/docker/library/python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sqs_consumer.py /app/sqs_consumer.py

CMD ["python", "/app/sqs_consumer.py"]