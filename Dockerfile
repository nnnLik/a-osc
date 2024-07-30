FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY main.py .
RUN pip install --no-cache-dir mysql-connector-python

ENTRYPOINT ["python", "-u", "./main.py"]