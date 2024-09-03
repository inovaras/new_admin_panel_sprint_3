FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /etl

COPY etl/wait-for-it.sh .
COPY etl/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY ./etl .

CMD ["bash", "-c", "./wait-for-it.sh elasticsearch:9200 -- python etl.py"]
