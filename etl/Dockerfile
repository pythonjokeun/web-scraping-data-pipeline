FROM python:3.7-alpine

COPY ./Pipfile /Pipfile
COPY ./Pipfile.lock /Pipfile.lock

RUN apk add --no-cache --virtual .build-deps \
    postgresql-libs linux-headers gcc musl-dev postgresql-dev && \
    pip install --no-cache-dir pipenv &&\
    pipenv lock -r > requirements.txt &&\
    pip install --no-cache-dir -r requirements.txt && \
    apk del .build-deps &&\
    rm Pipfile Pipfile.lock requirements.txt

COPY . /etl

WORKDIR /etl

CMD python etl.py
