FROM bitnami/spark:3.5.1
WORKDIR /usr/local/app

COPY src ./src
COPY data ./data