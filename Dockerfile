FROM python:3.12-slim
WORKDIR /app
COPY ml_capacity/ ml_capacity/
RUN mkdir -p /data/.ml-capacity && ln -s /data/.ml-capacity /app/.ml-capacity

ARG BUILD_SHA=dev
ENV MLCA_BUILD=$BUILD_SHA

ENV MLCA_HOST=localhost
ENV MLCA_PORT=8002
ENV MLCA_USER=admin
ENV MLCA_PASSWORD=admin
ENV MLCA_AUTH_TYPE=digest
ENV MLCA_DATABASE=Documents
ENV MLCA_INTERVAL=15m
ENV MLCA_SERVE_PORT=9090
ENV MLCA_RETENTION_DAYS=30

EXPOSE 9090

CMD python3 -m ml_capacity \
    --host $MLCA_HOST \
    --port $MLCA_PORT \
    --user $MLCA_USER \
    --password $MLCA_PASSWORD \
    --auth-type $MLCA_AUTH_TYPE \
    --database $MLCA_DATABASE \
    --serve \
    --interval $MLCA_INTERVAL \
    --serve-port $MLCA_SERVE_PORT \
    --retention-days $MLCA_RETENTION_DAYS
