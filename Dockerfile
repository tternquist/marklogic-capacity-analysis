FROM python:3.12-slim
WORKDIR /app
COPY ml_capacity.py .
RUN mkdir -p /data/.ml-capacity

ENV MLCA_HOST=localhost
ENV MLCA_PORT=8002
ENV MLCA_USER=admin
ENV MLCA_PASSWORD=admin
ENV MLCA_AUTH_TYPE=digest
ENV MLCA_DATABASE=Documents
ENV MLCA_INTERVAL=15m
ENV MLCA_SERVE_PORT=9090

EXPOSE 9090

CMD python3 ml_capacity.py \
    --host $MLCA_HOST \
    --port $MLCA_PORT \
    --user $MLCA_USER \
    --password $MLCA_PASSWORD \
    --auth-type $MLCA_AUTH_TYPE \
    --database $MLCA_DATABASE \
    --serve \
    --interval $MLCA_INTERVAL \
    --serve-port $MLCA_SERVE_PORT
