FROM apache/superset:latest

USER root

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | /app/.venv/bin/python
RUN /app/.venv/bin/python -m pip install trino sqlalchemy-trino

COPY datasources.yaml /app/datasources.yaml
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

USER superset

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/usr/bin/run-server.sh"]