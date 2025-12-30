FROM apache/superset:latest

USER root

# 1. Re-install pip (it is missing in the slim version)
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | /app/.venv/bin/python

# 2. Install the Trino driver into the correct virtual environment
RUN /app/.venv/bin/python -m pip install trino sqlalchemy-trino

USER superset