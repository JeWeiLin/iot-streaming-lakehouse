FROM apache/superset:5.0.0

# Superset 官方 image 不含 Trino 的 driver，裝進 /app/.venv
USER root
RUN uv pip install --python /app/.venv/bin/python --no-cache trino
USER superset

COPY --chown=superset:superset superset_config.py /app/pythonpath/
COPY --chown=superset:superset bootstrap.sh /app/bootstrap.sh
