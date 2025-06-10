FROM apache/superset:5.0.0rc2@sha256:00c4ddf3b8a6a9fc95f0dffb09e8f29129732ca5fee0aff2fd5c30cc6668deff

USER root

RUN /app/.venv/bin/python -m ensurepip --upgrade

COPY superset_config.py /app/pythonpath/
RUN chown -R superset:superset /app/pythonpath

COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Install all dependencies to /app/pythonpath
COPY requirements.txt /app/pythonpath
RUN pip3 install --no-cache-dir -r /app/pythonpath/requirements.txt

USER superset
ENTRYPOINT ["/app/docker-entrypoint.sh"]
