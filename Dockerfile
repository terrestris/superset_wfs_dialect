FROM apache/superset:5.0.0rc2@sha256:00c4ddf3b8a6a9fc95f0dffb09e8f29129732ca5fee0aff2fd5c30cc6668deff

USER root

COPY superset_config.py /app/pythonpath/
RUN chown -R superset:superset /app/pythonpath
RUN mkdir -p /app/owslib && chown -R superset:superset /app/owslib
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

RUN pip install --no-cache-dir --target=/app/pythonpath requests sqlglot debugpy lxml

USER superset

ENTRYPOINT ["/app/docker-entrypoint.sh"]
