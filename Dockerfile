FROM apache/superset:5.0.0rc2@sha256:00c4ddf3b8a6a9fc95f0dffb09e8f29129732ca5fee0aff2fd5c30cc6668deff

USER root

# Install git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN /app/.venv/bin/python -m ensurepip --upgrade

COPY superset_config.py /app/pythonpath/
RUN chown -R superset:superset /app/pythonpath

COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Install all dependencies to /app/pythonpath including OWSLib from git
COPY requirements.txt /app/pythonpath
RUN pip3 install --no-cache-dir -r /app/pythonpath/requirements.txt

RUN apt-get purge -y git && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

USER superset
ENTRYPOINT ["/app/docker-entrypoint.sh"]
