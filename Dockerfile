# ZETA image: the API node, the router, the data loader and the demo store app (docker-compose.yml picks the role).
FROM python:3.13-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY zeta zeta
COPY store store
COPY generate_data.py split_load.py schema.sql harness.py test_isolation.py simulate.py demo_live.py bench.py ./
COPY docker/entrypoint.sh /entrypoint.sh
RUN printf '#!/bin/sh\ncd /app && exec python -m zeta.guard "$@"\n' > /usr/local/bin/keys && chmod +x /usr/local/bin/keys
ENTRYPOINT ["sh", "/entrypoint.sh"]
CMD ["node"]
