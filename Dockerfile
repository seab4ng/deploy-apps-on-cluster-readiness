FROM python:3.12-slim

# Helm binary baked in at build time: the operator shells out to it to install
# ArgoCD on downstream clusters. Built with internet access, runs air-gapped.
ARG HELM_VERSION=3.16.4
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSL "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz" \
    | tar -xz --strip-components=1 -C /usr/local/bin linux-amd64/helm \
 && helm version \
 && apt-get purge -y curl \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY operator.py .

# Run as non-root; helm needs writable cache/config dirs.
ENV HELM_CACHE_HOME=/tmp/helm/cache \
    HELM_CONFIG_HOME=/tmp/helm/config \
    HELM_DATA_HOME=/tmp/helm/data
RUN useradd --uid 1000 --no-create-home operator
USER 1000

CMD ["kopf", "run", "--standalone", "--all-namespaces", "/app/operator.py"]
