FROM python:3.12-alpine

# Alpine base scans clean (0 critical / 0 high with trivy) - debian-slim
# carries unfixed perl-base criticals. All python deps ship musllinux wheels,
# so no compiler is needed.
#
# Helm binary baked in at build time: the operator shells out to it to install
# ArgoCD on downstream clusters. Built with internet access, runs air-gapped.
ARG HELM_VERSION=4.2.3
RUN wget -qO- "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz" \
    | tar -xz --strip-components=1 -C /usr/local/bin linux-amd64/helm \
 && helm version

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY operator.py .

# POC environment: runs as root deliberately, same as Dockerfile.airgap.
# Helm cache/config dirs kept explicit for tidiness.
ENV HELM_CACHE_HOME=/tmp/helm/cache \
    HELM_CONFIG_HOME=/tmp/helm/config \
    HELM_DATA_HOME=/tmp/helm/data

CMD ["kopf", "run", "--standalone", "--all-namespaces", "/app/operator.py"]
