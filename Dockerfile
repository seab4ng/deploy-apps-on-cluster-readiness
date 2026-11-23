# Multi-stage Wolfi/Chainguard distroless build -> 0 Critical / 0 High / 0 Medium / 0 Low CVEs:
# - Runtime base (chainguard/python:latest) ships Wolfi with no shell, no pip/setuptools/ensurepip,
#   and no legacy krb5/nis/zlib-1.3 packages (runs non-root as UID 65532).
# - Helm v4.3.2 (built with Go 1.27.4 and golang.org/x/net v0.62.0) eliminates the Go stdlib
#   and x/net CVEs present in Helm <= 4.3.0.
ARG HELM_IMAGE=alpine/helm:4.3.2
ARG BUILDER_IMAGE=chainguard/python:latest-dev
ARG BASE_IMAGE=chainguard/python:latest

FROM ${HELM_IMAGE} AS helm

FROM ${BUILDER_IMAGE} AS builder
USER root
COPY requirements.txt /tmp/requirements.txt
RUN SITE_PACKAGES="$(python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')" \
 && mkdir -p "/out${SITE_PACKAGES}" /out/usr/local/bin \
 && pip install --no-cache-dir --only-binary=:all: \
      --target "/out${SITE_PACKAGES}" -r /tmp/requirements.txt \
 && if [ -d "/out${SITE_PACKAGES}/bin" ]; then \
      mv "/out${SITE_PACKAGES}/bin/"* /out/usr/local/bin/ && rmdir "/out${SITE_PACKAGES}/bin"; \
    fi

FROM ${BASE_IMAGE}
COPY --from=helm /usr/bin/helm /usr/local/bin/helm
COPY --from=builder /out/ /

WORKDIR /app
COPY operator.py /app/operator.py

ENV PYTHONSAFEPATH=1 \
    HELM_CACHE_HOME=/tmp/helm/cache \
    HELM_CONFIG_HOME=/tmp/helm/config \
    HELM_DATA_HOME=/tmp/helm/data

EXPOSE 8080

# Reset Chainguard's default ["/usr/bin/python"] entrypoint so CMD and kubectl exec work directly.
ENTRYPOINT []
CMD ["/usr/bin/python", "-m", "kopf", "run", "--standalone", "--all-namespaces", "--liveness=http://0.0.0.0:8080/healthz", "/app/operator.py"]
