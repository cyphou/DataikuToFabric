# Multi-stage Dockerfile for Dataiku → Fabric Migration Toolkit
# Build:   docker build -t dataiku-to-fabric .
# Run:     docker run --rm -v ./config:/app/config -v ./output:/app/output dataiku-to-fabric discover -p MY_PROJECT

# ── Stage 1: build wheel ─────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build
COPY pyproject.toml MANIFEST.in requirements.txt ./
COPY src/ src/
COPY templates/ templates/
COPY config/config.template.yaml config/config.template.yaml

RUN pip install --no-cache-dir build \
    && python -m build --wheel --outdir /build/dist

# ── Stage 2: runtime ─────────────────────────────────────────
FROM python:3.12-slim

LABEL maintainer="Dataiku to Fabric Migration Team"
LABEL description="Automated Dataiku → Microsoft Fabric migration toolkit"

WORKDIR /app

# Install the wheel + deploy extras (azure-identity)
COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl[deploy] \
    && rm -rf /tmp/*.whl

# Copy templates & default config
COPY templates/ /app/templates/
COPY config/config.template.yaml /app/config/config.template.yaml

# Non-root user for security
RUN useradd --create-home appuser
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["dataiku-to-fabric", "--version"]

ENTRYPOINT ["dataiku-to-fabric"]
CMD ["--help"]
