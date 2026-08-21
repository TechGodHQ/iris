# syntax=docker/dockerfile:1
# Iris server image (linux/arm64 — built on builder-01).
# Multi-stage: cargo build in a fat builder, ship a slim runtime.

# Pin builder to bookworm so glibc matches the bookworm runtime —
# rust:1-slim now tracks trixie (glibc 2.39) and binaries fail on
# bookworm-slim (glibc 2.36) with `GLIBC_2.39 not found`.
FROM rust:1-slim-bookworm AS builder
WORKDIR /build
# native-tls (lettre, email provider) needs OpenSSL headers.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
# Build the release binary. Buildkit cache mounts keep rebuilds fast.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release -p iris-cli \
    && cp target/release/iris /usr/local/bin/iris

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /data --shell /usr/sbin/nologin iris
COPY --from=builder /usr/local/bin/iris /usr/local/bin/iris
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && mkdir -p /etc/iris /data \
    && chown -R iris:iris /etc/iris /data
ENV IRIS_ATTACHMENT_DIR=/data/attachments
ENV IRIS_AUDIT_DIR=/data/audit
USER iris
VOLUME /data
EXPOSE 9876
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["iris", "serve", "--addr", "0.0.0.0:9876"]
