# ZMQ Broker — Multi-stage Docker build
# Stage 1: Build with Zig compiler
# Stage 2: Minimal runtime image

FROM debian:bookworm-slim AS builder

# Install Zig 0.16.0
ARG TARGETARCH
ARG ZIG_VERSION=0.16.0
RUN apt-get update && apt-get install -y curl xz-utils && \
    case "$TARGETARCH" in \
      amd64) zig_arch=x86_64 ;; \
      arm64) zig_arch=aarch64 ;; \
      *) echo "unsupported Docker target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac && \
    curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-${zig_arch}-linux-${ZIG_VERSION}.tar.xz" | tar xJ -C /opt && \
    ln -s "/opt/zig-${zig_arch}-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY build.zig ./
COPY src/ src/

# Build release binary
RUN zig build -Doptimize=ReleaseFast

# Stage 2: Minimal runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libssl3 && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /data/automq

COPY --from=builder /src/zig-out/bin/zmq /usr/local/bin/zmq

# Default ports: 9092 (Kafka broker), 9093 (KRaft controller), 9090 (metrics/health)
EXPOSE 9092 9093 9090

# Data directory for WAL and metadata
VOLUME /data/automq

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

ENTRYPOINT ["zmq"]
CMD ["--port", "9092", "--controller-port", "9093", "--data-dir", "/data/automq", "--metrics-port", "9090"]
