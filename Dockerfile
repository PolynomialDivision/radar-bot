FROM rust:1.95-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libsqlite3-dev cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/radar-bot /usr/local/bin/radar-bot

VOLUME /app/store
VOLUME /app/config

WORKDIR /app

ENV RUST_LOG=info

CMD ["radar-bot", "/app/config/config.toml"]
