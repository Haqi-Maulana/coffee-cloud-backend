# Stage 1: Build
FROM rust:1.80-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y pkg-config libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates openssl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/coffee-cloud-backend /usr/local/bin/coffee-cloud-backend

ENV PORT=8080
EXPOSE 8080

CMD ["/usr/local/bin/coffee-cloud-backend"]
