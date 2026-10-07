FROM ubuntu:24.04
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY coffee-cloud-backend-bin /usr/local/bin/coffee-cloud-backend
COPY static /app/static

ENV PORT=8080
EXPOSE 8080

CMD ["/usr/local/bin/coffee-cloud-backend"]
