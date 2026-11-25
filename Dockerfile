FROM rust:1-bookworm@sha256:75d26c23c24cc6ea1cdabf0714a6bff8fd7bf3248716043fd7ae86dac7f41b94 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Extra Cargo features, for example --build-arg FEATURES=pyroscope.
ARG FEATURES=""
# Static glibc instead of musl: on musl, the profiler's unwinder calls scan all of .eh_frame and
# profiling costs about 20% of the throughput. See docs/benchmarks/2026-11-13-k8s-v1.2.4/profiling-cost.md.
ENV RUSTFLAGS="-C target-feature=+crt-static"
# The release version, which the binary reports as its build ID.
ARG VERSION=dev
ENV DISCUS_BUILD=$VERSION
RUN cargo build --release --locked -p discus --target x86_64-unknown-linux-gnu --features "$FEATURES"
# The default build reads the system trust roots. An OAuth 2.0 issuer without ca_file, an https
# OTLP endpoint without --otel-ca-file, and the Pyroscope agent need them, so every image gets the
# CA bundle.
RUN mkdir -p /out/etc/ssl/certs && cp /etc/ssl/certs/ca-certificates.crt /out/etc/ssl/certs/

FROM scratch
COPY --from=build /out/ /
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/discus /discus
USER 10001
EXPOSE 6379
ENTRYPOINT ["/discus"]

ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=unknown
LABEL org.opencontainers.image.title="Discus" \
      org.opencontainers.image.description="A Redis-compatible cache that scales horizontally and stays highly available" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.created="$CREATED" \
      org.opencontainers.image.source="https://github.com/RonaldPhilipsen/Discus" \
      org.opencontainers.image.url="https://github.com/RonaldPhilipsen/Discus" \
      org.opencontainers.image.documentation="https://github.com/RonaldPhilipsen/Discus/tree/main/docs" \
      org.opencontainers.image.licenses="MIT"
