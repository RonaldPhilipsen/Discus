FROM rust:1-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Extra Cargo features, for example --build-arg FEATURES=pyroscope.
ARG FEATURES=""
# Static glibc instead of musl: on musl, the profiler's unwinder calls scan all of .eh_frame and
# profiling costs about 20% of the throughput. See docs/benchmarks/2026-11-13-k8s-static-glibc.
ENV RUSTFLAGS="-C target-feature=+crt-static"
# The release version, which the binary reports as its build ID.
ARG VERSION=dev
ENV DISCUS_BUILD=$VERSION
RUN cargo build --release --locked -p discus --target x86_64-unknown-linux-gnu --features "$FEATURES"
# The Pyroscope agent cannot start without system CA certificates, so a profiling image gets them.
RUN mkdir -p /out \
    && case " $(echo "$FEATURES" | tr ',' ' ') " in *" pyroscope "*) \
        mkdir -p /out/etc/ssl/certs && cp /etc/ssl/certs/ca-certificates.crt /out/etc/ssl/certs/ ;; \
    esac

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
