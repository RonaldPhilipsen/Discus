FROM rust:1-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Extra Cargo features, for example --build-arg FEATURES=pyroscope.
ARG FEATURES=""
RUN cargo build --release --locked -p discus --target x86_64-unknown-linux-musl --features "$FEATURES"
# The Pyroscope agent cannot start without system CA certificates, so a profiling image gets them.
RUN mkdir -p /out \
    && case " $(echo "$FEATURES" | tr ',' ' ') " in *" pyroscope "*) \
        mkdir -p /out/etc/ssl/certs && cp /etc/ssl/certs/ca-certificates.crt /out/etc/ssl/certs/ ;; \
    esac

FROM scratch
COPY --from=build /out/ /
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/discus /discus
USER 10001
EXPOSE 6379
ENTRYPOINT ["/discus"]
