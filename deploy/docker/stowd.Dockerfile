# syntax=docker/dockerfile:1
# Multi-stage build: compile in the Rust image, ship only the binaries.
FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --profile dist --locked -p stowd -p stowctl -p stow-bench \
 && mkdir -p /out && cp target/dist/stowd target/dist/stowctl target/dist/stow-bench /out/

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 stowage && mkdir -p /data && chown stowage /data
COPY --from=build /out/ /usr/local/bin/
USER stowage
VOLUME ["/data"]
EXPOSE 7001
ENTRYPOINT ["stowd"]
