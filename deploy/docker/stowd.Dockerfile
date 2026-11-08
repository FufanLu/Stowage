# syntax=docker/dockerfile:1
# Multi-stage build: compile in the Rust image, build the console with Node,
# ship only the binaries and the console's static files.
FROM node:24-bookworm-slim AS console
WORKDIR /console
COPY web/console/package.json web/console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/console/ ./
RUN npm run build

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
COPY --from=console /console/dist/ /usr/share/stowage/console/
ENV STOWAGE_CONSOLE_DIR=/usr/share/stowage/console
USER stowage
VOLUME ["/data"]
EXPOSE 7001
ENTRYPOINT ["stowd"]
