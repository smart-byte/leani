ARG RUST_VERSION=1.97.0
FROM rust:${RUST_VERSION}-bookworm AS builder

ARG LEANI_GIT_COMMIT=unknown
WORKDIR /source
COPY . .
ENV CARGO_INCREMENTAL=0
ENV CARGO_PROFILE_RELEASE_STRIP=symbols
ENV LEANI_GIT_COMMIT=${LEANI_GIT_COMMIT}
ENV RUSTFLAGS="--remap-path-prefix=/usr/local/cargo=/cargo"
ENV CFLAGS="-ffile-prefix-map=/usr/local/cargo=/cargo"
ENV CXXFLAGS="-ffile-prefix-map=/usr/local/cargo=/cargo"
RUN cargo build --locked --release -p leani

FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="leani" \
      org.opencontainers.image.source="https://github.com/smart-byte/leani" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 leani \
    && useradd --uid 10001 --gid leani --no-create-home --shell /usr/sbin/nologin leani \
    && install --directory --owner leani --group leani /var/lib/leani /tmp/leani

COPY --from=builder /source/target/release/leani /usr/local/bin/leani
# THIRD_PARTY_LICENSES.txt is committed and verified against Cargo.lock in CI.
COPY LICENSE THIRD_PARTY_LICENSES.txt /usr/share/licenses/leani/
COPY deploy/container.toml /etc/leani/node.toml
COPY config/modes /etc/leani/examples

USER 10001:10001
VOLUME ["/var/lib/leani"]
EXPOSE 8080 8545 8546

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD ["curl", "--fail", "--silent", "http://127.0.0.1:8080/health/live"]

ENTRYPOINT ["/usr/local/bin/leani"]
CMD ["serve", "--config", "/etc/leani/node.toml", "--log-format", "json"]
