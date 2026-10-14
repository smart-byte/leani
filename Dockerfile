# Base images are pinned to multi-architecture index digests, which
# Dependabot refreshes. The Rust tag must name the rust-toolchain.toml version;
# scripts/check-container-toolchain.rb enforces this in CI.
FROM rust:1.99.0-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS builder

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

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS runtime

LABEL org.opencontainers.image.title="leani" \
      org.opencontainers.image.source="https://github.com/smart-byte/leani" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get upgrade --yes \
    && apt-get install --yes --no-install-recommends ca-certificates curl libjemalloc2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 leani \
    && useradd --uid 10001 --gid leani --no-create-home --shell /usr/sbin/nologin leani \
    && install --directory --owner leani --group leani /var/lib/leani /tmp/leani

COPY --from=builder /source/target/release/leani /usr/local/bin/leani
# THIRD_PARTY_LICENSES.txt is committed and verified against Cargo.lock in CI.
COPY LICENSE THIRD_PARTY_LICENSES.txt /usr/share/licenses/leani/
COPY deploy/container.toml /etc/leani/node.toml
COPY config/modes /etc/leani/examples

# The root filesystem may be read-only: SQLite spills large statements and
# sorts here, the one writable temporary directory.
ENV SQLITE_TMPDIR=/tmp/leani TMPDIR=/tmp/leani
# glibc keeps what threads free in per-thread arenas, so resident memory stays
# near its peak and grows with new threads. jemalloc returns it: a 100,000-block
# Xatu backfill held 711 MiB and fell to 130 MiB afterwards, where glibc reached
# 1.5 GiB. Debian's arm64 build supports kernel pages up to 64 KiB.
ENV LD_PRELOAD=libjemalloc.so.2

USER 10001:10001
VOLUME ["/var/lib/leani"]
EXPOSE 8080 8545 8546

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD ["curl", "--fail", "--silent", "http://127.0.0.1:8080/health/live"]

ENTRYPOINT ["/usr/local/bin/leani"]
CMD ["serve", "--config", "/etc/leani/node.toml", "--log-format", "json"]
