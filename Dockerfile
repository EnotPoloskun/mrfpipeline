# syntax=docker/dockerfile:1.7

FROM golang:1.26-bookworm AS build

WORKDIR /src
ENV GOPRIVATE=github.com/EnotPoloskun/*,github.com/enotpoloskun/mrfconsumer

COPY go.mod go.sum ./

# Private sibling modules are fetched through the caller's SSH agent. The
# agent socket is available only for this RUN instruction and is never copied
# into an image layer.
RUN --mount=type=ssh \
    git config --global url."git@github.com:".insteadOf "https://github.com/" && \
    GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new" go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/mrfpipeline ./cmd/mrfpipeline

# Linux mrfparser from the private Rust sibling
# github.com/EnotPoloskun/mrfparser_rust. Cross-compile from BUILDPLATFORM so
# arrow/parquet/zstd are not QEMU-emulated. Bump the rust builder pin together
# with MRFPARSER_RUST_REV.
FROM --platform=$BUILDPLATFORM rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS mrfparser

ARG TARGETARCH
ARG MRFPARSER_RUST_REV=d1a66424bf896074a9505f1ebe709f86a4cc18a1

WORKDIR /src
RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        ca-certificates \
        git \
        gcc \
        g++ \
        gcc-x86-64-linux-gnu \
        g++-x86-64-linux-gnu \
        gcc-aarch64-linux-gnu \
        g++-aarch64-linux-gnu \
        libc6-dev-amd64-cross \
        libc6-dev-arm64-cross \
        pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=ssh \
    git config --global url."git@github.com:".insteadOf "https://github.com/" && \
    GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new" \
      git clone git@github.com:EnotPoloskun/mrfparser_rust.git . && \
    git checkout --detach "$MRFPARSER_RUST_REV" && \
    test "$(git rev-parse HEAD)" = "$MRFPARSER_RUST_REV"

RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) rust_target=x86_64-unknown-linux-gnu; export CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc ;; \
      arm64) rust_target=aarch64-unknown-linux-gnu; export CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc ;; \
      *) echo "unsupported target architecture" >&2; exit 1 ;; \
    esac; \
    rustup target add "$rust_target"; \
    cargo build --release --locked --target "$rust_target"; \
    install -d /out; \
    install -m 0755 "target/${rust_target}/release/mrfparser" /out/mrfparser

FROM debian:bookworm-slim AS duckdb

ARG TARGETARCH

RUN set -eu; \
    duckdb_version='v1.5.5'; \
    apt-get update; \
    apt-get install --no-install-recommends --yes ca-certificates curl unzip; \
    case "$TARGETARCH" in \
      amd64) duckdb_archive='duckdb_cli-linux-amd64.zip'; duckdb_sha256='08c0ca117111fcede14239d0093792352befdc174218c344d232c13279643d05' ;; \
      arm64) duckdb_archive='duckdb_cli-linux-arm64.zip'; duckdb_sha256='02163197027a42149147364d31fa67cac82108517a4be43304a1cc226eaef07a' ;; \
      *) echo "unsupported target architecture" >&2; exit 1 ;; \
    esac; \
    archive_path="/tmp/$duckdb_archive"; \
    extract_path=/tmp/duckdb-extract; \
    curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
      "https://github.com/duckdb/duckdb/releases/download/${duckdb_version}/${duckdb_archive}" \
      --output "$archive_path"; \
    printf '%s  %s\n' "$duckdb_sha256" "$archive_path" | sha256sum --check --status; \
    mkdir "$extract_path"; \
    unzip -q "$archive_path" -d "$extract_path"; \
    duckdb_path="$extract_path/duckdb"; \
    test -x "$duckdb_path"; \
    duckdb_reported_version="$("$duckdb_path" --version)"; \
    test "${duckdb_reported_version%% *}" = "$duckdb_version"; \
    install -d /out; \
    install -m 0755 "$duckdb_path" /out/duckdb; \
    rm -rf "$archive_path" "$extract_path" /var/lib/apt/lists/*

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

COPY --from=build /out/mrfpipeline /usr/local/bin/mrfpipeline
COPY --from=mrfparser /out/mrfparser /usr/local/bin/mrfparser
COPY --from=duckdb /out/duckdb /usr/local/bin/duckdb
ENTRYPOINT ["/usr/local/bin/mrfpipeline"]
