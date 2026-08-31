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
COPY --from=duckdb /out/duckdb /usr/local/bin/duckdb
ENTRYPOINT ["/usr/local/bin/mrfpipeline"]
