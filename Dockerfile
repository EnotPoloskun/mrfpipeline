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

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/mrfpipeline /usr/local/bin/mrfpipeline
ENTRYPOINT ["/usr/local/bin/mrfpipeline"]
