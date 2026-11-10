# Multi-stage build for the MeterMesh node binary.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/metermesh ./cmd/server

FROM alpine:3.22
LABEL org.opencontainers.image.source="https://github.com/vinayrawat058/metermesh"
LABEL org.opencontainers.image.description="MeterMesh usage ingest node"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=build /out/metermesh /usr/local/bin/metermesh
EXPOSE 8080
ENTRYPOINT ["metermesh"]
