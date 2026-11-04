# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache file gcc musl-dev
ARG VERSION=dev
ARG VCS_REF=
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -buildvcs=false -trimpath \
      -ldflags="-s -w -linkmode=external -extldflags '-static -no-pie' -X main.version=${VERSION} -X main.vcsRef=${VCS_REF}" \
      -o /out/proxy-pool ./cmd/proxy-pool
RUN file /out/proxy-pool | grep -q 'statically linked'

FROM scratch AS release
COPY --from=build /out/proxy-pool /proxy-pool

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 10001 app \
    && adduser -D -u 10001 -G app app \
    && mkdir -p /var/lib/proxy-pool/data \
    && chown -R app:app /var/lib/proxy-pool
COPY --from=build /out/proxy-pool /usr/local/bin/proxy-pool
ARG VERSION=dev
ARG VCS_REF=
LABEL org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"
RUN set -eu; actual="$(/usr/local/bin/proxy-pool -version)"; \
    expected="${VERSION}"; \
    if [ -n "${VCS_REF}" ]; then expected="${VERSION} (${VCS_REF})"; fi; \
    test "$actual" = "$expected"
USER app
WORKDIR /var/lib/proxy-pool
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/proxy-pool"]
CMD ["-config", "/var/lib/proxy-pool/config.json"]
