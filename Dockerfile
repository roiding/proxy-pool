# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache gcc musl-dev
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux \
    go build -trimpath -ldflags='-s -w' -o /out/proxy-pool ./cmd/proxy-pool

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 10001 app \
    && adduser -D -u 10001 -G app app \
    && mkdir -p /var/lib/proxy-pool \
    && chown app:app /var/lib/proxy-pool
COPY --from=build /out/proxy-pool /usr/local/bin/proxy-pool
USER app
WORKDIR /var/lib/proxy-pool
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/proxy-pool"]
CMD ["-config", "/var/lib/proxy-pool/config.json"]
