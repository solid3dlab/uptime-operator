# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.0-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/uptime-operator ./cmd/uptime-operator

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/uptime-operator /uptime-operator
USER 65532:65532
ENTRYPOINT ["/uptime-operator"]
