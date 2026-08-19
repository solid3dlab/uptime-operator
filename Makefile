.PHONY: build test tidy image

IMAGE ?= ghcr.io/solid3dlab/uptime-operator
TAG ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/uptime-operator ./cmd/uptime-operator

test:
	go test ./...

tidy:
	go mod tidy

image:
	docker build -t $(IMAGE):$(TAG) -t $(IMAGE):latest .
