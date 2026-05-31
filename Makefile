.PHONY: all build test clean lint image-server image-client images

BIN_DIR       := bin
BINARY_SERVER := $(BIN_DIR)/server
BINARY_CLIENT := $(BIN_DIR)/client

KO_DOCKER_REPO ?= ko.local

all: build

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BINARY_SERVER) ./cmd/server
	go build -o $(BINARY_CLIENT) ./cmd/client

test:
	go test -v -race ./...

lint:
	go vet ./...

clean:
	rm -rf $(BIN_DIR)

image-server:
	KO_DOCKER_REPO=$(KO_DOCKER_REPO) ko build --base-import-paths ./cmd/server

image-client:
	KO_DOCKER_REPO=$(KO_DOCKER_REPO) ko build --base-import-paths ./cmd/client

images: image-server image-client
