# Build metadata. Values can be overridden by CI or by callers of make.
APP_NAME ?= plex-exporter
CMD_PATH ?= ./cmd/plex-exporter
.DEFAULT_GOAL := build
VERSION ?= $(shell ./tools/image-tag 2>/dev/null || echo dev)
GIT_REVISION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
GIT_BRANCH ?= $(shell git symbolic-ref --short -q HEAD 2>/dev/null || echo detached)

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

# These are the Unix-like targets that are built by the local build-all target
# and the CI matrix. Each target is available in both amd64 and arm64 in the
# Go toolchain used by this project.
UNIX_TARGETS ?= \
	linux/amd64 \
	linux/arm64 \
	darwin/amd64 \
	darwin/arm64 \
	freebsd/amd64 \
	freebsd/arm64

GO_LDFLAGS := -X main.Branch=$(GIT_BRANCH) -X main.Revision=$(GIT_REVISION) -X main.Version=$(VERSION) -s -w
GO_OPT := -mod=readonly -trimpath -ldflags "$(GO_LDFLAGS)"

IMAGE_NAME ?= ghcr.io/naterator/plex-exporter

### Development

.PHONY: run
run:
	go run $(CMD_PATH)

### Build

.PHONY: $(APP_NAME)
$(APP_NAME):
	@mkdir -p ./bin/$(GOOS)
	CGO_ENABLED=0 go build $(GO_OPT) -o ./bin/$(GOOS)/$(APP_NAME)-$(GOARCH) $(CMD_PATH)

.PHONY: build
build: $(APP_NAME)

.PHONY: build-all
build-all:
	@set -eu; \
	for target in $(UNIX_TARGETS); do \
		os=$${target%/*}; \
		arch=$${target#*/}; \
		echo "Building $(APP_NAME) for $$os/$$arch"; \
		$(MAKE) --no-print-directory GOOS=$$os GOARCH=$$arch $(APP_NAME); \
	done

.PHONY: exe
exe:
	$(MAKE) --no-print-directory GOOS=linux GOARCH=$(GOARCH) $(APP_NAME)

### Docker Images

.PHONY: docker-component
docker-component:
	docker build \
		--platform=linux/$(GOARCH) \
		--build-arg=VERSION=$(VERSION) \
		--build-arg=GIT_REVISION=$(GIT_REVISION) \
		--build-arg=GIT_BRANCH=$(GIT_BRANCH) \
		-t $(IMAGE_NAME):latest \
		-f ./Dockerfile .
	docker tag $(IMAGE_NAME):latest $(APP_NAME):latest

.PHONY: docker-plex-exporter
docker-plex-exporter: docker-component

.PHONY: docker-images
docker-images: docker-plex-exporter
