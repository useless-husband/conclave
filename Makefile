GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
SEEDS ?= 1-2000
WORKERS ?= 4

.PHONY: build test short race lint sweep mutations e2e bench cluster stop clean

build:
	$(GO) build -trimpath -ldflags "-X main.version=$(VERSION)" -o bin/conclave ./cmd/conclave

# Everything, including the kill/restart test with real processes and the
# mutation test (several minutes).
test:
	$(GO) test -p 4 -timeout 60m ./...

# Fast subset: unit tests, a short seed sweep, a short end-to-end run.
short:
	$(GO) test -short -p 4 ./...

race:
	$(GO) test -race -short -p 4 ./...

lint:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "gofmt needed on the files above" && exit 1)
	$(GO) vet ./...
	$(GO) run honnef.co/go/tools/cmd/staticcheck@latest ./...

# Run the simulator over a range of seeds: make sweep SEEDS=1-10000
sweep:
	$(GO) run ./cmd/conclave sim -seeds $(SEEDS) -workers $(WORKERS)

# How many seeds the simulator needs to expose each injected bug.
mutations:
	$(GO) test ./internal/sim -run TestMutationsAreDetected -v -timeout 120m

e2e:
	$(GO) test ./test/e2e -v -count=1

# Throughput and latency of a 3-server cluster on this machine.
bench: build
	sh scripts/bench.sh

cluster:
	sh scripts/cluster.sh start

stop:
	sh scripts/cluster.sh stop

clean:
	rm -rf bin .cluster coverage.out
