.PHONY: build test test-unit test-integration run clean

build:
	go build -o bin/pgtest-transient ./cmd/pgtest

test: test-unit test-integration

test-unit:
	go test -v ./pkg/... ./internal/...

test-integration:
	go test -v ./test/integration/... -tags=integration

run: build
	./bin/pgtest-transient

clean:
	rm -rf bin/
