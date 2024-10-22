.phony: clean
clean:
	go clean --testcache

.phony: test-v
test-v:
	go test ./... -v -timeout 5s

.phony: test
test:
	go test ./... -timeout 5s

.phony: run
run:
	go run .