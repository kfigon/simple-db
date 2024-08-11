.phony: clean
clean:
	go clean --testcache

.phony: test
test:
	go test ./... -v -timeout 5s

.phony: run
run:
	go run .