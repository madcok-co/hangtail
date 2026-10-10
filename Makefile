.PHONY: build test tidy lint demo-up demo-down demo-tidy

build:
	go build ./...

test:
	go test -race ./...

bench:
	go test -run='^$$' -bench=. -benchtime=3s ./bench/...

tidy:
	go mod tidy

lint:
	golangci-lint run ./...

# demo-tidy: generate go.sum for the api-go service before the first docker build.
demo-tidy:
	cd demo/services/api-go && go mod tidy

# demo-up: start the full demo stack (run demo-tidy first if go.sum is missing).
demo-up: demo-tidy
	docker compose -f demo/docker-compose.yml up --build

demo-down:
	docker compose -f demo/docker-compose.yml down -v

chaos-test:
	./chaos_test.sh
