.PHONY: dev-web dev-app build build-web test docker-up docker-down clean build-flink test-flink verify

# Run Vite dev server (port 5173, proxies /api to localhost:8080)
dev-web:
	cd web && npm run dev

# Run Go server (port 8080)
dev-app:
	cd app && go run .

# Build React then Go binary
build: build-web
	cd app && go build -o bin/app .

build-web:
	cd web && npm run build

build-flink:
	mvn -f flink/pom.xml clean package

# Run all Go tests
test:
	cd app && go test ./... -v

test-flink:
	mvn -f flink/pom.xml test

verify:
	cd app && go test ./...
	cd web && npm run test:jakarta
	cd web && npm run lint
	cd web && npm run build
	mvn -f flink/pom.xml clean verify

# Start full stack via Docker Compose
docker-up:
	docker compose up --build

# Stop full stack
docker-down:
	docker compose down

# Clean build artifacts
clean:
	rm -rf app/bin app/web/dist web/node_modules
