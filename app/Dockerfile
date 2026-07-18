# Stage 1: Build React frontend
FROM node:20-slim AS web-builder
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# Stage 2: Build Go binary
FROM golang:1.23 AS go-builder
WORKDIR /app
COPY app/go.mod app/go.sum ./
RUN go mod download
COPY app/ ./
COPY --from=web-builder /app/web/dist ./web/dist/
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app .

# Stage 3: Minimal runtime
FROM gcr.io/distroless/static-debian12
COPY --from=go-builder /bin/app /app
EXPOSE 8080
ENTRYPOINT ["/app"]
