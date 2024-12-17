# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Set working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -o /gohustle

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache tzdata ca-certificates gzip

# Set timezone
ENV TZ=Asia/Kolkata

# Create directories
RUN mkdir -p /data/exports

# Copy the binary from builder
COPY --from=builder /gohustle /usr/local/bin/

# Create non-root user
RUN adduser -D -h /app appuser
USER appuser

WORKDIR /app

# Command to run
CMD ["gohustle"] 