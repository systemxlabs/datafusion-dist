# Build stage
FROM rust:1.92-bookworm AS builder

WORKDIR /app

# Copy the entire workspace
# Note: Build this Dockerfile from the integration-tests root:
# docker build -f app/Dockerfile -t datafusion-dist-integration-tests-app .
COPY ../Cargo.toml ./
COPY ../Cargo.lock ./
COPY ../dist ./dist
COPY ../clusters ./clusters
COPY ../networks ./networks
COPY ../integration-tests ./integration-tests

# Build the application in release mode
RUN cargo build --release --package datafusion-dist-integration-tests-app

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder /app/target/release/datafusion-dist-integration-tests-app /app/datafusion-dist-integration-tests-app

# Expose the ports used by the application
# 50050: DistTonicService
# 50051: FlightSQL service
EXPOSE 50050 50051

# Set environment variables
ENV RUST_LOG=debug,datafusion_dist=debug,datafusion_dist_network_tonic=debug,datafusion_dist_cluster_postgres=debug

# Run the application
CMD ["./datafusion-dist-integration-tests-app"]
