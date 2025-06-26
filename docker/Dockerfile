# Multi-stage build for Minare application with NTP capabilities
FROM eclipse-temurin:11-jre-alpine AS base

# Install required packages for NTP and health checks
RUN apk add --no-cache \
    curl \
    chrony \
    tzdata \
    bash \
    libcap

# Create application user and directory
RUN addgroup -S minare && adduser -S minare -G minare
RUN mkdir -p /app/logs && chown -R minare:minare /app

# Set timezone (can be overridden with environment variable)
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Switch to application user
USER minare

# Copy startup script
COPY --chown=minare:minare start.sh /app/start.sh

# Make startup script executable
USER root
RUN chmod +x /app/start.sh
USER minare

# Expose application ports (including debug port)
EXPOSE 8080 4225 4226 5005

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Use startup script as entrypoint
ENTRYPOINT ["/app/start.sh"]

# Default command expects JAR to be mounted at /app/app.jar
CMD ["java", "-jar", "/app/app.jar"]