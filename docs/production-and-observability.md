# Production and Observability

This document describes recommended settings for running the Aerospike REST Gateway in production, including health checks, metrics, and management endpoints.

## Spring Boot Actuator

The application includes Spring Boot Actuator and Micrometer for health and metrics. By default, only a subset of endpoints may be exposed; configure them explicitly for production.

### Health endpoints

- **`/actuator/health`** — Aggregate health (e.g. UP/DOWN). Safe to expose internally.
- **`/actuator/health/liveness`** — Kubernetes liveness probe: is the process alive?
- **`/actuator/health/readiness`** — Kubernetes readiness probe: is the app ready to accept traffic? Can reflect Aerospike connectivity.

Example configuration (e.g. in `application.properties` or via environment):

```properties
# Expose health and liveness/readiness (recommended for Kubernetes)
management.endpoints.web.exposure.include=health,info
management.endpoint.health.probes.enabled=true
management.endpoint.health.show-details=when-authorized
```

Then use in Kubernetes:

- **Liveness:** `http://<pod>:8080/actuator/health/liveness`
- **Readiness:** `http://<pod>:8080/actuator/health/readiness`

### Prometheus metrics

If you use Prometheus, the registry is on the classpath. Expose the Prometheus endpoint only on an internal port or path and restrict access:

```properties
management.endpoints.web.exposure.include=health,info,prometheus
management.metrics.export.prometheus.enabled=true
```

Scrape URL: `http://<host>:8080/actuator/prometheus`

### Securing actuator

Do not expose actuator publicly without protection. Options:

- Restrict exposure (e.g. `management.server.port` on a different port with firewall rules).
- Use Spring Security to allow actuator only from internal IPs or with authentication.
- Keep `show-details` for health as `when-authorized` or `never` in production.

## Verifying gateway and cluster

A simple way to verify that the gateway is up and can reach Aerospike is:

```bash
curl http://localhost:8080/v1/cluster
```

For readiness, prefer `/actuator/health/readiness` when actuator is enabled, as it can reflect dependency health.

## Logging

Configure log level and output via standard Spring Boot properties, for example:

```properties
logging.level.root=INFO
logging.level.com.aerospike=INFO
logging.file.name=/var/log/aerospike-rest-gateway/application.log
```

Adjust levels and paths for your environment.

## Production checklist

- Enable and restrict actuator endpoints (health, optionally Prometheus).
- Use TLS for the gateway→Aerospike connection when required (see [Installation and Configuration](installation-and-config.md#tls-configuration)).
- Restrict CORS to your front-end origins when configurable (see installation docs).
- Do not commit credentials; use environment variables or a secrets manager.
- Set appropriate JVM memory and GC options when running the JAR or container.
