# Security

## Reporting a vulnerability

If you believe you have found a security vulnerability, please report it.

**How to report:** Open a [GitHub Security Advisory](https://github.com/aerospike/aerospike-rest-gateway/security/advisories/new) or contact maintainers privately. Include a clear description, steps to reproduce, and impact.

**What to expect:** We will acknowledge the report and work with you on timing and fixes where appropriate.

## Scope

**Aerospike REST Gateway is a community OSS project.** As noted in the [README](README.md) and [Product Stages](https://aerospike.com/docs/database/reference/product-stages#open-source-products-tools-and-libraries), Aerospike does not provide official support or vulnerability patching. Community maintainers will triage and address reports within the scope of the project.

## Secure configuration

For production: use TLS for gateway-to-Aerospike when possible, restrict CORS, do not commit credentials (use env or secrets manager), and keep dependencies and runtime up to date. See [Installation and Configuration](docs/installation-and-config.md).
