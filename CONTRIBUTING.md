# Contributing to Aerospike REST Gateway

Thank you for your interest in contributing. This document provides guidelines for building, testing, and submitting changes.

## Development setup

- **Java:** 17 or later
- **Build:** Gradle (use `./gradlew`; wrapper is in the repo)
- **Aerospike:** For integration tests, a running Aerospike server (e.g. 7.x) on the default host/port or as configured

## Building and testing

- **Build (no tests):** `./gradlew build -x test`
- **Build with tests:** `./gradlew build` (requires Aerospike for integration tests)
- **Run tests only:** `./gradlew test`
- **Run application locally:** `./gradlew bootRun` or `make run`
- **Package:** `make package` (runs doc validation, full build with tests; requires Aerospike and swagger-cli)

## Code style and quality

Use the existing code style and naming conventions. New code should be covered by tests where practical.

## Submitting changes

1. Use a feature branch (e.g. `feature/short-description`).
2. Use clear commit messages.
3. Open a PR against `master` and ensure CI passes.

## API and documentation

- OpenAPI spec: `./gradlew generateApiDocs` (output in `build/openapi.json`).
- Validate: `swagger-cli validate build/openapi.json` or `make validatedocs`.

For questions, open an issue. See also [README](README.md) and the [docs](docs/) folder.
