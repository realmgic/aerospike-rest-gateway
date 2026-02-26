# Development Branches for Production Readiness

Tasks from the production-grade readiness plan are split across feature branches. Merge in order for a clean history.

| Branch | Theme | Main tasks |
|--------|--------|------------|
| `feature/ci-and-safety` | CI and safety | Run tests in package/Docker; CodeQL on PR/push; dependency scanning; package only after tests |
| `feature/docs-and-community` | Documentation | CONTRIBUTING.md, SECURITY.md, CHANGELOG, production/observability docs |
| `feature/testing-quality` | Testing | JaCoCo coverage threshold; JUnit 5 readiness |
| `feature/security-and-runtime` | Security and runtime | Configurable CORS; health/readiness; request validation; Docker non-root + HEALTHCHECK |
| `feature/operational-convenience` | Operations | docker-compose; SpotBugs; optional K8s examples |

## Merge order

1. `feature/ci-and-safety` → master  
2. `feature/docs-and-community` → master  
3. `feature/testing-quality` → master  
4. `feature/security-and-runtime` → master  
5. `feature/operational-convenience` → master  

Develop and review each branch independently; merge in this order to reduce conflicts.
