# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in barrel_vectordb, please report it responsibly.

**DO NOT** open a public issue for security vulnerabilities.

### How to Report

1. **Email**: Send details to security@barrel-db.eu
2. **Include**:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Any suggested fixes (optional)

### What to Expect

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 5 business days
- **Resolution Timeline**: Depends on severity
  - Critical: 7 days
  - High: 14 days
  - Medium: 30 days
  - Low: 90 days

### Scope

This policy applies to:
- barrel_vectordb core library
- HTTP Gateway API
- Vector index backends (HNSW, FAISS, DiskANN)
- Multi-tenant isolation
- Storage layer

### Out of Scope

- Issues in dependencies (report to upstream)
- Self-hosted deployment misconfigurations
- Social engineering attacks

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.x     | Yes       |

## Security Best Practices

When deploying barrel_vectordb:

1. **Network Security**: Use TLS for HTTP endpoints
2. **API Keys**: Use strong, unique API keys for multi-tenant gateway
3. **Tenant Isolation**: Verify tenant isolation is properly configured
4. **Rate Limiting**: Enable rate limiting in production
5. **Updates**: Keep dependencies updated
6. **Monitoring**: Enable metrics and alerting

## Acknowledgments

We appreciate responsible disclosure and will acknowledge security researchers who report valid vulnerabilities (unless they prefer to remain anonymous).
