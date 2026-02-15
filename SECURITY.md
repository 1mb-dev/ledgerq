# Security Policy

## Supported Versions

We maintain security updates for the latest minor version series.

| Version | Supported          |
| ------- | ------------------ |
| 1.4.x   | :white_check_mark: |
| 1.3.x   | :white_check_mark: |
| 1.2.x   | :white_check_mark: |
| 1.1.x   | :white_check_mark: |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take security issues seriously. If you discover a security vulnerability, please follow these steps:

### How to Report

1. **Do NOT** open a public GitHub issue for security vulnerabilities
2. Use GitHub Security Advisories: https://github.com/1mb-dev/ledgerq/security/advisories/new
3. Provide detailed information about the vulnerability:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Any suggested fixes (optional)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your report within 48 hours
- **Investigation**: We will investigate and assess the severity of the issue
- **Updates**: We will keep you informed of our progress
- **Resolution**: Once fixed, we will:
  - Release a patch
  - Publish a security advisory
  - Credit you for the discovery (unless you prefer to remain anonymous)

### Security Best Practices

When using LedgerQ:

1. **File Permissions**: Queue files are created with 0644 permissions (world-readable). Place queue directories in protected locations with restrictive parent directory permissions (e.g., 0700). See [USAGE.md](docs/USAGE.md#security) for examples.
2. **Data Validation**: Validate and sanitize all data before enqueueing
3. **Resource Limits**: Set appropriate limits on queue size and message size
4. **Access Control**: Restrict access to queue directories to authorized processes only
5. **Monitoring**: Monitor queue statistics for unusual activity

For a complete security analysis including audit findings and tool results, see [SECURITY_AUDIT.md](docs/SECURITY_AUDIT.md).

### Scope

Security issues we consider in scope:
- Data corruption or loss vulnerabilities
- Unauthorized access to queue data
- Denial of service through resource exhaustion
- Memory safety issues
- Race conditions leading to data inconsistency

Out of scope:
- Issues in third-party dependencies (report to the respective projects)
- Social engineering attacks
- Physical access to storage media

## Vulnerability Disclosure Policy

We follow coordinated vulnerability disclosure:
- We request 90 days to fix critical issues before public disclosure
- We will work with reporters to establish appropriate timelines
- We will publicly acknowledge security researchers who report issues responsibly

Thank you for helping keep LedgerQ and its users secure.
