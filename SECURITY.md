# Security

## Reporting vulnerabilities

**Do not open a public GitHub issue for security vulnerabilities.**

Report privately via GitHub's security advisory system:

> https://github.com/forkwright/epitelesis/security/advisories/new

Include: description, reproduction steps, potential impact, affected
version or commit, and any suggested fix.

## Response SLA

| Severity | Acknowledgment | Fix Target |
|----------|----------------|------------|
| Critical (CVSS >= 9.0) | 24 hours | 7 days |
| High (CVSS 7.0-8.9) | 48 hours | 14 days |
| Medium (CVSS 4.0-6.9) | 5 days | 30 days |
| Low (CVSS < 4.0) | 10 days | 90 days |

## Scope

**In scope:**

- Argument-quoting, environment, or working-directory handling bugs in the
  `Command` builder that could trigger unintended subprocess behaviour.
- Timeout / wait / kill logic failures that could leak child processes,
  block indefinitely, or misreport `Error::Timeout`.
- Capture-path bugs that could leak credentials in stdout/stderr beyond the
  caller's intended sink.
- Build-script or dependency behaviour that creates a practical
  vulnerability in epitelesis consumers.

**Out of scope:**

- Social engineering.
- Physical access attacks.
- Vulnerabilities that require arbitrary local code execution before
  epitelesis APIs are called.
- Misuse of the wrapper by a caller (passing untrusted input as `program`
  or unvalidated args, etc.). Those are caller-side issues; report to the
  consuming repo.
- Issues only present in upstream dependencies and not made worse by
  epitelesis; report those upstream. Epitelesis will patch promptly when a
  dependency fix is available.

## Disclosure

After a fix ships, we publish a GitHub Security Advisory when warranted,
including affected versions, fixed version, impact, remediation, and credit
to the reporter.

## Security Standards

Epitelesis follows the fleet security standards maintained in
`kanon/crates/basanos/standards/SECURITY.md`. In particular:

- Do not log credentials, tokens, or sensitive arguments unless explicitly
  redacted. The tracing spans this crate opens record `program` and
  `arg_count` — never the argument values.
- Treat the captured `Output.stdout` / `Output.stderr` as untrusted byte
  buffers in callers; epitelesis is a transport, not a sanitiser.
