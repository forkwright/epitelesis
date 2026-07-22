# Security

## Reporting vulnerabilities

Do not open a public issue for a vulnerability. Use the repository's private
[GitHub security advisory form](https://github.com/forkwright/epitelesis/security/advisories/new).

Include a description, reproduction steps, potential impact, affected release
or commit, and any suggested remediation.

## Security boundary

Epitelesis enforces subprocess lifecycle policy; it is not a security sandbox.
On Unix the supervisor kills the process group before reaping so ordinary
descendants are cleaned up. A hostile child can call `setsid`, escape that
group, and survive. Use a purpose-built OS sandbox or container when executing
hostile code.

The v1 defaults reduce accidental exposure and resource exhaustion:

- `Clean` uses real environment clearing;
- environment allowlisting is explicit;
- inheriting the full environment requires a recorded reason;
- stdout and stderr are each limited to 10 MiB by default and limits fail
  closed; and
- deadlines are mandatory unless explicitly waived with a reason.

Explicit unbounded execution or capture is an exceptional policy choice, not a
safety guarantee. The caller owns the justification and the resulting resource
risk.

## Evidence handling

Captured stdout and stderr are untrusted bytes and may contain secrets. The
library transports them; callers own redaction, storage, retention, and safe
rendering. Tracing must not record argument values, inherited environment
values, or captured output by default.

Lifecycle errors retain aggregate evidence from termination, reap, and bounded
capture cleanup. Security-sensitive callers should inspect the whole typed
result instead of treating a timeout or cancellation label as proof that every
descendant was contained.

## In scope

- policy bypasses that allow execution without a declared deadline;
- environment clearing or allowlist failures;
- capture-limit, drain, cancellation, kill, or reap failures;
- evidence loss that masks incomplete cleanup; and
- dependency or build behavior that creates a practical vulnerability for
  consumers.

Misuse by a caller—such as executing an untrusted program without a sandbox,
passing unvalidated arguments, or publishing raw captured output—is outside the
library's security boundary unless Epitelesis makes the outcome worse.

## Disclosure

After a fix ships, maintainers may publish a GitHub Security Advisory with the
affected releases, impact, remediation, and reporter credit.
