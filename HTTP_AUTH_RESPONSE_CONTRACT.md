# Explicit HTTP authentication decisions

This qualification candidate intentionally tightens TBMQ 2.4.1's documented
HTTP response contract. A 2xx status alone, an absent/null/non-object JSON body,
a missing/unknown result, or invalid supplied client type/rules cannot approve
CONNECT or fall through to another provider. The provider returns FAILURE.
Only an explicit SUCCESS grants admission. Explicit FAILURE denies it; explicit
SKIPPED retains the provider-chain meaning. Transport failures retain their
existing SKIPPED behavior, so Photon must keep every fallback provider disabled
while HTTP stays enabled and TLS client certificates remain mandatory.

Optional client type and permissions may still use the configured defaults after
an explicit success. Photon must configure empty default publish/subscribe lists,
and the gate supplies the actual DEVICE/APPLICATION role and topic permissions.
Supplied client types accept only DEVICE and APPLICATION; invalid provided rules
are rejected instead of being replaced by defaults. No database, cache, extra
network hop, or compatibility flag is introduced.

This is a deliberate behavior change for integrations that use an empty 2xx or
omit result. Those integrations must return an explicit verdict before adopting
this candidate. Upstream currently documents the permissive behavior:
https://tbmq.io/docs/security/authentication/http/

The real-HTTP regression suite has 18 cases: 13 malformed-policy cases fail on
the original candidate, with five valid explicit verdict/default controls.
After-change checks, real MQTT integration and qualification-rig measurements
must be recorded separately before deployment or PR promotion. No live rig
broker was changed to establish the before result.
