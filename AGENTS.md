# TBMQ (Photon fork)

Photon's fork of thingsboard/tbmq, the MQTT 3.1/3.1.1/5.0 broker that carries all Photon device and
service MQTT traffic. Java 25 (`maven.compiler.release` in `pom.xml`), Spring Boot, Kafka for message
persistence and delivery, PostgreSQL, Redis/Valkey, Netty; modules `application`, `common`,
`netty-mqtt`, `dao`, `integration`, `msa` (Docker images) and `ui-ngx`. `main` tracks upstream;
the qualified Photon changes are on `rig/2.4.1-qualification` (PR #1 into `release-2.4.1`).
Device and service admission is decided by the Photon HTTP gate (rabbit_http_auth_backend).

## Scaling and production guardrails

Proven in the October 2026 qualification at 10K/15K/100K devices:

- Admit only on an explicit HTTP SUCCESS verdict; an empty, `result`-less or malformed verdict, or
  invalid roles or rules, is FAILURE (CONNACK 135). The stock broker admitted 7 invalid cases.
  Keep HTTP the only provider, fallbacks disabled, client certificates required, default topic
  permissions empty.
- Bound the HTTP auth request lifetime, and evict idle pooled connections to the gate
  (`HttpAuthClient`; 30 s idle / 5 min life / 15 s eviction on `fix/http-auth-pool-idle-eviction`).
  A day-idle connection caused a fail-closed `Connection reset` on staging (2026-10-20).
- The publish-message consumer uses `auto.offset.reset=earliest` (`KafkaPublishMsgQueueFactory`);
  every new consumer group sets its offset reset explicitly.
- Register APPLICATION shared subscriptions through `/api/app/shared/subs` with `cleanStart=false`
  and session expiry 86,400 s, and size partitions up front: a single partition put OTA fetch on
  1 of 12 consumers at 100K (633 QoS0 overloads), and the count can't change without recreation.
- Broker JVM at 100K: `-Xms2g -Xmx6g -XX:MaxDirectMemorySize=1g -XX:+ExitOnOutOfMemoryError`
  (about 33K sessions per broker; r20: 0 reconnects). The broker never autoscales.
- Device admission stays CN == UUID == clientId; no registry, environment or denylist checks.
- Base images are digest-pinned and multi-arch; the 56 inherited HIGH/CRITICAL findings are
  tracked, never waived.

Full list and review checklist: SCALING-GUARDRAILS.md. Reviewers must apply its checklist; a PR that weakens a listed value needs measured evidence at the scale it affects.
