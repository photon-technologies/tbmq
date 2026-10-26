# TBMQ (Photon fork)

Photon's fork of thingsboard/tbmq, the MQTT 3.1/3.1.1/5.0 broker that carries all Photon device and
service MQTT traffic. Java 25 (`maven.compiler.release` in `pom.xml`), Spring Boot, Kafka for message
persistence and delivery, PostgreSQL, Redis/Valkey, Netty; modules `application`, `common`,
`netty-mqtt`, `dao`, `integration`, `msa` (Docker images) and `ui-ngx`. `main` tracks upstream;
the qualified Photon changes are on `rig/2.4.1-qualification` (PR #1 into `release-2.4.1`).
Device and service admission is decided by the Photon HTTP gate (rabbit_http_auth_backend).

## Scaling and production guardrails

Read the repository-root `SCALING-GUARDRAILS.md` for dated measurements, the inspected source snapshot, unresolved adoption requirements and the review checklist. Keep caller identity separate from metric labels and pool-equivalent allowances separate from individual connections. Source configuration is not deployed proof.
