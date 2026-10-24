# Scaling and production guardrails

Proven in the October 2026 qualification at 10K, 15K and 100K devices. Change a value only with new measured evidence.

## Platform-wide rules (identical in every Photon repo)

These held the platform at 10K, 15K and 100K devices in the October 2026 qualification campaign, on AWS Graviton and on k3s dev. A change that weakens one needs a measured reason in the PR and new evidence at the scale it affects.

- **A merge to `dev` is a deployment.** Each merge is followed by its deploy run, a rollout check, pods Ready with 0 restarts, migrations, logs compared with the baseline, and one functional check. Merges go one at a time. If something fails, roll that step back; never leave dev broken.
- **No unbounded growth.** Every query has a page size or deadline. Every Kafka topic has an age and size bound. Every ClickHouse history table has a TTL. Every log group has a retention. Every metric label has a finite vocabulary. Every cache has a TTL or a size bound. Every retry has a cap.
- **Time is epoch microseconds end to end** (`device_ts`, `deviceTs`, ClickHouse `DateTime64(6)`). Never a millisecond type, a unit guess or a tolerance band.
- **One transport and one record.**
  - MQTT goes through TBMQ; RabbitMQ is retired.
  - Current device state lives in State (Valkey: durable, `noeviction`).
  - There are no compacted Kafka state topics, and no ClickHouse current-state tables.
- **Connection budgets are sums.** For every PostgreSQL user, (pool max × maximum replicas) + 2 transient slots + admin reserve must fit `max_connections`. Pools never renew in lock-step: no short identical max lifetimes. Core runs a 24 h max lifetime.
- **Per-caller rate limits are sized for aggregating callers, not for users.** The State API is 600/s with a burst of 1,200 per caller per pod. 200/s caused 20,258 HTTP 503s at 100K.
- **One scaling owner per workload.**
  - HPA on CPU for serving pods.
  - KEDA on Kafka lag for consumers.
  - Ceilings are bounded by partitions, the database budget and downstream capacity.
  - Stateful stores (Kafka, TBMQ, ClickHouse, Keeper, Valkey) don't autoscale.
- **Requests cover the measured working set, and JVM ergonomics match the container limit.**
- **Images are multi-arch (amd64 + arm64) and pinned by digest,** on the current base: `platform/java-runtime:21`, or `:21-native-crypto` where native crypto is needed. HIGH/CRITICAL scan findings are fixed, never waived.
- **Observability is part of the contract.**
  - Metric and tag names that dashboards and alerts use are never renamed silently.
  - `service.instance.id` stays per pod.
  - Client aborts are logged as cancelled, not as 500.
  - No log line per consumed record at INFO.
  - Trace sampling stays bounded after idle periods (photon-platform 2.16.2 hotfix; 3.0.2 or later).
- **Reactive code never blocks:** no `.block()`, and no DB connection held across continuations. Kafka handlers propagate failures; they never swallow a DB error and commit.
- **Migrations are backward-compatible.** A schema contraction lands only after no running code depends on it.
- **Secrets are referenced, never written down.** They are generated in place and delivered by External Secrets.
- **Dev-only shortcuts never reach prod.** Examples: OTA `skipSignatureVerify`, single-replica Prometheus on emptyDir, rig certificate lifetimes.
- **Evidence over assertion.**
  - A fix's test must fail on the pre-fix code.
  - A config change is proven on the running object, not only in the file.
  - A dashboard counts as working only when the deployed Grafana renders the panel with data.
  - Results are recorded exactly as observed, never relabelled.

## Review checklist (reviewers and AI review: block the PR if a box can't be ticked)

- [ ] **Unbounded growth:** no new unbounded query, topic, table, cache, label or retry. Bounds are stated.
- [ ] **Guarded values:** none of the values in this repo's guardrails section are weakened. If one changes, the PR gives the measurement and the scale it was proven at.
- [ ] **Connection budget:** pool, replica or HPA/KEDA changes come with the updated connection-budget sum.
- [ ] **Timestamps:** microseconds are preserved on every path that touches device time.
- [ ] **Blocking and failures:** no blocking call in reactive code, and no swallowed failure in a consumer.
- [ ] **Metrics:** no rename or removal of a metric, tag or log field used by a dashboard or alert. Any new metric label has bounded values.
- [ ] **Images:** base and Dockerfile changes keep the digest pin, both architectures and the scan gate.
- [ ] **Migrations:** backward-compatible, or declared roll-forward-only before merge.
- [ ] **Dev-only settings:** no dev-only flag or value is copied into staging or prod configuration.
- [ ] **Tests:** they fail without the fix, and the PR says how the change will be verified on dev after the deploy.

## This repository: keep these

**Where the code is.** `main` mirrors the upstream 2.4.1 release plus docs. The qualified Photon changes live on branch `rig/2.4.1-qualification` (PR #1 into `release-2.4.1`); the idle-eviction fix is on `fix/http-auth-pool-idle-eviction`. Paths below name files on those branches. On `main` each item is a requirement, not yet present. "Deploy-side" means the value lives in the broker Helm values or ConfigMaps (kub-server-config, aws-fresh), not in this repo.

### HTTP authentication (the gate contract)

- **Admit only on an explicit allow (MQ-1).** `HttpMqttClientAuthProvider` treats a 2xx alone, an absent, null or non-object body, a missing or unknown `result`, or invalid client type or rules as FAILURE; only an explicit SUCCESS admits (CONNACK 135 otherwise). The stock broker admitted 7 invalid-response cases over mutual-TLS MQTT; the before-change regression recorded 13 failures in 18 real-HTTP verdict cases (`HttpAuthExplicitVerdictTest`, contract in `HTTP_AUTH_RESPONSE_CONTRACT.md`). This deliberately tightens the upstream contract (empty body = success). Deploy-side: HTTP is the only credential provider, fallback providers disabled, TLS client certificates required, empty default publish/subscribe permissions; only the gate supplies roles and topic rules. On `main`, `HttpMqttClientAuthProvider` still has the upstream behavior.
- **Bound the whole HTTP auth request and stop after an admission rejection.** `HttpAuthClient` refuses a non-positive read timeout and bounds the complete request lifetime (`HttpAuthDeadlineTest`); it stops authentication after an admission rejection (`HttpAuthClientAdmissionTest`).
- **Evict idle pooled connections to the gate (MQ-5).** After about a day idle, staging TBMQ hit `WebClientRequestException: Connection reset` (`CONNECTION_REFUSED_NOT_AUTHORIZED`, 2026-10-20 21:26:31Z) and failed closed on a valid device. On `fix/http-auth-pool-idle-eviction` (commit `ebb9bab`), `HttpAuthClient.PoolSettings` closes connections idle for 30 s, recycles them after 5 min and evicts every 15 s (`TB_HTTP_AUTH_CLIENT_POOL_MAX_IDLE_TIME_MS`, `..._MAX_LIFE_TIME_MS`, `..._EVICTION_INTERVAL_MS`; 0 disables), tested by `HttpAuthPoolIdleEvictionTest`. That branch's PR #2 is closed and the fix is on no maintained branch; the gate-side counterpart is rabbit_http_auth_backend PR #54 (60 s server idle timeout). Keep any client idle limit below the gate's. The catalogue has no qualified value [GAP].
- **Gate outages fail closed (MQ-3).** `r4-tbmq-outage-r1` proved device and service CONNECTs are refused while the gate is down. Re-prove with the 27-case / 360-assertion ARM matrix on all 3 brokers (`aacd6d204a560e79`) after any auth-path change.
- **Device admission stays simple (MQ-4, user decision 2026-10-21).** CN is a UUID, clientId == CN, own-topic ACLs only. Don't add registry, environment or denylist checks at CONNECT.

### Kafka and shared subscriptions

- **The publish-message consumer starts at `earliest` (MQ-2).** `KafkaPublishMsgQueueFactory.createConsumer` applies `requiredConsumerProperties` (`auto.offset.reset=earliest`, from `AbstractQueueFactory`), because PUBACK can precede the group's first partition assignment. On real Kafka 4.2 the old JAR skipped an already-acknowledged record at offset 0; not proven to be the cause of the failed upgrade attempts r1/r4 [UNVERIFIED as cause]. Test: `KafkaPublishMsgQueueFactoryTest`. On `main`, `createConsumer` does not apply the override.
- **APPLICATION shared subscriptions are registered natively and sized up front (MQ-7).** Register them through `/api/app/shared/subs`; consumers use `cleanStart=false` with a positive session expiry (86,400 s). A shared subscription's partition count can only change by recreating it, which also recreates and loses its Kafka transport topic. Loopback r3 missed all 11 upstream deliveries with `cleanStart=true` and expiry 0. At native 100K r4, one partition put OTA fetch on 1 of 12 Telemetry subscribers: 99,367 of 100,000 boot OTA messages arrived, with 633 QoS0 overloads. OTA fetch/status/progress went from 1 to 12 partitions, RF3, min ISR 2, 24 h / 1 GiB per partition.

### Sizing and runtime (deploy-side)

- **Size the broker JVM by sessions per broker (MQ-6).** 10K/15K (Helm 213): `-Xms512m -Xmx2g -XX:MaxDirectMemorySize=512m`, request 500m / 3Gi, limit 4Gi. 100K: `-Xms2g -Xmx6g -XX:MaxDirectMemorySize=1g -XX:+ExitOnOutOfMemoryError`, about 33K device sessions per broker; heap used 3.9–4.2 GiB per pod, ramp peak 2.89 cores per pod. This qualified 100K devices with 0 reconnects and 0 protocol violations (r20) and 4,022,740 acknowledged QoS1 messages (r17). Keep `ExitOnOutOfMemoryError` (also exported in this repo's `k8s/*/tbmq-configmap.yml`).
- **The broker is stateful and does not autoscale.** Scale it by planned capacity, not HPA. Staging runs a single broker (accepted limit).
- **Database connections:** TBMQ held 40 of 320 slots in the Helm 213 PostgreSQL budget. Pool or replica changes come with the updated budget sum.

### Images and claims

- **Track the inherited CVEs; don't waive them (MQ-12).** The TBMQ image carried 56 inherited HIGH/CRITICAL findings, unresolved and not waived. The qualification branch pins the base by digest (`docker.base.image` in `msa/pom.xml`: `amazoncorretto:25-al2023-headless@sha256:2cffa0f4…`); `main` uses the unpinned `thingsboard/openjdk25:trixie-slim`.
- **Check native code per architecture (IM-2):** MQTT TLS, native crypto and LZ4 on amd64 and arm64. Commit `829a577` on the qualification branch adds native ARM and AMD runtime support.
- **Simulator proof is not hardware proof (MQ-13).** Don't claim device TLS, heap or power-loss behavior from the simulator. Open: confirm firmware trusts the Let's Encrypt chain ending at ISRG Root X2 on the dev broker.

### Process

- A fix's test must fail on the pre-fix code (QP-14); the verdict regression above is the model. Tests never depend on the wall clock (BD-11). Never bypass the scan gate (IM-4).

## Never do here

- A17: single-partition shared subscriptions (OTA fetch on 1 of 12 consumers; 633 QoS0 overloads).
- A23: a new consumer group defaulting to `latest` (an acknowledged record skipped in the Kafka 4.2 proof).
- A36: wall-clock-dependent test fixtures (BDI time-bomb fixtures caught by dev gates).
- Not an A-number, from MQ-1 and MQ-5: treating an empty or `result`-less HTTP verdict as success, letting a malformed verdict fall through to another provider, or keeping pooled auth connections with no idle bound.

## Extra review checks for this repository

- [ ] HTTP auth changes keep explicit-verdict semantics; the 18-case real-HTTP regression and the 27-case ARM admission matrix pass.
- [ ] Every new Kafka consumer group sets its `auto.offset.reset` explicitly; publish-message consumers use `earliest`.
- [ ] HTTP auth client pool changes keep an idle bound below the gate's server idle timeout.
- [ ] Shared-subscription changes state the partition count and that it is final.
- [ ] Base-image changes keep a digest pin and both architectures; no new HIGH/CRITICAL waiver.
- [ ] The PR names which branch it targets (`main`, `release-2.4.1` or `rig/2.4.1-qualification`) and whether the qualified behavior above is present there.
