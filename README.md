# dcre-cpx

> Part of the DCRE fleet. For the fleet map, the rulings and the diagrams that specify every stage, start at the [DCRE design register](https://github.com/sean-huni/dcre-design-register); the complete list of live repositories is its [Repositories](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories) table.

PBSR response reader: an ephemeral Spring Batch job that ingests Fintegrate PBSR reply files into `pbsr_resp`, one verdict row per transaction, replay-safe on `UNIQUE (response_file, e2e)`.

## What it does

| | |
|---|---|
| Stage | `CPX` |
| Family / leg | Collections (DC), RES |
| Trigger | arrival-launched: a reply file on the `fint-resp` route whose name carries `_PBSR`, for a client whose flow is collections |
| Upstream | none in the DAG (response DAGs have no edges; AGT token-picks exactly one of `CIX`, `CSX`, `CPX` per reply). The replied-to batch was emitted by `CRW` |
| Downstream | none in the DAG. `CRG` (clock-launched) reads `pbsr_resp` through its `ext_tx_status` view |
| Diagram sheet | `dcre-collections-res` |

DAG position per AGT `RouteDags.FINT_RESP_COL` and the `_PBSR` token pick in `DagEngine` on origin/dev (checked 2026-09-28); a pair with no DAG shape fails closed rather than falling back to another family's readers. CPX parses the reply, one `<OrgnlMsgId>` plus repeated `<Tx>` blocks of `<OrgnlEndToEndId>` + `<TxSts>` + optional `<Rsn>` ([SYNTHETIC-CONTRACT R-35] shape), and upserts one `pbsr_resp` row per Tx block. PBSR carries the final per-transaction statuses and ranks highest in CRG's `ext_tx_status` consolidation (PBSR 4 > SBSR 3 > ISR 2 > CTV 1, R-17).

## Architecture and principles

- **SOLID, 3-tier, layer-first packages**: `ReaderTasklet` is a thin entry adapter (no SQL, no parsing); parsing and sliced upserts live in `service/ReaderService`; persistence only via `data/repo/PbsrRespRepo`; `PbsrRespEntity` extends the platform `BaseEntity` (version, created_at, updated_at). One responsibility per unit.
- **Batch correlation (SCRUM-55)**: the file's `OrgnlMsgId` resolves once to the CRW emission (`crw_emission.outbound_msg_id`) and its frozen member set (`crw_emission_member`). A verdict whose e2e is not a member is skipped with a `FOREIGN_E2E` WARN (fail closed); an unknown `OrgnlMsgId` ingests fail-open with `emission_id` NULL and an `UNKNOWN_OUTBOUND_MSG` WARN. A partial PBSR persists only the transactions it names.
- **12FactorApp Alignment (https://12factor.net/)**: config strictly from the environment over committed working dev defaults in `application.yml` (clean clone runs with NO `.env`); stateless one-shot process whose JVM exit code is the job verdict (`ExitCodeMain` from platform-batch, R-34); CockroachDB and the exchange directory are attached backing resources.
- **Idempotent restart semantics**: the upsert targets the business identity, `INSERT ... ON CONFLICT (response_file, e2e) DO UPDATE` (CRDB `UPSERT` arbitrates on the PK only, so the business key needs `ON CONFLICT`). Large replies commit in bounded slices (SCRUM-42: one giant serializable transaction is unrefreshable at 300k rows, RETRY_SERIALIZABLE), each slice in its own `REQUIRES_NEW` transaction behind a bounded 40001 retry (`CrdbRetry`, 5 attempts, exponential backoff); committed slices stand when a later slice fails, and a restart no-ops over them and resumes the rest. The step itself carries the shared `CrdbRetryExceptionHandler("CPX")` for commit-time aborts. An `@Order(-10)` runner calls `StaleExecutionSweeper.abandonStale(ds, "CPX_BATCH_", 60)` before launch so a relaunch after a pod kill never throws JobExecutionAlreadyRunning (A-39a). Kill-resume is chaos-validated fleet-wide (2026-07-15: SIGKILL at every stage, same-identity relaunch, zero duplicates).
- **Outcome seam (R-33)**: on COMPLETED, platform-batch's `OutcomeSeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>`. A non-COMPLETED run writes nothing: the exit code and the Kubernetes Failed condition are the witnesses; AGT treats absence as never-success (R-33).
- **Isolated batch metadata**: Liquibase-owned copy of the Batch 6 DDL under prefix `CPX_BATCH_` (`dcre.batch.table-prefix`; Boot 4.1 no longer binds `spring.batch.jdbc.*`), with per-service Liquibase history tables `cpx_databasechangelog` / `cpx_databasechangeloglock` on the shared DB.

Job parameters (R-16): `arrival.id` identifying; `input.file` and `original.name` non-identifying; `original.name` becomes the `response_file` identity column.

### Data

| Datasource | Database (dev default) | Env vars | Access |
|---|---|---|---|
| primary | `dcre_col` | `DCRE_DB_URL`, `DCRE_DB_USER`, `DCRE_DB_PASSWORD` | read/write |
| heartbeat (platform-batch) | `agt_ops` | `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`, `DCRE_AGTOPS_DB_PASSWORD` | `HeartbeatWriter` liveness stamp on `launch_intent` |

- Writes: `pbsr_resp` (Liquibase `2026/08/002-pbsr-resp.xml`, the v1 baseline): `response_file` VARCHAR(512), `orgnl_msg_id`, `emission_id` (nullable, no FK), `e2e`, `status`, `reason` (nullable) plus the BaseEntity columns; `UNIQUE (response_file, e2e)` and index `ix_pbsr_emission`. Plus the `CPX_BATCH_*` tables (`001-batch-metadata.xml`).
- Reads: `crw_emission`, `crw_emission_member` (CRW-owned).

## Prerequisites

- Java 25 (`.sdkmanrc`: `java=25-tem`; `build.gradle` sets source/target compatibility 25)
- Gradle 9.5.1 via the wrapper
- Docker (Testcontainers in the test suite; image build)
- Platform libs in Maven Local: `za.co.fnb.dcre:platform-persistence:0.1.0` and `za.co.fnb.dcre:platform-batch:0.1.0` (no remote repository)

## Quickstart

```bash
# 1. Publish the platform libs to Maven Local (fleet checkout: collections/cpx beside platform/*;
#    batch needs model -> files first, persistence is standalone)
for repo in platform-model platform-files platform-batch platform-persistence; do
  (cd ../../platform/$repo && ./gradlew publishToMavenLocal)
done

# 2. Build and run the full test suite (needs Docker)
./gradlew test

# 3. One-shot local run against a local CockroachDB (localhost:26257 by default)
./gradlew bootJar
java -jar build/libs/cpx-2.0.jar \
  'arrival.id=<uuid>,java.lang.String,true' \
  'input.file=/path/to/reply.xml,java.lang.String,false' \
  'original.name=20260712_FNB_PBSR_reply.xml,java.lang.String,false'
```

A clean clone runs with no `.env` at all: the working dev defaults are committed in `application.yml`.

## Configuration

Precedence: yml default < real environment variable.

| Env var | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | Shared collections DB (CockroachDB via the PostgreSQL driver) |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | heartbeat datasource |
| `DCRE_AGTOPS_DB_USER` / `DCRE_AGTOPS_DB_PASSWORD` | `root` / (empty) | heartbeat credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam (AGT sets `/exchange` in-cluster) |
| `DCRE_CPX_INGEST_SLICE_SIZE` | `10000` | Tx rows committed per ingest slice (SCRUM-42) |
| `JOB_NAME` | `local-cpx-<executionId>` | Names the outcome seam file; set by AGT on the Kubernetes Job |

This is the documented set, not a closed total: Spring relaxed binding lets any Spring or `dcre.*` property be overridden by its derived environment variable name.

## Testing

```bash
./gradlew test
```

Docker required; all DB tests run on Testcontainers `cockroachdb/cockroach:v26.2.3`:

- `CpxJobTest`: the full job on real CRDB ingests a 4-Tx reply (ACSC and RJCT with reason AC04), asserts per-row status/reason/orgnl_msg_id, and proves replaying the same file stays at 4 rows.
- `ReaderServiceSliceTest`: sliced-ingest proofs; committed slices survive a failing slice, a transient 40001 abort retries in a fresh transaction, and a re-run no-ops over committed slices with row identity preserved.
- `BatchCorrelationIT`: emission resolution and foreign-e2e exclusion; an unknown `OrgnlMsgId` ingests fail-open with a NULL emission; a partial PBSR persists immediately and mints no synthetic siblings.
- `ResponseFileWidthIT`: a 200-character `response_file` round-trips; the replay-guard unique constraint holds at baseline width.
- `CpxJobConfigRetryTest`: the shared CRDB 40001 retry handler on the real step wiring covers commit-time aborts.
- Cucumber BDD (`@cpx`, `src/test/resources/features/pbsr-reply-reader.feature`): positive and negative reply-ingestion scenarios, including malformed and empty replies.

## Local cluster deployment

```bash
# cluster (once): creates kind cluster dcre-dev with CRDB and the exchange hostPath
(cd ../../../../../../infra/dcre-infra && scripts/kind-up.sh)

# build and load the stage image (tag = fleet release version)
VERSION=<fleet release tag>
./gradlew bootJar
docker build -t dcre-cpx:$VERSION .
kind load docker-image --name dcre-dev dcre-cpx:$VERSION

# point AGT at it: switch-version.sh does NOT set AGT_CPX_IMAGE (see below)
kubectl set env -n dcre deploy/dcre-agt AGT_CPX_IMAGE=dcre-cpx:$VERSION
```

The image is `eclipse-temurin:25-jre-alpine` carrying `build/libs/cpx-2.0.jar`. AGT resolves it from `AGT_CPX_IMAGE` (empty by default, which leaves the stage launch-disabled). dcre-infra `scripts/switch-version.sh` does not set it: its stage list still names the retired `PXR` (checked 2026-09-28). CPX is not deployed as a server: per reply AGT mints a one-shot Kubernetes Job (`backoffLimit: 0`, `restartPolicy: Never`) in the collections flow namespace (AGT `AGT_NAMESPACE_COL`, default `dcre-col`), passing `arrival.id` / `input.file` / `original.name` as program args and `JOB_NAME`, `DCRE_DB_URL` (AGT `service-db-url`, `dcre_col`), `DCRE_EXCHANGE_ROOT=/exchange`, `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER` in the env, with the shared exchange PVC mounted at `/exchange` (AGT `JobLauncher` on origin/dev, checked 2026-09-28).

Release tags are digits-only 3-component SemVer; this repo carries 1.0.0 through 2.2.1, and tagging is not uniform across the fleet (the payments stages carry none; `git ls-remote --tags`, checked 2026-09-28).

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
