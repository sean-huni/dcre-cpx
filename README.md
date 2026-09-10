# dcre-cpx

PBSR response reader: an ephemeral Spring Batch job that ingests Fintegrate PBSR reply files into `pbsr_resp`, one verdict row per transaction, replay-safe on `UNIQUE (response_file, e2e)`.

## What it does

CPX is the PBSR leg of the DCRE Collections response flow: `CIX | CSX | CPX -> ext_tx_status -> CRG`. When Fintegrate drops a reply file whose name carries the `_PBSR` token into the exchange, AGT's fint-resp route launches CPX as a short-lived Kubernetes Job (unknown reply tokens quarantine fail-closed). CPX parses the reply, one `<OrgnlMsgId>` plus repeated `<Tx>` blocks of `<OrgnlEndToEndId>` + `<TxSts>` + optional `<Rsn>` ([SYNTHETIC-CONTRACT R-35] shape), and upserts one `pbsr_resp` row per Tx block. PBSR carries the final per-transaction statuses and ranks highest in the `ext_tx_status` consolidation (precedence PBSR > SBSR > ISR > CTV, R-17) that CRG reads.

## Architecture and principles

- **SOLID, 3-tier, layer-first packages**: `ReaderTasklet` is a thin entry adapter (no SQL, no parsing); parsing and sliced upserts live in `service/ReaderService`; persistence only via `data/repo/PbsrRespRepo`; `PbsrRespEntity` extends the platform `BaseEntity` (version, created_at, updated_at). One responsibility per unit, small classes, clear interfaces.
- **12FactorApp Alignment (https://12factor.net/)**: config strictly from the environment over committed working dev defaults in `application.yml` (clean clone runs with NO `.env`); stateless one-shot process whose JVM exit code is the job verdict (`ExitCodeMain` from platform-batch, R-34); CockroachDB and the exchange directory are attached backing resources.
- **Idempotent restart semantics**: the upsert targets the business identity, `INSERT ... ON CONFLICT (response_file, e2e) DO UPDATE` (CRDB `UPSERT` arbitrates on the PK only, so the business key needs `ON CONFLICT`). Large replies commit in bounded slices (SCRUM-42: one giant serializable transaction is unrefreshable at 300k rows, RETRY_SERIALIZABLE), each slice in its own `REQUIRES_NEW` transaction behind a bounded 40001 retry (`CrdbRetry`, 5 attempts, exponential backoff); committed slices stand when a later slice fails, and a restart no-ops over them and resumes the rest. The step itself carries the shared `CrdbRetryExceptionHandler("CPX")` for commit-time aborts. `StaleExecutionSweeper.abandonStale(ds, "CPX_BATCH_", 60)` runs before launch so a relaunch after a pod kill never throws JobExecutionAlreadyRunning (A-39a). Kill-resume is chaos-validated fleet-wide (2026-07-15: SIGKILL at every stage, same-identity relaunch, zero duplicates).
- **Outcome seam (R-35)**: on COMPLETED the job writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>`. A non-COMPLETED run writes nothing: the exit code and the Kubernetes Failed condition are the witnesses; AGT treats absence as never-success (R-33).
- **Isolated batch metadata**: Liquibase-owned copy of the Batch 6 DDL under prefix `CPX_BATCH_` (`dcre.batch.table-prefix`; Boot 4.1 no longer binds `spring.batch.jdbc.*`), with per-service Liquibase history tables `cpx_databasechangelog` / `cpx_databasechangeloglock` on the shared DB.

Job parameters (R-16): `arrival.id` identifying; `input.file` and `original.name` non-identifying; `original.name` becomes the `response_file` identity column.

Data: `pbsr_resp` (Liquibase `2026/08/002-pbsr-resp.xml`, the v1 baseline): `response_file` VARCHAR(512), `orgnl_msg_id`, `emission_id` (nullable, SCRUM-55 batch correlation, no FK), `e2e`, `status`, `reason` (nullable) plus the BaseEntity columns; `UNIQUE (response_file, e2e)` and index `ix_pbsr_emission`.

## Prerequisites

- Java 25 (Gradle toolchain, `./gradlew` provided)
- Docker (Testcontainers in the test suite; image build)
- Platform libs in Maven Local: `za.co.fnb.dcre:platform-persistence:0.1.0` and `za.co.fnb.dcre:platform-batch:0.1.0` (no remote repository)

## Quickstart

```bash
# 1. Publish the platform libs to Maven Local (sibling repos; batch needs model -> files first)
for repo in platform-model platform-files platform-batch platform-persistence; do
  (cd ../$repo && ./gradlew publishToMavenLocal)
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
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam (AGT sets `/exchange` in-cluster) |
| `DCRE_CPX_INGEST_SLICE_SIZE` | `10000` | Tx rows committed per ingest slice (SCRUM-42) |
| `JOB_NAME` | `local-<executionId>` | Names the outcome seam file; set by AGT on the Kubernetes Job |

## Testing

```bash
./gradlew test
```

Docker required; all DB tests run on Testcontainers `cockroachdb/cockroach:v26.2.3`:

- `CpxJobTest`: the full job on real CRDB ingests a 4-Tx reply (ACSC and RJCT with reason AC04), asserts per-row status/reason/orgnl_msg_id, and proves replaying the same file stays at 4 rows.
- `ReaderServiceSliceTest`: sliced-ingest proofs; committed slices survive a failing slice, a transient 40001 abort retries in a fresh transaction, and a re-run no-ops over committed slices with row identity preserved.
- `CpxJobConfigRetryTest`: the shared CRDB 40001 retry handler on the real step wiring covers commit-time aborts.
- Cucumber BDD (`@cpx`, `src/test/resources/features/pbsr-reply-reader.feature`): positive and negative reply-ingestion scenarios, including malformed and empty replies.

## Local cluster deployment

```bash
# cluster (once): creates kind cluster dcre-dev with CRDB and the exchange hostPath
(cd ../../../../../infra/dcre-infra && scripts/kind-up.sh)

# build and load the stage image (tag = fleet release version)
./gradlew bootJar
docker build -t dcre-cpx:2.1.1 .
kind load docker-image --name dcre-dev dcre-cpx:2.1.1

# switch the whole fleet to the version (sets AGT_CPX_IMAGE=dcre-cpx:2.1.1 on the AGT deployment)
(cd ../../../../../infra/dcre-infra && scripts/switch-version.sh 2.1.1)
```

The image is `eclipse-temurin:25-jre-alpine` carrying `build/libs/cpx-2.0.jar`. CPX is not deployed as a server: AGT's fint-resp route matches the `_PBSR` filename token and mints a one-shot Kubernetes Job (`backoffLimit: 0`, `restartPolicy: Never`) from `AGT_CPX_IMAGE`, passing `arrival.id` / `input.file` / `original.name` as program args and `JOB_NAME`, `DCRE_DB_URL`, `DCRE_EXCHANGE_ROOT=/exchange` in the env, with the shared exchange PVC mounted at `/exchange`.

Releases are uniform digits-only 3-component SemVer git tags across the fleet; this repo carries `1.0.0` through `2.1.1` (current).

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Sibling response readers: [dcre-ixr](https://github.com/sean-huni/dcre-ixr), [dcre-sxr](https://github.com/sean-huni/dcre-sxr)
- Other stages: [dcre-crr](https://github.com/sean-huni/dcre-crr), [dcre-ctv](https://github.com/sean-huni/dcre-ctv), [dcre-cde](https://github.com/sean-huni/dcre-cde), [dcre-cir](https://github.com/sean-huni/dcre-cir), [dcre-crw](https://github.com/sean-huni/dcre-crw), [dcre-prg](https://github.com/sean-huni/dcre-prg), [dcre-ais](https://github.com/sean-huni/dcre-ais), [dcre-hcs](https://github.com/sean-huni/dcre-hcs)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model), [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files), [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch), [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Environment and tooling: [dcre-infra](https://github.com/sean-huni/dcre-infra), [dcre-fixture-toolkit](https://github.com/sean-huni/dcre-fixture-toolkit), [dcre-design-register](https://github.com/sean-huni/dcre-design-register), [dcre-rpt](https://github.com/sean-huni/dcre-rpt)
