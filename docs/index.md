# CCE Data Pipeline — Documentation Index

## Overview

The CCE Data Pipeline lands committed PostgreSQL data into ClickHouse via Change Data Capture (**Debezium + Kafka + ClickHouse**) — no custom stream processing. The `cce-insights-service` + `cce-insights-ui` apps (separate repos) consume ClickHouse to serve dashboards.

**Core principle:** Analytics should be purely on committed data in the database.

---

## Document Map

| Document | Purpose | Key Audience |
|----------|---------|--------------|
| [Architecture Overview](architecture-overview.md) | System context, principles, technology decisions, component roles, capacity planning, security, failure modes | Architects, tech leads, DevOps |
| [Data Flow & Schema Design](data-flow.md) | CDC pipeline config, ClickHouse DDL, MATERIALIZED columns, MV catalog, Entity × Behavior matrix, query patterns | Data engineers, backend developers |
| [Deployment Guide](deployment-guide.md) | Prerequisites, Docker/server setup, connector registration, schema deployment, monitoring, validation, rollback, operational procedures, troubleshooting | DevOps, SRE, platform engineers |

---

## Quick Reference

### Data Path

```
PostgreSQL (WAL) → Debezium → Kafka → ClickHouse (Kafka engine + MVs) → cce-insights-service / cce-insights-ui
```

### Reading Order

1. **New to the project?** Start with [Architecture Overview](architecture-overview.md)
2. **Working on schema/CDC?** Reference [Data Flow & Schema Design](data-flow.md)
3. **Deploying/operating?** Follow [Deployment Guide](deployment-guide.md)

---

## Repository Layout (file → component)

This repo only deploys **ClickHouse** and a **Kafka Connect (Debezium)** worker; Kafka, PostgreSQL
(`ccedb`), Prometheus/Grafana, and the insights apps belong to the platform stack
([openphc/deploy-scripts](https://github.com/openphc/deploy-scripts)) on the shared `cce-net`
network. So many files here are *applied to* or *consumed by* components that live elsewhere.

Files relate to components in one of four ways:

| Relationship | Meaning |
|---|---|
| **applied once** | Run against a component; the effect persists in that component's state |
| **mounted / read at runtime** | The component loads the file while it runs |
| **pushed in** | An operator sends it into a component's API, which stores it |
| **run by operator/CI** | A person or pipeline executes it to *drive* the components |

| Path | Component | Relationship | Why |
|------|-----------|--------------|-----|
| `cdc/01-configure-replication.sql` | PostgreSQL `ccedb` | applied once (DBA via psql) | Enables logical replication; creates the CDC user, `REPLICA IDENTITY FULL`, and the `cce_analytics_pub` publication that Debezium subscribes to |
| `connectors/debezium-postgres-source.json` | Kafka Connect / Debezium | pushed in (via `register-connectors.sh`) | The connector definition: tables, `pgoutput`, slot/publication, JSON converters, ReselectColumns |
| `schema/01`–`schema/08` `*.sql` | ClickHouse | applied once (in order) | Base tables (14, incl. `facility` + `*_history`) → Kafka-engine queues + consumer MVs → aggregation MVs → indexes → dictionaries → current-state rollups → daily-summary refreshable MVs (08 is documentation-only) |
| `schema/09-historical-backfill.sql` | ClickHouse | run by operator (manual, parameterised) | Rebuilds past `snapshot_date` rows of the schema/07 daily MVs from the `*_history` tables after a full re-snapshot — **never** auto-applied |
| `infra/clickhouse/*.xml` | ClickHouse | mounted (config.d / users.d) | `config.xml` (server tuning, metrics), `named-collections.xml` (`cce_kafka` broker via `from_env`), `users.xml` (`cce_pipeline` grants; `analytics` profile w/ `final=1` available) |
| `infra/prometheus/`, `infra/grafana/` | Prometheus + Grafana (platform) | read at runtime (imported there) | Scrape config, data sources, the pipeline-health dashboard, and alerts — shipped here, added to the platform's instances |
| `docker-compose.yml` | Docker (starts ClickHouse + Kafka Connect) | read by Docker | Images, ports, env, volume mounts, the external `cce-net` network |
| `.env` (from `.env.example`) | Docker Compose **and** the scripts | read at runtime | Single source of truth: `KAFKA_BOOTSTRAP_SERVERS`, `CLICKHOUSE_PASSWORD`, `POSTGRES_*`, `CONNECT_URL` |
| `scripts/*.sh` | drives Kafka Connect / Postgres / ClickHouse | run by operator/CI | Register the connector, health-check, re-snapshot, validate CDC config, validate ClickHouse, data-quality checks |
| `tests/*.sh` | drives the whole path | run by operator/CI | End-to-end smoke test and a source-load test |

> Each file also carries a `-- Usage:` / header comment with its exact invocation — this table is
> the map; the file headers are the detail.

### Where each file sits along the data path

```
ccedb (Postgres) ─► Debezium (Kafka Connect) ─► Kafka ─► ClickHouse ─► Grafana / insights-service
       ▲                     ▲                            ▲                  ▲
 cdc/01-…sql          connectors/*.json            schema/01–06.sql    infra/grafana/*
 (DBA, once)          register-connectors.sh       infra/clickhouse/*  infra/prometheus/*
 validate-cdc-config  check-connector-health       validate-clickhouse (platform imports)
                      (operator)                    data-quality-checks
```

**Intuition:** files on the **left** prepare the *source* (Postgres), the **middle** defines the
*movement* (the Debezium connector + the scripts that push/monitor it), the **right** defines the
*destination and its views* (ClickHouse schema + config + monitoring) — and `.env` is the shared
knob feeding both Docker and the scripts.
