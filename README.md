# Multi-Tenant SaaS Audit Log System with Kafka ACLs and Quotas

A complete proof-of-concept stack for a secure multi-tenant audit logging platform using Apache Kafka, SASL/SCRAM authentication, ACL-based isolation, client quotas, and MinIO archival.

## Overview

This repository demonstrates how to enforce tenant separation at the Kafka broker layer by:

- assigning each tenant a unique SASL/SCRAM credential
- restricting topic access with Kafka ACLs
- enforcing per-tenant producer/consumer quotas
- writing tenant audit events through a gateway service
- archiving older audit messages to MinIO

## Architecture

- `docker-compose.yml` launches:
  - `zookeeper` for cluster coordination
  - `kafka` broker with `SASL_PLAINTEXT` and `PLAINTEXT` listeners
  - `minio` for object storage
  - `app` gateway service for event ingestion and archival
- `app/server.js` exposes a REST `POST /events` endpoint and a background archiver
- `provision.sh` / `provision.ps1` create tenant topics, SCRAM users, ACLs, and quotas
- `app/test_acl_violation.js` and `app/test_quota_violation.js` validate enforcement
- `SECURITY.md` documents threat model and production hardening guidance

## Prerequisites

- Docker Engine
- Docker Compose v2+
- Bash or WSL for Linux/macOS workflows
- PowerShell for Windows provisioning

## Setup

1. Start the stack:

```bash
docker compose up -d zookeeper kafka minio app
```

2. Wait until the services are healthy.

3. Provision tenants, ACLs, and quotas.

### Linux/macOS

```bash
./provision.sh
```

### Windows PowerShell

```powershell
./provision.ps1
```

## Environment configuration

Use `.env.example` as the template for environment variables.

Key variables:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_INTERNAL_SERVER`
- `MINIO_ENDPOINT`, `MINIO_PORT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- `SYSTEM_USER`, `SYSTEM_PASSWORD`
- `ADMIN_USER`, `ADMIN_PASSWORD`
- `ARCHIVE_INTERVAL_MS`

## Verification

### Service health

- Kafka and ZooKeeper should be `Up` via `docker compose ps`
- App health endpoint:

```bash
curl http://localhost:8080/health
```

### Send a tenant event

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-acme" \
  -d '{"actor_id":"user-123","action":"login","timestamp":"2026-05-28T12:00:00Z","details":{"ip":"10.0.0.1"}}'
```

### ACL verification

Run the ACL violation test to confirm tenant isolation:

```bash
./test_acl_violation.sh
```

A successful test returns a non-zero exit code and prints `TopicAuthorizationException` to stderr.

### Quota verification

Run the quota violation script to generate sustained traffic and verify throttling:

```bash
./test_quota_violation.sh
```

## Project files

- `docker-compose.yml` — orchestration for Kafka, ZooKeeper, MinIO, and gateway
- `provision.sh` — Linux provisioning script
- `provision.ps1` — Windows provisioning script
- `app/server.js` — gateway + archival worker
- `app/test_acl_violation.js` — ACL enforcement test
- `app/test_quota_violation.js` — quota throttling test
- `.env.example` — environment configuration template
- `SECURITY.md` — security and hardening guidance

## Notes

- This demo uses `SASL_PLAINTEXT` within Docker Compose. For production, migrate to `SASL_SSL`.
- If Kafka metadata becomes stale, remove the broker data volume and restart:

```bash
docker compose down -v
docker compose up -d zookeeper kafka
```

## Security

See `SECURITY.md` for details on tenant isolation, credential rotation, breach impact, and enterprise hardening recommendations.
Commit marker 1 - 2026-05-29T16:04:38.4975862+05:30
Commit marker 2 - 2026-05-29T16:04:38.7599111+05:30
Commit marker 3 - 2026-05-29T16:04:38.9196151+05:30
Commit marker 4 - 2026-05-29T16:04:39.1028193+05:30
Commit marker 5 - 2026-05-29T16:04:39.3928142+05:30
Commit marker 6 - 2026-05-29T16:04:39.6443741+05:30
Commit marker 7 - 2026-05-29T16:04:39.8292684+05:30
Commit marker 8 - 2026-05-29T16:04:40.0057555+05:30
Commit marker 9 - 2026-05-29T16:04:40.1916577+05:30
Commit marker 10 - 2026-05-29T16:04:40.4541917+05:30
Commit marker 11 - 2026-05-29T16:04:40.7182082+05:30
Commit marker 12 - 2026-05-29T16:04:40.8875462+05:30
Commit marker 13 - 2026-05-29T16:04:41.0762982+05:30
Commit marker 14 - 2026-05-29T16:04:41.2699525+05:30
Commit marker 15 - 2026-05-29T16:04:41.5838723+05:30
