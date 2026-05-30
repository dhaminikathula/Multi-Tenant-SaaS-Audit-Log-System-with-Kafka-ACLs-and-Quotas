# Multi-Tenant SaaS Audit Log System with Kafka ACLs and Quotas

> Secure multi-tenant audit logging with Kafka authorization, quotas, and MinIO archival.

## Overview

This repository delivers a polished proof-of-concept for a multi-tenant audit log platform built on Apache Kafka.
It demonstrates tenant isolation and security using:

- SASL/SCRAM authentication for tenant clients
- Kafka ACL-based read/write/describe controls per topic
- Client quotas to enforce fair usage and prevent noisy-neighbor behavior
- A REST gateway for tenant audit ingestion
- Background archival of aged audit records to MinIO object storage

## Key Features

- Tenant-specific audit topics: `audit.<tenant>.events`
- Strong tenant authentication with `SCRAM-SHA-256` and `SCRAM-SHA-512`
- Kafka ACL enforcement for topic access control
- Client quotas to throttle excessive producer traffic
- MinIO archival pipeline for cold audit storage
- Validation scripts for ACL and quota enforcement

## Architecture

The stack includes:

- `zookeeper` — cluster coordination for Kafka
- `kafka` — single-broker SASL-enabled Kafka with ACL authorizer
- `minio` — S3-compatible object storage for archived audit events
- `app` — Node.js gateway service for event ingestion and archival

The gateway accepts tenant events over HTTP, writes them to tenant-dedicated Kafka topics, and runs a background archiver that uploads older records to MinIO.

## Quick Start

### 1. Start the platform

```bash
docker compose up -d zookeeper kafka minio app
```

### 2. Provision tenants, topics, ACLs, and quotas

Linux/macOS:

```bash
./provision.sh
```

Windows PowerShell:

```powershell
./provision.ps1
```

### 3. Confirm service health

```bash
docker compose ps
curl http://localhost:8080/health
```

## Tenant Usage

Send an audit event for a tenant:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-acme" \
  -d '{"actor_id":"user-123","action":"login","timestamp":"2026-05-28T12:00:00Z","details":{"ip":"10.0.0.1"}}'
```

Successful requests are accepted with `202 Accepted` and routed into the matching `audit.<tenant>.events` topic.

## Validation

### ACL enforcement

Verify tenant isolation with the ACL test script:

```bash
./test_acl_violation.sh
```

Expected behavior:

- tenant clients can only write to their own topic
- cross-tenant writes are rejected with `TopicAuthorizationException`

### Quota enforcement

Validate client quota throttling:

```bash
./test_quota_violation.sh
```

This script generates sustained producer traffic and confirms broker throttling for tenants that exceed configured quotas.

## Configuration

Use `.env.example` as the basis for environment configuration.

Important values:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_INTERNAL_SERVER`
- `MINIO_ENDPOINT`, `MINIO_PORT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- `SYSTEM_USER`, `SYSTEM_PASSWORD`
- `ADMIN_USER`, `ADMIN_PASSWORD`
- `ARCHIVE_INTERVAL_MS`

## Project Files

- `docker-compose.yml` — orchestrates Kafka, ZooKeeper, MinIO, and the gateway
- `provision.sh` — Linux provisioning script for topics, users, ACLs, and quotas
- `provision.ps1` — Windows provisioning script for tenants and ACLs
- `app/server.js` — HTTP gateway and archival worker
- `app/test_acl_violation.js` — ACL enforcement test
- `app/test_quota_violation.js` — quota throttling validation
- `SECURITY.md` — security model and production hardening guidance

## Notes

- This demo uses `SASL_PLAINTEXT` in Docker Compose for simplicity.
- For production deployments, migrate to `SASL_SSL` and secure all credentials.
- If Kafka broker metadata becomes inconsistent, clear the Kafka volume and restart the broker:

```bash
docker compose down -v
docker compose up -d zookeeper kafka
```

## Security

This repository is designed to illustrate multi-tenant access control and auditing.
Refer to `SECURITY.md` for:

- authentication and authorization strategy
- tenant isolation risks
- credential rotation guidance
- production hardening recommendations
