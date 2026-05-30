#!/usr/bin/env bash
set -u

# Run the client quota violation load test inside the containerized app service
docker compose exec -T app node test_quota_violation.js
