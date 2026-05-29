#!/usr/bin/env bash
set -u

# Run the ACL violation test script inside the containerized app service
docker-compose exec -T app node test_acl_violation.js
