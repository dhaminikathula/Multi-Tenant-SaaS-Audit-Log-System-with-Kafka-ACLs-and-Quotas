#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo "Bootstrapping Multi-Tenant Audit Log System"
echo "========================================="

# 1. Wait for Kafka to be ready
echo "Waiting for Kafka broker to start and become healthy..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  echo "Kafka is not fully active yet, sleeping 3s..."
  sleep 3
done
echo "Kafka broker is ready!"

# 2. Define tenants
TENANTS=("tenant-acme" "tenant-globex" "tenant-initech")
TENANT_PASSWORDS=("tenant-acme-password" "tenant-globex-password" "tenant-initech-password")

# System and admin principals
SYSTEM_USER="gateway-system"
SYSTEM_PASSWORD="gateway-system-password"
ADMIN_USER="admin"
ADMIN_PASSWORD="admin-password"

# 3. Provision each tenant
for tenant in "${TENANTS[@]}"; do
  echo "-----------------------------------------"
  echo "Provisioning tenant: ${tenant}"
  echo "-----------------------------------------"

  # Create topic
  echo "Creating topic audit.${tenant}.events..."
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic "audit.${tenant}.events" \
    --partitions 1 --replication-factor 1 \
    --if-not-exists
done

echo "-----------------------------------------"
echo "Creating SCRAM users, ACLs and quotas for tenants"
echo "-----------------------------------------"

for idx in "${!TENANTS[@]}"; do
  tenant=${TENANTS[$idx]}
  password=${TENANT_PASSWORDS[$idx]}

  echo "Creating SCRAM credentials for ${tenant}..."
  docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
    --alter --add-config "SCRAM-SHA-512=[password=${password}]" \
    --entity-type users --entity-name ${tenant} || true

  echo "Setting ACLs for ${tenant} on topic audit.${tenant}.events..."
  # Allow produce (Write) and consume (Read) only on the tenant's topic
  docker exec kafka kafka-acls --authorizer-properties zookeeper.connect=zookeeper:2181 \
    --add --allow-principal User:${tenant} --operation Write --topic audit.${tenant}.events || true
  docker exec kafka kafka-acls --authorizer-properties zookeeper.connect=zookeeper:2181 \
    --add --allow-principal User:${tenant} --operation Read --topic audit.${tenant}.events || true

  echo "Applying producer/consumer byte-rate quotas for ${tenant}..."
  docker exec kafka kafka-configs --bootstrap-server localhost:9093 \
    --alter --add-config "producer_byte_rate=1048576,consumer_byte_rate=1048576" \
    --entity-type users --entity-name ${tenant} || true
done

echo "-----------------------------------------"
echo "Create admin and system SCRAM principals"
echo "-----------------------------------------"

docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --alter --add-config "SCRAM-SHA-512=[password=${SYSTEM_PASSWORD}]" \
  --entity-type users --entity-name ${SYSTEM_USER} || true

docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --alter --add-config "SCRAM-SHA-512=[password=${ADMIN_PASSWORD}]" \
  --entity-type users --entity-name ${ADMIN_USER} || true

echo "Grant admin (archiver) READ access to all tenant topics"
for tenant in "${TENANTS[@]}"; do
  docker exec kafka kafka-acls --authorizer-properties zookeeper.connect=zookeeper:2181 \
    --add --allow-principal User:${ADMIN_USER} --operation Read --topic audit.${tenant}.events || true
done

echo "Grant system producer access to audit.violations"
docker exec kafka kafka-acls --authorizer-properties zookeeper.connect=zookeeper:2181 \
  --add --allow-principal User:${SYSTEM_USER} --operation Write --topic audit.violations || true

echo "-----------------------------------------"
echo "Provisioning System Violations Topic"
echo "-----------------------------------------"

# Create audit.violations topic
echo "Creating topic audit.violations..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic "audit.violations" \
  --partitions 1 --replication-factor 1 \
  --if-not-exists

echo "-----------------------------------------"
echo "Listing all topics..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo "========================================="
echo "Provisioning completed successfully!"
echo "========================================="
