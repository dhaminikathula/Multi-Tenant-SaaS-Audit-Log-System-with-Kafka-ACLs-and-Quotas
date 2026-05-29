Write-Host "========================================="
Write-Host "Bootstrapping Multi-Tenant Audit Log System"
Write-Host "========================================="

# 1. Wait for Kafka to be ready
Write-Host "Waiting for Kafka broker to start and become healthy..."
while ($true) {
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >$null 2>&1
    if ($LASTEXITCODE -eq 0) {
        break
    }
    Write-Host "Kafka is not fully active yet, sleeping 3s..."
    Start-Sleep -Seconds 3
}
Write-Host "Kafka broker is ready!"

# 2. Define tenants
$TENANTS = @("tenant-acme", "tenant-globex", "tenant-initech")

# 3. Provision each tenant
foreach ($tenant in $TENANTS) {
    Write-Host "-----------------------------------------"
    Write-Host "Provisioning tenant: $tenant"
    Write-Host "-----------------------------------------"

    # Create topic
    Write-Host "Creating topic audit.$tenant.events..."
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "audit.$tenant.events" --partitions 1 --replication-factor 1 --if-not-exists

    # Create SASL/SCRAM user
    Write-Host "Creating user principal $tenant..."
    docker exec kafka kafka-configs --bootstrap-server localhost:9092 --entity-type users --entity-name "$tenant" --alter --add-config "SCRAM-SHA-512=[password=$tenant-password]"

    # Apply Producer ACLs (Write, Describe)
    Write-Host "Applying Producer ACLs for $tenant..."
    docker exec kafka kafka-acls --bootstrap-server localhost:9092 --add --allow-principal "User:$tenant" --producer --topic "audit.$tenant.events"

    # Apply Consumer ACLs (Read, Describe) for topic and all consumer groups
    Write-Host "Applying Consumer ACLs for $tenant..."
    docker exec kafka kafka-acls --bootstrap-server localhost:9092 --add --allow-principal "User:$tenant" --consumer --topic "audit.$tenant.events" --group "*"

    # Apply Client Quotas (1 MB/s read/write)
    Write-Host "Applying client quotas (1 MB/s) for $tenant..."
    docker exec kafka kafka-configs --bootstrap-server localhost:9092 --entity-type users --entity-name "$tenant" --alter --add-config "producer_byte_rate=1048576,consumer_byte_rate=1048576"
}

Write-Host "-----------------------------------------"
Write-Host "Provisioning System Violations Topic & Users"
Write-Host "-----------------------------------------"

# Create audit.violations topic
Write-Host "Creating topic audit.violations..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "audit.violations" --partitions 1 --replication-factor 1 --if-not-exists

# Create gateway-system user
Write-Host "Creating user principal gateway-system..."
docker exec kafka kafka-configs --bootstrap-server localhost:9092 --entity-type users --entity-name "gateway-system" --alter --add-config "SCRAM-SHA-512=[password=gateway-system-password]"

# Create admin user
Write-Host "Creating user principal admin..."
docker exec kafka kafka-configs --bootstrap-server localhost:9092 --entity-type users --entity-name "admin" --alter --add-config "SCRAM-SHA-512=[password=admin-password]"

# Apply Producer ACLs for gateway-system on violations topic
Write-Host "Applying Producer ACLs for gateway-system..."
docker exec kafka kafka-acls --bootstrap-server localhost:9092 --add --allow-principal "User:gateway-system" --producer --topic "audit.violations"

# Grant admin read access to tenant topics
Write-Host "Applying admin read access to tenant topics..."
foreach ($tenant in $TENANTS) {
    docker exec kafka kafka-acls --bootstrap-server localhost:9092 --add --allow-principal "User:admin" --operation Read --topic "audit.$tenant.events"
}

Write-Host "========================================="
Write-Host "Provisioning completed successfully!"
Write-Host "========================================="
