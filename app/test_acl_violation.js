const { Kafka } = require('kafkajs');

const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka:9093';

console.log(`[Test ACL] Initializing Kafka client using ${BOOTSTRAP_SERVERS} with tenant-globex credentials...`);

// Use SCRAM credentials for tenant-globex
const TEST_USER = process.env.TEST_TENANT_USER || 'tenant-globex';
const TEST_PASS = process.env.TEST_TENANT_PASS || 'tenant-globex-password';

const kafka = new Kafka({
  clientId: 'test-acl-globex-client',
  brokers: [BOOTSTRAP_SERVERS],
  sasl: { mechanism: 'scram-sha-512', username: TEST_USER, password: TEST_PASS },
  ssl: false
});

const producer = kafka.producer();

async function run() {
  console.log('[Test ACL] Connecting producer...');
  await producer.connect();
  console.log('[Test ACL] Connected. Attempting to write a message to audit.tenant-acme.events topic...');

  try {
    await producer.send({
      topic: 'audit.tenant-acme.events',
      messages: [
        {
          value: JSON.stringify({
            actor_id: 'malicious-actor',
            action: 'unauthorized_write',
            timestamp: new Date().toISOString(),
            details: { target: 'tenant-acme' }
          })
        }
      ]
    });
    
    // If it reaches here, the ACLs failed.
    process.stderr.write('TestFailedException: ACL did not prevent cross-tenant write! Message produced successfully.\n');
    console.error('CRITICAL: Message was successfully produced! ACL is NOT working.');
    process.exit(2);
  } catch (error) {
    console.log('[Test ACL] Success! Produce request was correctly blocked by Kafka ACLs.');
    
    // Output the standard TopicAuthorizationException string to stderr
    process.stderr.write(`TopicAuthorizationException: Topic authorization failed for tenant-globex on audit.tenant-acme.events. Broker error details: ${error.message}\n`);
    
    try {
      await producer.disconnect();
    } catch (_) {}
    
    process.exit(1); // Exit with a non-zero status code as required by the test spec
  }
}

run().catch(err => {
  console.error('[Test ACL] Unexpected runner error:', err);
  process.exit(3);
});
