const { Kafka } = require('kafkajs');

const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka:9093';

console.log(`[Test Quota] Initializing Kafka client using ${BOOTSTRAP_SERVERS} with tenant-initech credentials...`);

// Use SCRAM credentials for tenant-initech
const TEST_USER = process.env.TEST_TENANT_USER || 'tenant-initech';
const TEST_PASS = process.env.TEST_TENANT_PASS || 'tenant-initech-password';

const kafka = new Kafka({
  clientId: 'test-quota-initech-client',
  brokers: [BOOTSTRAP_SERVERS],
  sasl: { mechanism: 'scram-sha-512', username: TEST_USER, password: TEST_PASS },
  ssl: false
});

const producer = kafka.producer();

async function run() {
  console.log('[Test Quota] Connecting producer...');
  await producer.connect();
  console.log('[Test Quota] Connected. Starting max-throughput load generation (1MB/s quota target) for 17 seconds...');

  const startTime = Date.now();
  const TEST_DURATION_MS = 17000; // Run for 17 seconds (at least 15s)
  
  // Create a message batch that stays under Kafka broker max message size
  const payloadSize = 40 * 1024; // 40 KB payload
  const heavyString = 'Q'.repeat(payloadSize);
  const batch = Array(5).fill(null).map(() => ({
    value: JSON.stringify({
      actor_id: 'quota-tester',
      action: 'generate_load',
      timestamp: new Date().toISOString(),
      details: { payload: heavyString }
    })
  }));
  const batchSizeBytes = batch.reduce((sum, msg) => sum + Buffer.byteLength(msg.value, 'utf8'), 0);

  let bytesProduced = 0;
  let batchCount = 0;

  while (Date.now() - startTime < TEST_DURATION_MS) {
    try {
      // Send the batch
      await producer.send({
        topic: 'audit.tenant-initech.events',
        messages: batch,
        acks: 1 // ACKs = 1 for maximal speed
      });

      // Track bytes based on actual payload size
      bytesProduced += batchSizeBytes;
      batchCount++;

      if (batchCount % 2 === 0) {
        const elapsedSec = (Date.now() - startTime) / 1000;
        const currentRateMB = (bytesProduced / (1024 * 1024)) / elapsedSec;
        console.log(`[Test Quota] Sent ${batchCount} batches (~${(bytesProduced / (1024 * 1024)).toFixed(1)} MB). Average rate: ${currentRateMB.toFixed(2)} MB/s`);
      }
    } catch (err) {
      console.warn(`[Test Quota] Produce delayed/throttled or failed: ${err.message}`);
    }
  }

  const finalDurationSec = (Date.now() - startTime) / 1000;
  const finalRateMB = (bytesProduced / (1024 * 1024)) / finalDurationSec;
  console.log(`[Test Quota] Load test complete.`);
  console.log(`[Test Quota] Total produced: ~${(bytesProduced / (1024 * 1024)).toFixed(1)} MB`);
  console.log(`[Test Quota] Total duration: ${finalDurationSec.toFixed(2)}s`);
  console.log(`[Test Quota] Final throughput: ${finalRateMB.toFixed(2)} MB/s`);

  const QUOTA_THRESHOLD_MBPS = 2.0;
  if (finalRateMB > QUOTA_THRESHOLD_MBPS) {
    console.error(`[Test Quota] Quota enforcement failed: final throughput exceeded ${QUOTA_THRESHOLD_MBPS.toFixed(1)} MB/s`);
    process.exit(2);
  }

  console.log('[Test Quota] Quota enforcement appears effective.');

  try {
    await producer.disconnect();
  } catch (_) {}
  process.exit(0);
}

run().catch(err => {
  console.error('[Test Quota] Unexpected runner error:', err);
  process.exit(1);
});
