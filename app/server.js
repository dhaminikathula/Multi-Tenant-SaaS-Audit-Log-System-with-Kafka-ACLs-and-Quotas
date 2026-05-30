const express = require('express');
const { Kafka } = require('kafkajs');
const Minio = require('minio');

const PORT = parseInt(process.env.PORT || '8080');
const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka:9093';
const INTERNAL_SERVER = process.env.KAFKA_INTERNAL_SERVER || 'kafka:9093';

const MINIO_ENDPOINT = process.env.MINIO_ENDPOINT || 'minio';
const MINIO_PORT = parseInt(process.env.MINIO_PORT || '9000');
const MINIO_ACCESS_KEY = process.env.MINIO_ACCESS_KEY || 'minioadmin';
const MINIO_SECRET_KEY = process.env.MINIO_SECRET_KEY || 'minioadmin';
const MINIO_USE_SSL = process.env.MINIO_USE_SSL === 'true';

const ARCHIVE_INTERVAL_MS = parseInt(process.env.ARCHIVE_INTERVAL_MS || '300000'); // 5 minutes default
const BUCKET_NAME = 'kafka-archive';

const app = express();
app.use(express.json());

// ----------------------------------------------------
// Kafka and MinIO Setup
// ----------------------------------------------------

const tenants = {
  'tenant-acme': 'tenant-acme-password',
  'tenant-globex': 'tenant-globex-password',
  'tenant-initech': 'tenant-initech-password'
};

// System/admin credentials used for violations and archival consumer
const SYSTEM_USER = process.env.SYSTEM_USER || 'gateway-system';
const SYSTEM_PASSWORD = process.env.SYSTEM_PASSWORD || 'gateway-system-password';
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin-password';

const tenantProducers = {};
let systemProducer = null;

// Initialize Plaintext Producers for each Tenant
function initProducers() {
  console.log(`[Gateway] Initializing dynamic tenant producers using Kafka endpoint: ${BOOTSTRAP_SERVERS}`);

  for (const [tenantId, password] of Object.entries(tenants)) {
    const kafka = new Kafka({
      clientId: `gateway-${tenantId}`,
      brokers: [BOOTSTRAP_SERVERS],
      sasl: { mechanism: 'scram-sha-512', username: tenantId, password },
      ssl: false
    });
    tenantProducers[tenantId] = kafka.producer();
  }

  // Initialize Producer for System Gateway (violations topic)
  const systemKafka = new Kafka({
    clientId: 'gateway-system',
    brokers: [BOOTSTRAP_SERVERS],
    sasl: { mechanism: 'scram-sha-512', username: SYSTEM_USER, password: SYSTEM_PASSWORD },
    ssl: false
  });
  systemProducer = systemKafka.producer();
}

// Initialize MinIO client
const minioClient = new Minio.Client({
  endPoint: MINIO_ENDPOINT,
  port: MINIO_PORT,
  useSSL: MINIO_USE_SSL,
  accessKey: MINIO_ACCESS_KEY,
  secretKey: MINIO_SECRET_KEY
});

// ----------------------------------------------------
// HTTP Event Gateway Endpoint
// ----------------------------------------------------

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK' });
});

app.post('/events', async (req, res) => {
  const tenantId = req.headers['x-tenant-id'];
  const sourceIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';

  // 1. Validation check
  if (!tenantId || !tenants[tenantId]) {
    console.warn(`[Gateway] Unauthorized request received. Tenant-ID: '${tenantId}' from IP: ${sourceIp}`);
    
    // Log violation to audit.violations topic
    const violationEvent = {
      attempted_tenant_id: tenantId || null,
      timestamp: new Date().toISOString(),
      source_ip: sourceIp,
      reason: tenantId ? `Invalid tenant identity: '${tenantId}'` : 'Missing X-Tenant-ID header',
      request_details: {
        headers: req.headers,
        body: req.body
      }
    };

    try {
      if (systemProducer) {
        await systemProducer.send({
          topic: 'audit.violations',
          messages: [{ value: JSON.stringify(violationEvent) }]
        });
        console.log(`[Gateway] Violation logged to audit.violations topic.`);
      }
    } catch (err) {
      console.error('[Gateway] Failed to produce violation message:', err);
    }

    return res.status(401).json({ error: 'Unauthorized' });
  }

  // 2. Schema validation
  const { actor_id, action, timestamp, details } = req.body;
  if (!actor_id || !action || !timestamp) {
    return res.status(400).json({ error: 'Bad Request: Missing required fields (actor_id, action, timestamp)' });
  }

  // 3. Produce to the tenant's dedicated Kafka topic
  const topic = `audit.${tenantId}.events`;
  const eventMessage = {
    actor_id,
    action,
    timestamp,
    details: details || {}
  };

  try {
    const producer = tenantProducers[tenantId];
    await producer.send({
      topic: topic,
      messages: [{ value: JSON.stringify(eventMessage) }]
    });

    console.log(`[Gateway] Event accepted and produced successfully to ${topic}. Tenant-ID: ${tenantId}`);
    return res.status(202).json({ status: 'Accepted' });
  } catch (err) {
    console.error(`[Gateway] Error producing message to ${topic} under tenant credentials:`, err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

// ----------------------------------------------------
// S3/MinIO Archival Worker
// ----------------------------------------------------

async function startArchiver() {
  console.log(`[Archiver] Initializing background archival worker...`);

  // Ensure MinIO bucket exists
  try {
    const bucketExists = await minioClient.bucketExists(BUCKET_NAME);
    if (!bucketExists) {
      await minioClient.makeBucket(BUCKET_NAME);
      console.log(`[Archiver] Successfully created MinIO bucket '${BUCKET_NAME}'`);
    } else {
      console.log(`[Archiver] Verified MinIO bucket '${BUCKET_NAME}' exists`);
    }
  } catch (err) {
    console.error('[Archiver] Error checking/creating MinIO bucket:', err);
    process.exit(1);
  }

  // Connect to the INTERNAL listener using admin credentials
  const archiverKafka = new Kafka({
    clientId: 'audit-log-archiver',
    brokers: [INTERNAL_SERVER],
    sasl: { mechanism: 'scram-sha-512', username: ADMIN_USER, password: ADMIN_PASSWORD },
    ssl: false
  });

  const consumer = archiverKafka.consumer({ groupId: 'audit-archiver-group' });
  await consumer.connect();

  const targetTopics = Object.keys(tenants).map(id => `audit.${id}.events`);
  console.log(`[Archiver] Subscribing to topics: ${targetTopics.join(', ')}`);
  
  await consumer.subscribe({ topics: targetTopics, fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      console.log(`[Archiver] Processing batch of ${batch.messages.length} messages from topic: ${batch.topic}`);

      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;

        // Parse message and fetch the event timestamp
        let msgPayload;
        try {
          msgPayload = JSON.parse(message.value.toString());
        } catch (e) {
          msgPayload = {};
        }

        // Parse date from message or fallback to Kafka broker timestamp
        let msgTimeMs = parseInt(message.timestamp);
        if (msgPayload.timestamp) {
          const parsed = Date.parse(msgPayload.timestamp);
          if (!isNaN(parsed)) {
            msgTimeMs = parsed;
          }
        }

        const ageMs = Date.now() - msgTimeMs;

        if (ageMs >= ARCHIVE_INTERVAL_MS) {
          const offset = message.offset;
          const paddedOffset = offset.padStart(20, '0');
          const content = message.value.toString();

          // Structured paths inside MinIO
          const objectKey = `${batch.topic}/partition=${batch.partition}/${paddedOffset}.json`;

          console.log(`[Archiver] Archiving message older than ${ARCHIVE_INTERVAL_MS}ms. Topic: ${batch.topic}, Offset: ${offset}, Object: ${objectKey}`);
          
          await minioClient.putObject(BUCKET_NAME, objectKey, content, {
            'Content-Type': 'application/json'
          });

          // Acknowledge and commit this specific offset
          resolveOffset(message.offset);
          await consumer.commitOffsets([{
            topic: batch.topic,
            partition: batch.partition,
            offset: (BigInt(offset) + 1n).toString()
          }]);
        } else {
          // This message is too new.
          // Pause processing this partition/topic, seek back to this offset, and sleep.
          console.log(`[Archiver] Message ${batch.topic}:${batch.partition}:${message.offset} is only ${ageMs}ms old (limit: ${ARCHIVE_INTERVAL_MS}ms). Pausing.`);
          
          await consumer.seek({
            topic: batch.topic,
            partition: batch.partition,
            offset: message.offset
          });

          // Sleep 5 seconds before returning (which lets the next poll delay safely)
          await new Promise(resolve => setTimeout(resolve, 5000));
          break;
        }

        await heartbeat();
      }
    }
  });
}

// ----------------------------------------------------
// Server Start
// ----------------------------------------------------

async function startServer() {
  initProducers();

  // Connect gateway producers
  try {
    await systemProducer.connect();
    console.log('[Gateway] System producer connected to Kafka.');
    for (const [tenantId, producer] of Object.entries(tenantProducers)) {
      await producer.connect();
      console.log(`[Gateway] Producer for ${tenantId} connected to Kafka.`);
    }
  } catch (err) {
    console.error('[Gateway] Failed to connect producers to Kafka:', err);
    process.exit(1);
  }

  // Start express app
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`[Gateway] HTTP Gateway service running on port ${PORT}`);
  });

  // Start background archiver
  // Wrap in timeout to give Kafka a brief moment after starting if needed
  setTimeout(() => {
    startArchiver().catch(err => {
      console.error('[Archiver] Error running archival worker:', err);
    });
  }, 5000);
}

startServer();
