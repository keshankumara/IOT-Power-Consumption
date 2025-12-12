// NILM Live Service (Node.js)
// Express + MQTT + WebSocket bridge replicating Python backend

const express = require('express');
const http = require('http');
const cors = require('cors');
const mqtt = require('mqtt');
const WebSocket = require('ws');

const LABEL_TO_DEVICE_STATES = {
  1: { bulb: true, fan: false, iron: false },
  2: { bulb: false, fan: true, iron: false },
  3: { bulb: true, fan: true, iron: false },
  4: { bulb: false, fan: false, iron: true },
  5: { bulb: true, fan: false, iron: true },
  6: { bulb: false, fan: true, iron: true },
  7: { bulb: true, fan: true, iron: true },
};

const DEVICE_METADATA = {
  bulb: { label: 'Bulb', nominal_w: 60 },
  fan: { label: 'Fan', nominal_w: 80 },
  iron: { label: 'Iron', nominal_w: 1100 },
};

// Config via environment
const MQTT_BROKER = process.env.NILM_MQTT_BROKER || 'broker.hivemq.com';
const MQTT_PORT = Number(process.env.NILM_MQTT_PORT || 1883);
const MQTT_TOPIC = process.env.NILM_MQTT_TOPIC || 'nivra_power/all';
const HISTORY_SIZE = Number(process.env.NILM_HISTORY_SIZE || 512);
const ALLOWED_ORIGINS = (process.env.NILM_ALLOWED_ORIGINS || 'http://localhost:5173,http://127.0.0.1:5173')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

// Minimal in-memory store
class LiveDataStore {
  constructor(maxlen = 512) {
    this.maxlen = maxlen;
    this.history = [];
    this.latest = null;
    this.totalMessages = 0;
    this.wsClients = new Set();
  }

  push(record) {
    this.latest = record;
    this.history.push(record);
    if (this.history.length > this.maxlen) {
      this.history.splice(0, this.history.length - this.maxlen);
    }
    this.totalMessages += 1;
    // broadcast to WS clients
    for (const ws of Array.from(this.wsClients)) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(record));
      } else {
        this.wsClients.delete(ws);
      }
    }
  }

  snapshot(limit = 50) {
    const n = Math.max(1, Math.min(limit, this.maxlen));
    return this.history.slice(-n);
  }
}

// Predictor stub: since sklearn joblib cannot be loaded in Node,
// we compute a naive label based on simple thresholds, then map states.
// Replace with a proper model service if needed.
class PredictionEngine {
  constructor(featureColumns = []) {
    this.featureColumns = featureColumns; // optional, not enforced here
  }

  predict(payload) {
    // naive heuristic: use apparent power if provided
    const p = Number(payload.power || payload.P || 0);
    const v = Number(payload.voltage || payload.V || 230);
    const i = Number(payload.current || payload.I || (v ? p / v : 0));

    let label = 1;
    if (p > 900) label = 4; // iron likely on
    else if (p > 120) label = 2; // fan likely on
    else if (p > 30) label = 1; // bulb likely on
    else label = 0; // none (not in map)

    const deviceStates = LABEL_TO_DEVICE_STATES[label] || { bulb: false, fan: false, iron: false };
    const probabilities = [1, 2, 3, 4, 5, 6, 7].map((lbl) => ({
      label: lbl,
      probability: lbl === label ? 0.8 : 0.2 / 6,
      device_states: LABEL_TO_DEVICE_STATES[lbl] || { bulb: false, fan: false, iron: false },
    })).sort((a, b) => b.probability - a.probability);

    return {
      label,
      device_states: deviceStates,
      probabilities,
    };
  }
}

// MQTT bridge
class MqttPredictionBridge {
  constructor({ broker, port, topic }, predictor, store) {
    this.broker = broker;
    this.port = port;
    this.topic = topic;
    this.predictor = predictor;
    this.store = store;
    this.client = null;
    this.connected = false;
    this.messagesSeen = 0;
  }

  start() {
    const url = `mqtt://${this.broker}:${this.port}`;
    this.client = mqtt.connect(url);
    this.client.on('connect', () => {
      this.connected = true;
      this.client.subscribe(this.topic, (err) => {
        if (err) console.error('MQTT subscribe error:', err);
      });
      console.log('MQTT connected to', url, 'topic=', this.topic);
    });
    this.client.on('reconnect', () => {
      console.log('MQTT reconnecting...');
    });
    this.client.on('close', () => {
      this.connected = false;
      console.warn('MQTT disconnected');
    });
    this.client.on('message', (topic, messageBuffer) => {
      try {
        const payload = JSON.parse(messageBuffer.toString());
        const features = { ...payload }; // pass-through
        const prediction = this.predictor.predict(payload);
        const record = {
          timestamp: new Date().toISOString(),
          source: { broker: this.broker, topic },
          features,
          raw: payload,
          prediction,
        };
        this.messagesSeen += 1;
        this.store.push(record);
      } catch (e) {
        console.error('Failed to process MQTT message:', e);
      }
    });
  }

  stop() {
    if (this.client) {
      try { this.client.end(true); } catch {}
      this.client = null;
    }
  }

  status() {
    return {
      broker: this.broker,
      topic: this.topic,
      connected: this.connected,
      messages_seen: this.messagesSeen,
    };
  }
}

// App setup
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws/live' });

app.use(express.json());
app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);
    if (ALLOWED_ORIGINS.length === 0 || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error('Not allowed by CORS'));
  },
  credentials: true,
}));

const store = new LiveDataStore(HISTORY_SIZE);
const predictor = new PredictionEngine();
const bridge = new MqttPredictionBridge({ broker: MQTT_BROKER, port: MQTT_PORT, topic: MQTT_TOPIC }, predictor, store);

// REST endpoints
app.get('/api/status', (req, res) => {
  res.json({
    mqtt: bridge.status(),
    history_length: store.history.length,
    total_messages: store.totalMessages,
    latest_timestamp: store.latest ? store.latest.timestamp : null,
    devices: DEVICE_METADATA,
  });
});

app.get('/api/latest', (req, res) => {
  if (!store.latest) return res.status(404).json({ detail: 'No data available yet' });
  res.json(store.latest);
});

app.get('/api/history', (req, res) => {
  const limit = Number(req.query.limit || 50);
  res.json({
    limit,
    items: store.snapshot(limit),
  });
});

// WebSocket broadcast: send latest on connect, then updates on push
wss.on('connection', (ws) => {
  store.wsClients.add(ws);
  if (store.latest) {
    try { ws.send(JSON.stringify(store.latest)); } catch {}
  }
  ws.on('close', () => {
    store.wsClients.delete(ws);
  });
});

const PORT = Number(process.env.PORT || 8000);

server.listen(PORT, () => {
  bridge.start();
  console.log(`NILM Live Service (Node) listening on http://localhost:${PORT}`);
});
