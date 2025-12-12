# NILM Real-Time System

Non-Intrusive Load Monitoring system with live MQTT data ingestion, Random Forest classification, and a React dashboard.

---

## Architecture

```
[Device/Sensor] ──MQTT──> [Backend Service] ──WebSocket──> [React Frontend]
                              |
                              ├─ MQTT subscriber (paho-mqtt)
                              ├─ RandomForest predictor (scikit-learn)
                              ├─ FastAPI REST + WebSocket
                              └─ Trained model artifact (.joblib)
```

### Components

1. **nivra.py** – PyQt5 desktop app for monitoring MQTT streams (original tooling)
2. **backend/** – FastAPI service that bridges MQTT → ML → WebSocket
   - `train_model.py` – Train the RF model offline
   - `service.py` – Live inference server
   - `artifacts/` – Persisted model
3. **frontend/** – React + Vite + Tailwind dashboard for real-time device classification

---

## Quick Start

### Prerequisites

- Python 3.10+
- Node.js 18+
- Active MQTT broker (default: `broker.hivemq.com`)

### 1. Train the model

```bash
python backend/train_model.py --data "full dataset.csv" --artifact backend/artifacts/nilm_rf.joblib
```

**Output:** `backend/artifacts/nilm_rf.joblib` (contains the trained RandomForest model + feature metadata)

### 2. Start the backend service

```bash
pip install -r backend/requirements.txt

# Option A: Using uvicorn directly
uvicorn backend.service:app --host 0.0.0.0 --port 8000 --reload

# Option B: Run the module
python -m backend.service
```

**Endpoints:**
- `GET /api/status` – MQTT connection status & metadata
- `GET /api/latest` – Most recent prediction
- `GET /api/history?limit=50` – Last N predictions
- `WS /ws/live` – WebSocket stream of live predictions

### 3. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Visit **http://localhost:5173**

---

## Environment Variables

### Backend (`backend/service.py`)

| Variable | Default | Description |
|----------|---------|-------------|
| `NILM_MQTT_BROKER` | `broker.hivemq.com` | MQTT broker hostname |
| `NILM_MQTT_PORT` | `1883` | MQTT broker port |
| `NILM_MQTT_TOPIC` | `nivra_power/all` | MQTT topic to subscribe |
| `NILM_MODEL_PATH` | `backend/artifacts/nilm_rf.joblib` | Path to trained model |
| `NILM_HISTORY_SIZE` | `512` | In-memory history buffer size |
| `NILM_ALLOWED_ORIGINS` | `http://localhost:5173,http://127.0.0.1:5173` | CORS allowed origins |
| `PORT` | `8000` | HTTP server port |

### Frontend (`frontend/.env`)

Create `.env.local` in `frontend/`:

```env
VITE_BACKEND_URL=http://localhost:8000
```

---

## MQTT Payload Format

The backend expects JSON payloads with these fields (matching the training dataset):

```json
{
  "voltage": 230.5,
  "current": 0.45,
  "active_power": 103.7,
  "reactive_power": 64.2,
  "apparent_power": 122.3,
  "power_factor": 0.85,
  "timestamp": "2025-12-11T12:34:56.789Z",
  "device_id": "power_monitor_1",
  "load_type": "unknown",
  "probe_id": "default"
}
```

**Required features for inference:** `voltage`, `current`, `active_power`, `reactive_power`, `apparent_power`, `power_factor`

---

## Device Labels

The model classifies loads into 7 categories based on which combination of devices is active:

| Label | Devices Active |
|-------|----------------|
| 1 | Bulb only |
| 2 | Fan only |
| 3 | Bulb + Fan |
| 4 | Iron only |
| 5 | Bulb + Iron |
| 6 | Fan + Iron |
| 7 | All devices (Bulb + Fan + Iron) |

**Nominal power:**
- Bulb: 60 W
- Fan: 80 W
- Iron: 1100 W

---

## Development

### Re-train the model

If you modify the dataset or want to tune hyperparameters:

```bash
python backend/train_model.py \
  --data "full dataset.csv" \
  --artifact backend/artifacts/nilm_rf.joblib
```

Check `NILM_RF.ipynb` for exploratory analysis.

### Run tests

```bash
# Backend unit tests (if added)
pytest backend/

# Frontend type check
cd frontend
npm run build
```

### Lint & format

```bash
# Python
ruff check backend/ nivra.py
black backend/ nivra.py

# TypeScript
cd frontend
npm run lint
```

---

## Deployment

### Backend (Dockerized)

Create `backend/Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/ backend/
COPY full\ dataset.csv .
ENV NILM_MODEL_PATH=backend/artifacts/nilm_rf.joblib
EXPOSE 8000
CMD ["uvicorn", "backend.service:app", "--host", "0.0.0.0", "--port", "8000"]
```

```bash
docker build -t nilm-backend .
docker run -p 8000:8000 \
  -e NILM_MQTT_BROKER=broker.hivemq.com \
  -e NILM_MQTT_TOPIC=nivra_power/all \
  nilm-backend
```

### Frontend (Static hosting)

```bash
cd frontend
npm run build
# Upload dist/ to Vercel, Netlify, or any static host
# Set env var VITE_BACKEND_URL to your backend URL
```

---

## Troubleshooting

### Backend doesn't receive MQTT messages

- Check broker connectivity: `telnet broker.hivemq.com 1883`
- Verify topic name matches publisher
- Inspect logs for connection errors

### Frontend shows "WebSocket error"

- Ensure backend is running on port 8000
- Check CORS configuration in `backend/service.py`
- Verify `VITE_BACKEND_URL` in frontend `.env.local`

### Model predictions are inaccurate

- Retrain with more diverse data
- Tune hyperparameters in `backend/train_model.py`
- Check feature scaling/normalization

---

## License

MIT

## Contributors

- Dineth Keragala ([@DinethKeragala](https://github.com/DinethKeragala))

---

**Last updated:** December 11, 2025
