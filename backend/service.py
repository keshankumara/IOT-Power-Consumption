"""MQTT -> NILM inference -> Web API bridge."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import joblib
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

LOGGER = logging.getLogger("nilm.service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ARTIFACT_PATH = BASE_DIR / "artifacts" / "nilm_rf.joblib"

LABEL_TO_DEVICE_STATES = {
    1: {"bulb": True, "fan": False, "iron": False},
    2: {"bulb": False, "fan": True, "iron": False},
    3: {"bulb": True, "fan": True, "iron": False},
    4: {"bulb": False, "fan": False, "iron": True},
    5: {"bulb": True, "fan": False, "iron": True},
    6: {"bulb": False, "fan": True, "iron": True},
    7: {"bulb": True, "fan": True, "iron": True},
}
DEVICE_METADATA = {
    "bulb": {"label": "Bulb", "nominal_w": 60},
    "fan": {"label": "Fan", "nominal_w": 80},
    "iron": {"label": "Iron", "nominal_w": 1100},
}


@dataclass
class MqttSettings:
    broker: str
    port: int
    topic: str
    keepalive: int = 60


class PredictionEngine:
    def __init__(self, artifact_path: Path):
        if not artifact_path.exists():
            raise FileNotFoundError(f"Model artifact not found at {artifact_path}")
        bundle: Dict[str, Any] = joblib.load(artifact_path)
        self.model = bundle["model"]
        self.feature_columns: List[str] = bundle["feature_columns"]
        self.class_labels: List[int] = [int(lbl) for lbl in bundle.get("class_labels", [])]
        self._lock = threading.Lock()
        LOGGER.info("Loaded model with %d features", len(self.feature_columns))

    def _vectorize(self, payload: Dict[str, Any]) -> List[List[float]]:
        row = [float(payload.get(col, 0.0) or 0.0) for col in self.feature_columns]
        return [row]

    def predict(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        features = self._vectorize(payload)
        with self._lock:
            label = int(self.model.predict(features)[0])
            probabilities = self.model.predict_proba(features)[0].tolist()
        probability_pairs = sorted(
            (
                {
                    "label": int(lbl),
                    "probability": float(prob),
                    "device_states": LABEL_TO_DEVICE_STATES.get(int(lbl), {
                        "bulb": False,
                        "fan": False,
                        "iron": False,
                    }),
                }
                for lbl, prob in zip(self.class_labels, probabilities)
            ),
            key=lambda item: item["probability"],
            reverse=True,
        )
        device_states = LABEL_TO_DEVICE_STATES.get(label, {
            "bulb": False,
            "fan": False,
            "iron": False,
        })
        return {
            "label": label,
            "device_states": device_states,
            "probabilities": probability_pairs,
        }


class LiveDataStore:
    def __init__(self, maxlen: int = 512):
        self.maxlen = maxlen
        self.history: deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self.latest: Dict[str, Any] | None = None
        self.total_messages = 0
        self._subscribers: set[asyncio.Queue] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def push_from_thread(self, message: Dict[str, Any]) -> None:
        if not self._loop:
            LOGGER.warning("Event loop not attached yet, dropping message")
            return
        self._loop.call_soon_threadsafe(self._fan_out, message)

    def _fan_out(self, message: Dict[str, Any]) -> None:
        self.latest = message
        self.history.append(message)
        self.total_messages += 1
        stale: list[asyncio.Queue] = []
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            self._subscribers.discard(queue)

    async def register(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=20)
        self._subscribers.add(queue)
        return queue

    def unregister(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def snapshot(self, limit: int) -> List[Dict[str, Any]]:
        limit = max(1, min(limit, self.maxlen))
        return list(self.history)[-limit:]


class MqttPredictionBridge:
    def __init__(self, settings: MqttSettings, predictor: PredictionEngine, store: LiveDataStore):
        self.settings = settings
        self.predictor = predictor
        self.store = store
        self.client: mqtt.Client | None = None
        self.connected = False
        self.messages_seen = 0

    def start(self) -> None:
        LOGGER.info("Starting MQTT bridge on %s:%s topic=%s", self.settings.broker, self.settings.port, self.settings.topic)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.connect(self.settings.broker, self.settings.port, self.settings.keepalive)
        self.client.loop_start()

    def stop(self) -> None:
        LOGGER.info("Stopping MQTT bridge")
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.client = None

    # MQTT callbacks -----------------------------------------------------
    def _on_connect(self, client, userdata, flags, reason_code, properties):  # noqa: D401
        self.connected = reason_code == 0
        if self.connected:
            LOGGER.info("MQTT connected, subscribing to %s", self.settings.topic)
            client.subscribe(self.settings.topic)
        else:
            LOGGER.error("MQTT connection failed rc=%s", reason_code)

    def _on_disconnect(self, client, userdata, reason_code, properties):  # noqa: D401
        self.connected = False
        LOGGER.warning("MQTT disconnected rc=%s", reason_code)

    def _on_message(self, client, userdata, msg):  # noqa: D401
        try:
            payload = json.loads(msg.payload.decode())
            features = {col: float(payload.get(col, 0.0) or 0.0) for col in self.predictor.feature_columns}
            prediction = self.predictor.predict(payload)
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": {"broker": self.settings.broker, "topic": msg.topic},
                "features": features,
                "raw": payload,
                "prediction": prediction,
            }
            self.messages_seen += 1
            self.store.push_from_thread(record)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.exception("Failed to process MQTT message: %s", exc)

    def status(self) -> Dict[str, Any]:
        return {
            "broker": self.settings.broker,
            "topic": self.settings.topic,
            "connected": self.connected,
            "messages_seen": self.messages_seen,
        }


# Instantiate singletons -------------------------------------------------
settings = MqttSettings(
    broker=os.getenv("NILM_MQTT_BROKER", "broker.hivemq.com"),
    port=int(os.getenv("NILM_MQTT_PORT", 1883)),
    topic=os.getenv("NILM_MQTT_TOPIC", "nivra_power/all"),
)
predictor = PredictionEngine(Path(os.getenv("NILM_MODEL_PATH", DEFAULT_ARTIFACT_PATH)))
store = LiveDataStore(maxlen=int(os.getenv("NILM_HISTORY_SIZE", 512)))
bridge = MqttPredictionBridge(settings, predictor, store)

allowed_origins = os.getenv("NILM_ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
allowed_origins = [origin.strip() for origin in allowed_origins if origin.strip()]

app = FastAPI(title="NILM Live Service", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _startup() -> None:
    loop = asyncio.get_running_loop()
    store.attach_loop(loop)
    bridge.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    bridge.stop()


@app.get("/api/status")
async def api_status() -> Dict[str, Any]:
    return {
        "mqtt": bridge.status(),
        "history_length": len(store.history),
        "total_messages": store.total_messages,
        "latest_timestamp": store.latest.get("timestamp") if store.latest else None,
        "devices": DEVICE_METADATA,
    }


@app.get("/api/latest")
async def api_latest() -> Dict[str, Any]:
    if not store.latest:
        raise HTTPException(status_code=404, detail="No data available yet")
    return store.latest


@app.get("/api/history")
async def api_history(limit: int = 50) -> Dict[str, Any]:
    return {
        "limit": limit,
        "items": store.snapshot(limit),
    }


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket) -> None:
    await websocket.accept()
    queue = await store.register()
    try:
        if store.latest:
            await websocket.send_json(store.latest)
        while True:
            message = await queue.get()
            await websocket.send_json(message)
    except WebSocketDisconnect:
        LOGGER.info("WebSocket disconnected")
    finally:
        store.unregister(queue)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.service:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
