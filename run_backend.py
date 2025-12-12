#!/usr/bin/env python
"""Quick launcher for the NILM backend service."""

import os
import sys
from pathlib import Path

# Ensure backend module is importable
sys.path.insert(0, str(Path(__file__).parent))

if __name__ == "__main__":
    import uvicorn
    from backend.service import app

    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")

    print(f"🚀 Starting NILM backend service on {host}:{port}")
    print(f"   MQTT broker: {os.getenv('NILM_MQTT_BROKER', 'broker.hivemq.com')}")
    print(f"   MQTT topic: {os.getenv('NILM_MQTT_TOPIC', 'nivra_power/all')}")
    print(f"   Model: {os.getenv('NILM_MODEL_PATH', 'backend/artifacts/nilm_rf.joblib')}")
    print()

    uvicorn.run(
        "backend.service:app",
        host=host,
        port=port,
        reload=os.getenv("DEV_MODE", "0") == "1",
        log_level="info",
    )
