import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

LOG_DIR = "logs-optimized"
os.makedirs(LOG_DIR, exist_ok=True)

SERVICES = ["api", "orders", "auth"]
LEVELS = ["info", "warning", "error"]

print("Replaying 600 log events with OPTIMIZED label architecture...")

for i in range(600):
    service = random.choice(SERVICES)
    level = random.choices(LEVELS, weights=[0.8, 0.15, 0.05])[0]
    
    req_id = str(uuid.uuid4())[:8]
    u_id = f"user_{random.randint(1000, 9999)}"
    o_id = f"ord_{random.randint(50000, 99999)}" if service == "orders" else ""

    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "level": level,
        "request_id": req_id,
        "user_id": u_id,
        "order_id": o_id,
        "message": f"Processed {service} action successfully for {u_id}"
    }

    log_file = os.path.join(LOG_DIR, f"{service}.log")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry) + "\n")

    time.sleep(0.01)

print("Workload replay complete. Pushed to logs-optimized directory.")