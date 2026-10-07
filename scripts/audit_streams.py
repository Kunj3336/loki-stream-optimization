import urllib.request
import json

LOKI_HOST = "http://localhost:3100"

def get_loki_json(path):
    url = f"{LOKI_HOST}{path}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"Error querying {url}: {e}")
        return {}

print("Auditing Loki stream cardinality and index metrics...")

# 1. Fetch all series / streams in the last 1 hour
series_data = get_loki_json('/loki/api/v1/series?match[]={job="demo-apps"}')
streams = series_data.get("data", [])
total_streams = len(streams)

# 2. Count streams per service
service_streams = {"api": 0, "orders": 0, "auth": 0}
for item in streams:
    svc = item.get("service")
    if svc in service_streams:
        service_streams[svc] += 1

# 3. Check cardinality of dynamic labels
labels_to_check = ["request_id", "user_id", "order_id", "service", "level"]
cardinality_counts = {}

for lbl in labels_to_check:
    val_data = get_loki_json(f"/loki/api/v1/label/{lbl}/values")
    values = val_data.get("data", [])
    cardinality_counts[lbl] = len(values)

print("=" * 75)
print("LOKI STREAM CARDINALITY AUDIT REPORT (BASELINE - UNOPTIMIZED)")
print("=" * 75)
print(f"Total Active Loki Streams Created : {total_streams}")
print("-" * 75)
print("Streams by Service:")
for svc, count in service_streams.items():
    print(f"  - {svc:<10} : {count} unique streams")
print("-" * 75)
print("Label Cardinality (Distinct Values Indexed):")
for lbl, count in cardinality_counts.items():
    flag = " [CRITICAL: HIGH CARDINALITY]" if count > 5 else " [OPTIMAL]"
    print(f"  - {lbl:<15} : {count:<5} unique values{flag}")
print("=" * 75)