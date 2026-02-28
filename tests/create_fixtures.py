import csv
import json
import os

ROWS = [
    {"name": "Alice", "age": 30, "city": "NYC", "score": 95.5},
    {"name": "Bob", "age": 25, "city": "LA", "score": 87.3},
    {"name": "Carol", "age": 35, "city": "Chicago", "score": 91.8},
    {"name": "Dave", "age": 28, "city": "NYC", "score": 78.2},
    {"name": "Eve", "age": 32, "city": "LA", "score": 88.9},
]

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
os.makedirs(FIXTURE_DIR, exist_ok=True)

# CSV
with open(os.path.join(FIXTURE_DIR, "sample.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["name", "age", "city", "score"])
    w.writeheader()
    w.writerows(ROWS)

# TSV
with open(os.path.join(FIXTURE_DIR, "sample.tsv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["name", "age", "city", "score"], delimiter="\t")
    w.writeheader()
    w.writerows(ROWS)

# JSON
with open(os.path.join(FIXTURE_DIR, "sample.json"), "w") as f:
    json.dump(ROWS, f, indent=2)

# JSONL
with open(os.path.join(FIXTURE_DIR, "sample.jsonl"), "w") as f:
    for row in ROWS:
        f.write(json.dumps(row) + "\n")

# Parquet
import pyarrow as pa
import pyarrow.parquet as pq
table = pa.table({
    "name": [r["name"] for r in ROWS],
    "age": [r["age"] for r in ROWS],
    "city": [r["city"] for r in ROWS],
    "score": [r["score"] for r in ROWS],
})
pq.write_table(table, os.path.join(FIXTURE_DIR, "sample.parquet"))

# Avro
import fastavro
avro_schema = {
    "type": "record",
    "name": "Sample",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "city", "type": "string"},
        {"name": "score", "type": "double"},
    ]
}
with open(os.path.join(FIXTURE_DIR, "sample.avro"), "wb") as f:
    fastavro.writer(f, avro_schema, ROWS)

# Excel
import openpyxl
wb = openpyxl.Workbook()
ws = wb.active
ws.append(["name", "age", "city", "score"])
for r in ROWS:
    ws.append([r["name"], r["age"], r["city"], r["score"]])
wb.save(os.path.join(FIXTURE_DIR, "sample.xlsx"))

print("All fixtures created!")
