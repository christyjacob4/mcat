import csv
import json
import os
import pandas as pd

ROWS = [
    {"name": "Alice", "age": 30, "city": "NYC", "score": 95.5},
    {"name": "Bob", "age": 25, "city": "LA", "score": 87.3},
    {"name": "Carol", "age": 35, "city": "Chicago", "score": 91.8},
    {"name": "Dave", "age": 28, "city": "NYC", "score": 78.2},
    {"name": "Eve", "age": 32, "city": "LA", "score": 88.9},
]

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
os.makedirs(FIXTURE_DIR, exist_ok=True)

with open(os.path.join(FIXTURE_DIR, "sample.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["name", "age", "city", "score"])
    w.writeheader()
    w.writerows(ROWS)

with open(os.path.join(FIXTURE_DIR, "sample.tsv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["name", "age", "city", "score"], delimiter="\t")
    w.writeheader()
    w.writerows(ROWS)

with open(os.path.join(FIXTURE_DIR, "sample.json"), "w") as f:
    json.dump(ROWS, f, indent=2)

with open(os.path.join(FIXTURE_DIR, "sample.jsonl"), "w") as f:
    for row in ROWS:
        f.write(json.dumps(row) + "\n")

# Parquet
df = pd.DataFrame(ROWS)
for col in df.select_dtypes(include=["string", "object"]).columns:
    df[col] = df[col].astype("object")
df.to_parquet(os.path.join(FIXTURE_DIR, "sample.parquet"), engine="fastparquet")

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

import openpyxl
wb = openpyxl.Workbook()
ws = wb.active
ws.append(["name", "age", "city", "score"])
for r in ROWS:
    ws.append([r["name"], r["age"], r["city"], r["score"]])
wb.save(os.path.join(FIXTURE_DIR, "sample.xlsx"))

print("All fixtures created!")
