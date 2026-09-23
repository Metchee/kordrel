import csv
import os
from pathlib import Path

output = Path("output")
output.mkdir(exist_ok=True)
year = os.environ.get("KORDREL_DEMO_YEAR", "2026")
rows = [
    (f"{year}-01", "Nord", 120),
    (f"{year}-01", "Sud", 95),
    (f"{year}-02", "Nord", 135),
    (f"{year}-02", "Sud", 110),
    (f"{year}-03", "Nord", 142),
    (f"{year}-03", "Sud", 127),
]
with (output / "sales.csv").open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["month", "region", "amount"])
    writer.writerows(rows)
print(f"Données préparées : {len(rows)} ventes dans {output / 'sales.csv'}", flush=True)
