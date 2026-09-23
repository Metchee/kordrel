import csv
from collections import defaultdict
from pathlib import Path

totals = defaultdict(int)
with Path("output/sales.csv").open(encoding="utf-8") as file:
    for row in csv.DictReader(file):
        totals[row["region"]] += int(row["amount"])
with Path("output/by_region.csv").open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["region", "total"])
    writer.writerows(sorted(totals.items()))
print(f"Agrégation par région : {dict(totals)}", flush=True)
