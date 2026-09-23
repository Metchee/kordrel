import csv
from collections import defaultdict
from pathlib import Path

totals = defaultdict(int)
with Path("output/sales.csv").open(encoding="utf-8") as file:
    for row in csv.DictReader(file):
        totals[row["month"]] += int(row["amount"])
with Path("output/by_month.csv").open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["month", "total"])
    writer.writerows(sorted(totals.items()))
print(f"Tendance mensuelle : {dict(totals)}", flush=True)
