import csv
from pathlib import Path


def total(path):
    with Path(path).open(encoding="utf-8") as file:
        return sum(int(row["total"]) for row in csv.DictReader(file))


region = total("output/by_region.csv")
month = total("output/by_month.csv")
assert region == month and region > 0, f"Totaux incohérents : {region} / {month}"
print(f"Validation réussie : total {region}", flush=True)
