import csv
from pathlib import Path


def read(path):
    with Path(path).open(encoding="utf-8") as file:
        return list(csv.DictReader(file))


regions = read("output/by_region.csv")
months = read("output/by_month.csv")
lines = ["# Rapport des ventes", "", "## Par région", ""]
lines += [f"- {row['region']} : {row['total']}" for row in regions]
lines += ["", "## Par mois", ""]
lines += [f"- {row['month']} : {row['total']}" for row in months]
Path("output/report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print("Rapport généré : output/report.md", flush=True)
