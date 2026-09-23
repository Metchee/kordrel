from pathlib import Path

output = Path("output")
output.mkdir(exist_ok=True)
count = output / "preparation-count.txt"
value = int(count.read_text() if count.exists() else "0") + 1
count.write_text(str(value))
print(f"Préparation exécutée {value} fois", flush=True)
