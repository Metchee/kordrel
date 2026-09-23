import time
from pathlib import Path

marker = Path("output/interruption.marker")
if marker.exists():
    print("Reprise : marqueur trouvé, tâche terminée", flush=True)
else:
    marker.write_text("première tentative")
    print("Première tentative : attente de 30 s ; pressez Ctrl-C dans Kordrel", flush=True)
    time.sleep(30)
