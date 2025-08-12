from pathlib import Path
import sys

if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = BASE_DIR / "output"
CLEAN_OUTPUT_FILE = OUTPUT_DIR / "cleaned.csv"
RECEBIDAS_FILE = OUTPUT_DIR / "recebidas.csv"
DEVOLVIDAS_FILE = OUTPUT_DIR / "devolvidas.csv"
