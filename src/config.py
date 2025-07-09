from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "../input/maio.csv"
OUTPUT_DIR = BASE_DIR / "../output"
CLEAN_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "todas.csv")
