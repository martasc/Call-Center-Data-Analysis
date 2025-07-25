from pathlib import Path
import os

# Base directory (points to src/)
BASE_DIR = Path(__file__).resolve().parent

# Input/Output directories
INPUT_DIR = BASE_DIR / "../input"
OUTPUT_DIR = BASE_DIR / "../output"

# Create directories if they don't exist
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# File paths (RAW_INPUT_FILE will be set dynamically when user uploads)
CLEAN_OUTPUT_FILE = OUTPUT_DIR / "todas.csv"    # Cleaned data
RECEBIDAS_FILE = OUTPUT_DIR / "recebidas.csv"   # Answered calls
NAO_ATENDIDAS_FILE = OUTPUT_DIR / "nao_atendidas.csv"  # Missed calls
DEVOLVIDAS_FILE = OUTPUT_DIR / "devolvidas.csv" # Returned calls