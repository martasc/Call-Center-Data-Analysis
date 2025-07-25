from pathlib import Path
import os

# Base directory (points to src/)
BASE_DIR = Path(__file__).resolve().parent

# Input/Output directories
INPUT_DIR = BASE_DIR.parent / "input"
OUTPUT_DIR = BASE_DIR.parent / "output"

# Create directories if they don't exist
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Processed file paths
CLEAN_OUTPUT_FILE = OUTPUT_DIR / "todas.csv"
RECEBIDAS_FILE = OUTPUT_DIR / "recebidas.csv"
NAO_ATENDIDAS_FILE = OUTPUT_DIR / "nao_atendidas.csv"
DEVOLVIDAS_FILE = OUTPUT_DIR / "devolvidas.csv"