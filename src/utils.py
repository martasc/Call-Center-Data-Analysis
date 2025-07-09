import os
import glob
import shutil
import pandas as pd

def clear_output_directory(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Pasta criada: {output_dir}")
    else:
        for file_path in glob.glob(os.path.join(output_dir, "*")):
            try:
                os.remove(file_path)
            except IsADirectoryError:
                shutil.rmtree(file_path)
        print(f"Pasta limpa: {output_dir}")

def normalize_number(number):
    if pd.isna(number):
        return ""
    return (
        str(number)
        .strip()
        .replace("'", "")
        .replace(" ", "")
        .removeprefix("+351")
        .removeprefix("351")
    )