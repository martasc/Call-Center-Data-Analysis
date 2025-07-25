import streamlit as st

from utils import clear_output_directory
import metricas
from data_filtering import process_and_clean_input
from config import OUTPUT_DIR

def setup_cleaning_environment():
    clear_output_directory(OUTPUT_DIR)
    process_and_clean_input()
    metricas.analisar_chamadas()

def main():
    st.title("Data Cleaning & Analysis")
    st.write("Click the button below to process and clean the input data.")

    if st.button("Run Cleaning Pipeline"):
        with st.spinner("Processing..."):
            setup_cleaning_environment()
        st.success("Processing complete. Check outputs in your dashboard or notify your admin for retrieval.")

if __name__ == "__main__":
    main()
