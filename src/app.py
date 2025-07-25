import streamlit as st
from pathlib import Path
from config import (
    INPUT_DIR,
    OUTPUT_DIR,
    CLEAN_OUTPUT_FILE,
    RECEBIDAS_FILE,
    NAO_ATENDIDAS_FILE,
    DEVOLVIDAS_FILE
)
from utils import clear_output_directory
import metricas
from data_filtering import process_and_clean_input
import pandas as pd
import shutil

def save_uploaded_file(uploaded_file):
    """Save uploaded file to input directory with consistent name"""
    input_path = INPUT_DIR / "uploaded_data.csv"
    with open(input_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return input_path

def setup_cleaning_environment(input_file):
    """Run the processing pipeline"""
    with st.spinner('🧹 Cleaning output directory...'):
        clear_output_directory(OUTPUT_DIR)
    
    with st.spinner('🔧 Processing data...'):
        process_and_clean_input(input_file)  # Pass the uploaded file path
    
    with st.spinner('📊 Calculating metrics...'):
        return metricas.analisar_chamadas()

def display_results():
    """Display the processed data"""
    try:
        # Check files exist first
        if not RECEBIDAS_FILE.exists():
            st.error("Processed files not found. Please run the analysis first.")
            return
            
        # Load the processed data
        df_recebidas = pd.read_csv(RECEBIDAS_FILE, delimiter=';')
        df_nao_recebidas = pd.read_csv(NAO_ATENDIDAS_FILE, delimiter=';')
        
        # Basic metrics
        total_calls = len(df_recebidas) + len(df_nao_recebidas)
        answered_rate = len(df_recebidas)/total_calls*100 if total_calls > 0 else 0
        
        # Display metrics
        st.subheader("Call Metrics")
        col1, col2 = st.columns(2)
        col1.metric("Total Calls", total_calls)
        col2.metric("Answered Rate", f"{answered_rate:.1f}%")
        
        # Add download buttons
        st.subheader("Download Processed Data")
        with open(RECEBIDAS_FILE, "rb") as f:
            st.download_button("Download Answered Calls", f, file_name="answered_calls.csv")
        with open(NAO_ATENDIDAS_FILE, "rb") as f:
            st.download_button("Download Missed Calls", f, file_name="missed_calls.csv")
        
    except Exception as e:
        st.error(f"Error displaying results: {str(e)}")

def main():
    st.title("Call Center Analytics Dashboard")
    
    uploaded_file = st.file_uploader("Upload your call data (CSV)", type=['csv'])
    
    if uploaded_file is not None:
        # Save uploaded file
        input_path = save_uploaded_file(uploaded_file)
        
        if st.button("Run Analysis", type="primary"):
            with st.status("Processing data...", expanded=True) as status:
                try:
                    setup_cleaning_environment(input_path)
                    status.update(label="Analysis complete!", state="complete")
                    st.success("✅ Processing completed successfully!")
                    display_results()
                except Exception as e:
                    status.update(label="Analysis failed", state="error")
                    st.error(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()