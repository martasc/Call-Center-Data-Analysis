import streamlit as st
from pathlib import Path
import pandas as pd
import sys
import os

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent))

from config import (
    INPUT_DIR,
    OUTPUT_DIR,
    RECEBIDAS_FILE,
    NAO_ATENDIDAS_FILE,
    DEVOLVIDAS_FILE
)
from utils import clear_output_directory
import metricas
from data_filtering import process_and_clean_input

def save_uploaded_file(uploaded_file):
    """Save uploaded file with proper error handling"""
    try:
        input_path = INPUT_DIR / "uploaded_data.csv"
        input_path.parent.mkdir(exist_ok=True)
        
        with open(input_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        return input_path
    except Exception as e:
        st.error(f"Failed to save file: {str(e)}")
        return None

def display_metrics():
    """Display processed metrics with visualizations"""
    try:
        # Verify files exist
        required_files = [RECEBIDAS_FILE, NAO_ATENDIDAS_FILE]
        if not all(f.exists() for f in required_files):
            st.error("Processed files not found. Please run analysis first.")
            return

        # Load data
        df_recebidas = pd.read_csv(RECEBIDAS_FILE, delimiter=';')
        df_nao_recebidas = pd.read_csv(NAO_ATENDIDAS_FILE, delimiter=';')

        # Calculate metrics
        total_calls = len(df_recebidas) + len(df_nao_recebidas)
        answered_pct = (len(df_recebidas) / total_calls * 100) if total_calls > 0 else 0

        # Display metrics
        st.subheader("📊 Call Center Metrics")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Calls", total_calls)
        col2.metric("Answered Calls", len(df_recebidas), f"{answered_pct:.1f}%")
        col3.metric("Missed Calls", len(df_nao_recebidas))

        # Add visualizations
        st.subheader("Call Distribution")
        chart_data = pd.DataFrame({
            'Call Type': ['Answered', 'Missed'],
            'Count': [len(df_recebidas), len(df_nao_recebidas)]
        })
        st.bar_chart(chart_data.set_index('Call Type'))

        # Add data downloads
        st.subheader("📥 Download Processed Data")
        with open(RECEBIDAS_FILE, "rb") as f:
            st.download_button(
                "Download Answered Calls",
                f,
                file_name="answered_calls.csv",
                mime="text/csv"
            )
        with open(NAO_ATENDIDAS_FILE, "rb") as f:
            st.download_button(
                "Download Missed Calls",
                f,
                file_name="missed_calls.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"Error displaying results: {str(e)}")

def main():
    st.set_page_config(
        page_title="Call Center Analytics",
        page_icon="📞",
        layout="wide"
    )
    st.title("📞 Call Center Analytics Dashboard")

    # File upload section
    uploaded_file = st.file_uploader(
        "Upload your call data (CSV)", 
        type=['csv'],
        help="Please upload the raw call data CSV file"
    )

    if uploaded_file is not None:
        # Save and process file
        input_path = save_uploaded_file(uploaded_file)
        
        if input_path and st.button("Run Analysis", type="primary"):
            with st.status("Processing data...", expanded=True) as status:
                try:
                    # Clear previous results
                    clear_output_directory(OUTPUT_DIR)
                    
                    # Process data
                    if process_and_clean_input(input_path):
                        # Generate metrics
                        metricas.analisar_chamadas()
                        status.update(label="Analysis complete!", state="complete")
                        st.success("✅ Processing completed successfully!")
                        st.balloons()
                        display_metrics()
                    else:
                        status.update(label="Processing failed", state="error")
                        st.error("Failed to process the input file")

                except Exception as e:
                    status.update(label="Analysis failed", state="error")
                    st.error(f"❌ Error during processing: {str(e)}")

if __name__ == "__main__":
    main()