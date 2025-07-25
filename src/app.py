import streamlit as st
from utils import clear_output_directory
import metricas
from data_filtering import process_and_clean_input
from config import OUTPUT_DIR
import time
import os

# Set page config
st.set_page_config(
    page_title="Data Processing Dashboard",
    page_icon="📊",
    layout="wide"
)

def setup_cleaning_environment():
    """Original processing function"""
    with st.spinner('Cleaning output directory...'):
        clear_output_directory(OUTPUT_DIR)
    
    with st.spinner('Processing and cleaning data...'):
        process_and_clean_input()
    
    with st.spinner('Analyzing metrics...'):
        metricas.analisar_chamadas()

def main():
    st.title("📈 Data Processing Dashboard")
    st.markdown("""
    This app processes your input data and generates metrics.
    """)
    
    # File upload section
    uploaded_file = st.file_uploader(
        "Upload your data file (CSV/Excel)", 
        type=['csv', 'xlsx', 'xls'],
        key="file_uploader"
    )
    
    if uploaded_file is not None:
        # Save uploaded file to input directory
        input_path = os.path.join(OUTPUT_DIR, "uploaded_file.csv")
        with open(input_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success("File uploaded successfully!")
        
        if st.button("Run Analysis", type="primary"):
            with st.status("Processing data...", expanded=True) as status:
                try:
                    setup_cleaning_environment()
                    status.update(label="Processing complete!", state="complete")
                    st.balloons()
                    
                    # Display results
                    st.subheader("Results")
                    # Add your metric displays here
                    # Example:
                    # st.metric("Total Calls", metricas.total_calls)
                    
                except Exception as e:
                    status.update(label=f"Error: {str(e)}", state="error")
                    st.error(f"An error occurred: {str(e)}")

    # Add download button for results
    if os.path.exists(os.path.join(OUTPUT_DIR, "processed_data.csv")):
        with open(os.path.join(OUTPUT_DIR, "processed_data.csv"), "rb") as f:
            st.download_button(
                "Download Processed Data",
                f,
                file_name="processed_results.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()