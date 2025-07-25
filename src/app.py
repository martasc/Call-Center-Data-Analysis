import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import tempfile
from io import BytesIO
from utils import clear_output_directory
import metricas
from data_filtering import process_and_clean_input
from config import OUTPUT_DIR
import os

# Set page config
st.set_page_config(
    page_title="Call Center Analytics Dashboard",
    page_icon="📞",
    layout="wide"
)

def setup_cleaning_environment():
    """Run the processing pipeline and return metrics"""
    with st.spinner('🧹 Cleaning output directory...'):
        clear_output_directory(OUTPUT_DIR)
    
    with st.spinner('🔧 Processing data...'):
        process_and_clean_input()
    
    with st.spinner('📊 Calculating metrics...'):
        # Run analysis and capture printed output
        metrics = metricas.analisar_chamadas()
    return metrics

def create_download_button(df, filename):
    """Create a download button for a DataFrame"""
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label=f"Download {filename}",
        data=csv,
        file_name=filename,
        mime='text/csv'
    )

def display_metrics():
    """Display all metrics with visualizations"""
    st.subheader("📊 Call Center Performance Metrics")
    
    # Load the processed data
    try:
        df_recebidas = pd.read_csv(os.path.join(OUTPUT_DIR, "recebidas.csv"), delimiter=";")
        df_nao_recebidas = pd.read_csv(os.path.join(OUTPUT_DIR, "nao_atendidas.csv"), delimiter=";")
        df_devolvidas = pd.read_csv(os.path.join(OUTPUT_DIR, "devolvidas.csv"), delimiter=";")
    except Exception as e:
        st.error(f"Error loading data files: {e}")
        return

    # Calculate metrics (similar to your analisar_chamadas function)
    total_chamadas = len(df_recebidas) + len(df_nao_recebidas)
    total_atendidas = len(df_recebidas)
    total_nao_atendidas = len(df_nao_recebidas)
    percent_atendidas = (total_atendidas / total_chamadas * 100) if total_chamadas > 0 else 0
    
    # Convert wait times to seconds
    df_recebidas["Wait_Seconds"] = df_recebidas["Tempo de Toque"].apply(metricas.parse_tempo)
    chamadas_rapidas = (df_recebidas["Wait_Seconds"] < 60).sum()
    perc_rapidas = (chamadas_rapidas / total_atendidas * 100) if total_atendidas > 0 else 0
    
    # Create tabs for different metric categories
    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Answered Calls", "Missed Calls", "Returned Calls"])
    
    with tab1:
        st.subheader("Overall Statistics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Calls", total_chamadas)
        col2.metric("Answered Calls", total_atendidas, f"{percent_atendidas:.1f}%")
        col3.metric("Missed Calls", total_nao_atendidas, f"{100-percent_atendidas:.1f}%")
        
        # Call distribution pie chart
        fig1, ax1 = plt.subplots()
        ax1.pie([total_atendidas, total_nao_atendidas], 
                labels=["Answered", "Missed"], 
                colors=["green", "red"],
                autopct='%1.1f%%')
        ax1.set_title("Call Distribution")
        st.pyplot(fig1)
        
    with tab2:
        st.subheader("Answered Calls Analysis")
        col1, col2, col3 = st.columns(3)
        col1.metric("Fast Answers (<60s)", chamadas_rapidas, f"{perc_rapidas:.1f}%")
        col2.metric("Avg Wait Time", f"{df_recebidas['Wait_Seconds'].mean():.1f}s")
        col3.metric("Avg Duration", f"{df_recebidas['Duração'].apply(metricas.parse_tempo).mean():.1f}s")
        
        # Wait time distribution
        fig2, ax2 = plt.subplots()
        ax2.hist(df_recebidas["Wait_Seconds"], bins=20, color='green')
        ax2.set_xlabel("Wait Time (seconds)")
        ax2.set_ylabel("Number of Calls")
        ax2.set_title("Wait Time Distribution for Answered Calls")
        st.pyplot(fig2)
        
    with tab3:
        st.subheader("Missed Calls Analysis")
        st.metric("Unique Callers Not Answered", df_nao_recebidas["Origem"].nunique())
        
    with tab4:
        st.subheader("Returned Calls Analysis")
        if not df_devolvidas.empty:
            df_devolvidas["Return_Seconds"] = df_devolvidas["Tempo até Devolução (s)"]
            col1, col2 = st.columns(2)
            col1.metric("Returned within 3min", 
                       f"{(df_devolvidas['Return_Seconds'] <= 180).mean()*100:.1f}%")
            col2.metric("Returned within 15min", 
                       f"{(df_devolvidas['Return_Seconds'] <= 900).mean()*100:.1f}%")
            
            # Return time distribution
            fig3, ax3 = plt.subplots()
            ax3.hist(df_devolvidas["Return_Seconds"], bins=20, color='blue')
            ax3.set_xlabel("Return Time (seconds)")
            ax3.set_ylabel("Number of Calls")
            ax3.set_title("Time to Return Missed Calls")
            st.pyplot(fig3)
        else:
            st.warning("No returned calls data available")
    
    # Data download section
    st.subheader("📥 Download Processed Data")
    create_download_button(df_recebidas, "answered_calls.csv")
    create_download_button(df_nao_recebidas, "missed_calls.csv")
    if not df_devolvidas.empty:
        create_download_button(df_devolvidas, "returned_calls.csv")

def main():
    st.title("📞 Call Center Analytics Dashboard")
    
    uploaded_file = st.file_uploader("Upload your call data (CSV)", type=["csv"])
    
    if uploaded_file is not None:
        # Save uploaded file
        input_path = os.path.join(OUTPUT_DIR, "uploaded_data.csv")
        with open(input_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        if st.button("Run Full Analysis", type="primary"):
            with st.status("Processing data...", expanded=True) as status:
                try:
                    setup_cleaning_environment()
                    status.update(label="Analysis complete!", state="complete")
                    st.success("✅ Processing completed successfully!")
                    display_metrics()
                except Exception as e:
                    status.update(label="Analysis failed", state="error")
                    st.error(f"❌ Error: {str(e)}")
                    st.exception(e)

if __name__ == "__main__":
    main()