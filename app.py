"""
Cadence Calculator - Multi-Node Streamlit Web Application
Supports BLR, IAS, and GDN nodes
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from Processors import BLRProcessor, IASProcessor, GDNProcessor
from config import APP_NAME, APP_VERSION, MONTHS
import io
from datetime import datetime
import time

# ============== Page Configuration ==============
st.set_page_config(
    page_title=APP_NAME,
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============== Node Processor Mapping ==============
PROCESSORS = {
    'BLR': BLRProcessor,
    'IAS': IASProcessor,
    'GDN': GDNProcessor
}

NODE_COLORS = {
    'BLR': '#667eea',
    'IAS': '#28a745',
    'GDN': '#dc3545'
}

# ============== Custom CSS ==============
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0.5rem;
    }

    .sub-header {
        font-size: 1.1rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }

    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        padding: 20px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
        margin-bottom: 10px;
    }

    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
    }

    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }

    .node-badge {
        display: inline-block;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        color: white;
        margin: 5px;
    }

    .node-blr { background-color: #667eea; }
    .node-ias { background-color: #28a745; }
    .node-gdn { background-color: #dc3545; }

    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 10px 30px;
        font-weight: bold;
    }

    .log-container {
        background-color: #1e1e1e;
        color: #00ff00;
        font-family: 'Courier New', monospace;
        padding: 15px;
        border-radius: 10px;
        max-height: 300px;
        overflow-y: auto;
    }

    .info-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 15px;
    }

    .footer {
        text-align: center;
        color: #666;
        padding: 20px;
        font-size: 0.85rem;
    }
</style>
""", unsafe_allow_html=True)

# ============== Session State ==============
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'processing_logs' not in st.session_state:
    st.session_state.processing_logs = []
if 'selected_node' not in st.session_state:
    st.session_state.selected_node = 'BLR'


# ============== Helper Functions ==============
def create_download_buffer(df, filename):
    """Create a download buffer for Excel file"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output


def display_metrics(cadence_df, node_name):
    """Display key metrics in cards"""
    node_color = NODE_COLORS.get(node_name, '#667eea')

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="metric-container" style="background: {node_color};">
            <div class="metric-value">{len(cadence_df):,}</div>
            <div class="metric-label">Total Records</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        classes_current = len(cadence_df[cadence_df['NC Count'] != 'not Found!'])
        st.markdown(f"""
        <div class="metric-container" style="background: {node_color};">
            <div class="metric-value">{classes_current:,}</div>
            <div class="metric-label">Classes in Current Month</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        nc_found = len(cadence_df[cadence_df['NC Count'] != 'not Found!'])
        st.markdown(f"""
        <div class="metric-container" style="background: {node_color};">
            <div class="metric-value">{nc_found:,}</div>
            <div class="metric-label">Records with NC</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        jsr_yes = len(cadence_df[cadence_df['JSR'] == 'Yes']) if 'JSR' in cadence_df.columns else 0
        st.markdown(f"""
        <div class="metric-container" style="background: {node_color};">
            <div class="metric-value">{jsr_yes:,}</div>
            <div class="metric-label">JSR Policies</div>
        </div>
        """, unsafe_allow_html=True)


def create_visualizations(cadence_df):
    """Create interactive visualizations"""
    col1, col2 = st.columns(2)

    with col1:
        if 'Cadence Score' in cadence_df.columns:
            score_df = cadence_df[cadence_df['Cadence Score'] != 'not Found!'].copy()
            score_df['Cadence Score'] = pd.to_numeric(score_df['Cadence Score'], errors='coerce')
            score_df = score_df.dropna(subset=['Cadence Score'])

            if len(score_df) > 0:
                score_counts = score_df['Cadence Score'].value_counts().reset_index()
                score_counts.columns = ['Cadence Score', 'Count']

                fig = px.pie(
                    score_counts,
                    values='Count',
                    names='Cadence Score',
                    title='📊 Cadence Score Distribution',
                    color_discrete_sequence=px.colors.sequential.Purples_r,
                    hole=0.4
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        if 'risk score' in cadence_df.columns:
            risk_df = cadence_df.copy()
            risk_df['risk score'] = pd.to_numeric(risk_df['risk score'], errors='coerce')
            risk_df = risk_df.dropna(subset=['risk score'])

            if len(risk_df) > 0:
                risk_counts = risk_df['risk score'].value_counts().sort_index().reset_index()
                risk_counts.columns = ['Risk Score', 'Count']

                fig = px.bar(
                    risk_counts,
                    x='Risk Score',
                    y='Count',
                    title='📈 Risk Score Distribution',
                    color='Count',
                    color_continuous_scale='Purples'
                )
                st.plotly_chart(fig, use_container_width=True)

    if 'Source' in cadence_df.columns:
        source_df = cadence_df[cadence_df['Source'] != 'not Found!'].copy()

        if len(source_df) > 0:
            source_counts = source_df['Source'].value_counts().head(10).reset_index()
            source_counts.columns = ['Source', 'Count']

            fig = px.bar(
                source_counts,
                y='Source',
                x='Count',
                title='🌍 Top 10 Sources',
                orientation='h',
                color='Count',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)


def display_logs(logs):
    """Display processing logs"""
    log_html = "<div class='log-container'>"
    for log in logs:
        log_html += f"<div>> {log}</div>"
    log_html += "</div>"
    st.markdown(log_html, unsafe_allow_html=True)


def process_with_status(processor_class, files_dict, output_month):
    """Process files with real-time status updates"""

    status_container = st.status("🚀 Processing Cadence...", expanded=True)
    current_status = st.empty()
    log_expander = st.expander("📋 Detailed Processing Logs", expanded=True)
    log_placeholder = log_expander.empty()
    progress_bar = st.progress(0)

    live_logs = []

    def update_status(message):
        """Callback function to update UI"""
        live_logs.append(message)

        current_status.markdown(f"""
        <div style="background: #1e1e1e; padding: 15px; border-radius: 10px; margin: 10px 0;">
            <span style="color: #ffcc00; font-family: 'Courier New', monospace; font-size: 1.1rem;">
                ⏳ {message}
            </span>
        </div>
        """, unsafe_allow_html=True)

        log_html = ""
        for i, log in enumerate(live_logs):
            if i == len(live_logs) - 1:
                log_html += f'<div style="color: #ffcc00; padding: 5px 0;">▶ {log}</div>'
            else:
                log_html += f'<div style="color: #00ff00; padding: 5px 0;">✓ {log}</div>'

        log_placeholder.markdown(f"""
        <div style="background: #1e1e1e; padding: 15px; border-radius: 10px; 
                    font-family: 'Courier New', monospace; max-height: 400px; overflow-y: auto;">
            {log_html}
        </div>
        """, unsafe_allow_html=True)

        progress_mapping = {
            'Starting': 5, 'Reading': 10, 'ARMT processed': 25,
            'Outflow processed': 40, 'node mappings': 50,
            'Creating Cadence': 55, 'Cadence created': 65,
            'master lookups': 70, 'Merging': 75,
            'Updating Cadence': 80, 'Calculating due': 85,
            'Finalizing': 90, 'ready': 100,
        }

        for key, progress in progress_mapping.items():
            if key.lower() in message.lower():
                progress_bar.progress(progress)
                break

    processor = processor_class(status_callback=update_status)

    with status_container:
        st.write("Processing your files...")
        result = processor.process(files_dict, output_month)

    if result['success']:
        status_container.update(label="✅ Processing Complete!", state="complete", expanded=False)
        current_status.markdown(f"""
        <div style="background: #155724; padding: 15px; border-radius: 10px; margin: 10px 0;">
            <span style="color: #d4edda; font-family: 'Courier New', monospace; font-size: 1.1rem;">
                ✅ Cadence for {output_month} is ready! Total records: {len(result['cadence']):,}
            </span>
        </div>
        """, unsafe_allow_html=True)
        progress_bar.progress(100)
    else:
        status_container.update(label="❌ Processing Failed", state="error", expanded=True)
        current_status.markdown(f"""
        <div style="background: #721c24; padding: 15px; border-radius: 10px; margin: 10px 0;">
            <span style="color: #f8d7da; font-family: 'Courier New', monospace; font-size: 1.1rem;">
                ❌ Error: {result.get('error', 'Unknown error')}
            </span>
        </div>
        """, unsafe_allow_html=True)

    return result


# ============== Main Application ==============
def main():
    # Header
    st.markdown(f'<h1 class="main-header">📊 {APP_NAME}</h1>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-header">Multi-Node Cadence Calculator | v{APP_VERSION}</p>', unsafe_allow_html=True)

    # ============== Sidebar ==============
    with st.sidebar:
        # Node Selection
        st.markdown("### 🌐 Select Node")
        selected_node = st.selectbox(
            "Choose processing node",
            options=['BLR', 'IAS', 'GDN'],
            index=['BLR', 'IAS', 'GDN'].index(st.session_state.selected_node),
            format_func=lambda x: f"{'🔵' if x == 'BLR' else '🟢' if x == 'IAS' else '🔴'} {x} Node"
        )
        st.session_state.selected_node = selected_node

        # Get processor and its config
        processor_class = PROCESSORS[selected_node]
        temp_processor = processor_class()
        node_config = temp_processor.get_node_config()
        required_files = temp_processor.get_required_files()

        # Display node info
        st.markdown(f"""
        <div style="background: {node_config['color']}; padding: 10px; border-radius: 10px; color: white; text-align: center;">
            <strong>{node_config['full_name']}</strong><br>
            <small>{node_config['description']}</small>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("### 📁 File Upload")

        # Dynamic file uploads based on selected node
        uploaded_files = {}
        all_files_uploaded = True

        for i, file_info in enumerate(required_files):
            st.markdown(f"#### {i + 1}️⃣ {file_info['label']}")
            uploaded_file = st.file_uploader(
                file_info['description'],
                type=['xlsx', 'xls'],
                help=file_info['help'],
                key=f"{selected_node}_{file_info['key']}"
            )
            if uploaded_file:
                st.success(f"✅ {uploaded_file.name}")
                uploaded_files[file_info['key']] = uploaded_file
            else:
                all_files_uploaded = False

        st.markdown("---")

        # Output month selection
        st.markdown("#### 📅 Output Month")
        current_month_idx = datetime.now().month - 1
        output_month = st.selectbox(
            "Select Month",
            MONTHS,
            index=current_month_idx,
            label_visibility="collapsed"
        )

        st.markdown("---")

        # Process button
        process_clicked = st.button(
            f"🚀 Process {selected_node} Cadence",
            type="primary",
            disabled=not all_files_uploaded,
            use_container_width=True
        )

        if not all_files_uploaded:
            st.warning("⚠️ Please upload all files")

        st.markdown("---")

        with st.expander("ℹ️ Instructions"):
            st.markdown(f"""
            1. Select **Node** ({selected_node})
            2. Upload all required files
            3. Select the **Output Month**
            4. Click **Process {selected_node} Cadence**
            5. View results and download outputs
            """)

    # ============== Main Content ==============

    if process_clicked and all_files_uploaded:
        st.session_state.processed_data = None
        st.session_state.processing_logs = []

        result = process_with_status(processor_class, uploaded_files, output_month)

        if result['success']:
            st.session_state.processed_data = result
            st.session_state.processing_logs = result['logs']
            st.balloons()
            time.sleep(1)
            st.rerun()
        else:
            st.error(f"❌ Processing failed: {result.get('error', 'Unknown error')}")
            if 'logs' in result:
                st.session_state.processing_logs = result['logs']

    # Display Results
    if st.session_state.processed_data and st.session_state.processed_data['success']:
        data = st.session_state.processed_data
        node_name = data.get('node', selected_node)

        st.success(f"🎉 {node_name} Cadence for {output_month} processed successfully!")

        st.markdown("### 📈 Key Metrics")
        display_metrics(data['cadence'], node_name)

        st.markdown("---")

        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 Data View", "📊 Visualizations", "📥 Downloads", "📝 Logs"
        ])

        with tab1:
            st.markdown("### 📋 Cadence Data")

            col1, col2, col3, col4 = st.columns(4)
            cadence_df = data['cadence'].copy()

            with col1:
                sources = ['All'] + sorted([s for s in cadence_df['Source'].unique() if s != 'not Found!'])
                selected_source = st.selectbox("Filter by Source", sources)

            with col2:
                scores = ['All'] + sorted(
                    [str(s) for s in cadence_df['Cadence Score'].unique() if str(s) != 'not Found!'])
                selected_score = st.selectbox("Filter by Cadence Score", scores)

            with col3:
                jsr_options = ['All', 'Yes', 'No']
                selected_jsr = st.selectbox("Filter by JSR", jsr_options)

            with col4:
                programs = ['All'] + sorted([p for p in cadence_df['program'].unique() if p != 'not Found!'])
                selected_program = st.selectbox("Filter by Program", programs)

            filtered_df = cadence_df.copy()
            if selected_source != 'All':
                filtered_df = filtered_df[filtered_df['Source'] == selected_source]
            if selected_score != 'All':
                filtered_df = filtered_df[filtered_df['Cadence Score'].astype(str) == selected_score]
            if selected_jsr != 'All':
                filtered_df = filtered_df[filtered_df['JSR'] == selected_jsr]
            if selected_program != 'All':
                filtered_df = filtered_df[filtered_df['program'] == selected_program]

            st.markdown(f"**Showing {len(filtered_df):,} of {len(cadence_df):,} records**")
            st.dataframe(filtered_df, use_container_width=True, height=500)

        with tab2:
            st.markdown("### 📊 Data Visualizations")
            create_visualizations(data['cadence'])

        with tab3:
            st.markdown("### 📥 Download Output Files")

            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown(f"#### 📊 {node_name} Cadence Report")
                cadence_buffer = create_download_buffer(data['cadence'], f'{output_month}_{node_name}_Cadence.xlsx')
                st.download_button(
                    label="⬇️ Download Cadence",
                    data=cadence_buffer,
                    file_name=f"{output_month}_{node_name}_Cadence.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            with col2:
                st.markdown("#### 📋 Master Report")
                master_buffer = create_download_buffer(data['master'], f'{node_name}_Master.xlsx')
                st.download_button(
                    label="⬇️ Download Master",
                    data=master_buffer,
                    file_name=f"{node_name}_Master.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            with col3:
                st.markdown("#### 📁 ARMT Report")
                armt_buffer = create_download_buffer(data['armt'], f'{node_name}_ARMT.xlsx')
                st.download_button(
                    label="⬇️ Download ARMT",
                    data=armt_buffer,
                    file_name=f"{node_name}_ARMT.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            st.markdown("---")
            st.markdown("#### 📦 Download All Data")

            all_data_buffer = io.BytesIO()
            with pd.ExcelWriter(all_data_buffer, engine='openpyxl') as writer:
                data['cadence'].to_excel(writer, sheet_name='Cadence', index=False)
                data['master'].to_excel(writer, sheet_name='Master', index=False)
                data['armt'].to_excel(writer, sheet_name='ARMT', index=False)
            all_data_buffer.seek(0)

            st.download_button(
                label="⬇️ Download All Reports (Single File)",
                data=all_data_buffer,
                file_name=f"{output_month}_{node_name}_All_Reports.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        with tab4:
            st.markdown("### 📝 Processing Logs")
            display_logs(st.session_state.processing_logs)

            st.markdown("---")
            if st.button("🗑️ Clear Results & Start Over", use_container_width=True):
                st.session_state.processed_data = None
                st.session_state.processing_logs = []
                st.rerun()

    else:
        if not process_clicked:
            st.markdown("---")

            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown(f"""
                <div style="text-align: center; padding: 50px; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); border-radius: 20px;">
                    <h2>👋 Welcome to Cadence Calculator</h2>
                    <p style="color: #666; font-size: 1.1rem;">
                        Select a node and upload files to get started.
                    </p>
                    <br>
                    <div>
                        <span class="node-badge node-blr">🔵 BLR</span>
                        <span class="node-badge node-ias">🟢 IAS</span>
                        <span class="node-badge node-gdn">🔴 GDN</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("---")
            st.markdown("### ✨ Features")

            col1, col2, col3, col4 = st.columns(4)
            features = [
                ("🌐 Multi-Node", "Support for BLR, IAS, and GDN nodes"),
                ("📊 Processing", "Automated cadence calculation"),
                ("📈 Visualizations", "Interactive charts and graphs"),
                ("📥 Export", "Download in Excel format")
            ]

            for col, (title, desc) in zip([col1, col2, col3, col4], features):
                with col:
                    st.markdown(f"""
                    <div class="info-card">
                        <h4>{title}</h4>
                        <p>{desc}</p>
                    </div>
                    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(
        f'<p class="footer">📊 {APP_NAME} v{APP_VERSION} | Made by RAJ 😎 | © {datetime.now().year}</p>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()