# 📊 Cadence Calculator

A Streamlit web application for calculating and managing cadence scores based on ARMT, outflow, and historical data.

## Features

- 📁 Upload and process Excel files (ARMT, Master Cadence, Outflow)
- 📊 Interactive data visualizations
- 🔍 Filter data by multiple criteria
- 📥 Download processed reports
- 📈 Key metrics dashboard

## Live Demo

🔗 [https://cadence-calculator.streamlit.app](https://cadence-calculator.streamlit.app)

## Usage

1. Upload ARMT file (ARMT_AGCL.xlsx)
2. Upload previous month's cadence file
3. Upload outflow file
4. Select output month
5. Click "Process Cadence"
6. View results and download outputs

## Local Development

```bash
# Clone the repository
git clone https://github.com/yourusername/cadence-calculator.git
cd cadence-calculator

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run app.py