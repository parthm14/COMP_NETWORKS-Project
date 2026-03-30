#!/usr/bin/env bash
# Run on Machine 3 (control station)
cd "$(dirname "$0")"
streamlit run control_station/streamlit_app.py --server.address 0.0.0.0 --server.port 8501
