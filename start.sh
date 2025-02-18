#!/bin/bash
# Start the Python API in the background
python src/api/main.py &
# Start the Streamlit app (ensure it uses $PORT)
streamlit run src/app/main.py --server.port=$PORT
