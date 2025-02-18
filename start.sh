#!/bin/bash
# Start the Python API in the background
python src/api/main.py &
# Wait for the API to initialize
sleep 5
# Start the Streamlit app using the Heroku-provided port
streamlit run src/app/main.py --server.port=$PORT
