#!/bin/bash
# Start the Python API in the background
uvicorn src.api.main:app --host 0.0.0.0 --port 8000
