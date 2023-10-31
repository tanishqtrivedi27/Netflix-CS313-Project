#!/bin/bash

# Install Python dependencies
pip install -r requirements.txt

redis-cli FLUSHALL

# Execute Python scripts
python tables.py
python populate.py
python app.py &

# Wait for a moment to ensure the server is up
sleep 5

# Open the browser to the localhost
xdg-open http://localhost:5000
