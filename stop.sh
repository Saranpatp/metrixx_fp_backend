#!/bin/bash

# Check if main.pid file exists
if [ ! -f main.pid ]; then
  echo "main.pid file not found!"
  exit 1
fi

# Get the PID from the file
PID=$(cat main.pid)

# Kill the process
kill $PID

# Remove the PID file
rm main.pid

echo "main.py stopped"