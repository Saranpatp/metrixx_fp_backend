#!/bin/bash

source ../base_env/bin/activate
python main.py &

# Save the PID to a file
echo $! > main.pid

echo "main.py started with PID $(cat main.pid)"