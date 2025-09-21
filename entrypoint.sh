#!/bin/bash
# entrypoint.sh

set -e

# Function to handle SIGTERM for graceful shutdown
graceful_shutdown() {
    echo "Received shutdown signal, stopping client..."
    if [ -f /tmp/client.pid ]; then
        kill -TERM $(cat /tmp/client.pid) 2>/dev/null || true
        rm -f /tmp/client.pid
    fi
    exit 0
}

trap graceful_shutdown SIGTERM SIGINT

# If no command provided, start interactive shell
if [ $# -eq 0 ]; then
    exec /bin/bash
fi

# If command is to run the client
if [ "$1" = "run-client" ]; then
    shift
    echo "Starting BitTorrent client with arguments: $@"
    
    # Run the client in the background and save PID
    python main.py "$@" &
    CLIENT_PID=$!
    echo $CLIENT_PID > /tmp/client.pid
    
    # Wait for the client to finish
    wait $CLIENT_PID
    rm -f /tmp/client.pid
else
    # Execute any other command
    exec "$@"
fi
