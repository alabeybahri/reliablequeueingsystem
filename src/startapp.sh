#!/bin/bash

# Check if number of brokers is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <number_of_brokers>"
    exit 1
fi

NUM_BROKERS=$1
BASE_PORT=5000

echo "Compiling Java code..."
javac -d out broker/*.java client/*.java common/*.java

if [ $? -ne 0 ]; then
    echo "Compilation failed. Exiting."
    exit 1
fi

echo "Compilation successful. Starting $NUM_BROKERS brokers..."

# Calculate a sensible grid layout
COLUMNS=2
if [ $NUM_BROKERS -gt 4 ]; then
    COLUMNS=3
fi
if [ $NUM_BROKERS -gt 9 ]; then
    COLUMNS=4
fi

# Get screen resolution for macOS
SCREEN_WIDTH=$(system_profiler SPDisplaysDataType | grep Resolution | awk '{print $2}')
SCREEN_HEIGHT=$(system_profiler SPDisplaysDataType | grep Resolution | awk '{print $4}')

# Default values if not detected
if [ -z "$SCREEN_WIDTH" ] || [ -z "$SCREEN_HEIGHT" ]; then
    SCREEN_WIDTH=960
    SCREEN_HEIGHT=600
fi

# Calculate window size and rows
CELL_WIDTH=$((SCREEN_WIDTH / COLUMNS))
CELL_HEIGHT=$((SCREEN_HEIGHT / ((NUM_BROKERS + COLUMNS - 1) / COLUMNS)))

WINDOW_WIDTH=$((CELL_WIDTH * 2 / 3))
WINDOW_HEIGHT=$((CELL_HEIGHT * 2 / 3))

echo "Detected macOS, arranging terminals in a ${COLUMNS}x${ROWS} grid..."

# Start brokers in a grid using Terminal.app
for (( i=1; i<=$NUM_BROKERS; i++ ))
do
    PORT=$(($BASE_PORT + $i))
    echo "Starting broker on port $PORT"

    # Grid position
    ROW=$(( (i-1) / COLUMNS ))
    COL=$(( (i-1) % COLUMNS ))

    POS_X=$((COL * WINDOW_WIDTH))
    POS_Y=$((ROW * WINDOW_HEIGHT))
    WIN_RIGHT=$((POS_X + WINDOW_WIDTH))
    WIN_BOTTOM=$((POS_Y + WINDOW_HEIGHT))

    osascript <<EOF
tell application "Terminal"
    activate
    set newTab to do script "cd $(pwd); read"
    delay 0.3
    set bounds of front window to {$POS_X, $POS_Y, $WIN_RIGHT, $WIN_BOTTOM}
end tell
EOF

    sleep 0.7
done

echo "All brokers started."
