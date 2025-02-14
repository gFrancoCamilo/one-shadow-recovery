#!/bin/bash

# Function to check for panics in logs
check_for_panics() {
    if grep -nr 'panic' logs/; then
        echo "Panic found in logs. Restarting..."
        return 1
    else
        echo "Done."
        return 0
    fi
}

check_shifting_remaining() {
    filename="logs/node-0.log"  # Replace with the actual log file path
    if [ ! -f "$filename" ]; then
        echo "Log file '$filename' does not exist."
        return 1
    fi

    shifting_count=$(grep -o "Shifting" "$filename" | wc -l)
    remaining_count=$(grep -o "Remaining" "$filename" | wc -l)
    total=$((shifting_count + remaining_count))


    if [ $total -ge 2 ]; then
        return 0
    else
        echo "The sum of 'Shifting' and 'Remaining' is less than 2. Restarting..."
        return 1
    fi
}

while true; do
    fab localmal

    # Sometimes the code panics. In this case, we run again
    if ! check_for_panics; then
        continue
    fi

    if ! check_shifting_remaining; then
        continue
    fi

    python parse_logs2.py 31
    break
done
