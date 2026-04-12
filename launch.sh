#!/usr/bin/env bash
# WTSP — Wind Turbine Space Protocol
# Single launch script — starts all three nodes in separate terminal windows
# Usage: ./launch.sh

cd "$(dirname "$0")"

echo "======================================================"
echo "  WTSP — Wind Turbine Space Protocol"
echo "  Starting all three nodes..."
echo "======================================================"

# Kill any leftover processes on our ports before starting
echo "[*] Clearing ports 7001-7005, 8001-8002, 9001..."
for port in 7001 7002 7003 7004 7005 8001 8002 9001 9102 9103 9104; do
    lsof -ti:$port | xargs kill -9 2>/dev/null
done
sleep 1

# Detect OS to open terminals correctly
OS="$(uname -s)"

if [[ "$OS" == "Darwin" ]]; then
    # macOS — open three Terminal windows
    echo "[*] Detected macOS — opening three Terminal windows..."

    osascript -e "tell application \"Terminal\" to do script \"cd '$(pwd)' && echo '=== SATELLITE ===' && python3 satellite/satellite.py --config config.json\""
    sleep 1

    osascript -e "tell application \"Terminal\" to do script \"cd '$(pwd)' && echo '=== TURBINE ===' && python3 turbine/turbine.py --config config.json\""
    sleep 1

    osascript -e "tell application \"Terminal\" to do script \"cd '$(pwd)' && echo '=== CONTROL STATION ===' && python3 control_station/station.py --config config.json\""

elif [[ "$OS" == "Linux" ]]; then
    # Linux — try common terminal emulators
    echo "[*] Detected Linux — opening three terminal windows..."

    if command -v gnome-terminal &>/dev/null; then
        gnome-terminal -- bash -c "cd '$(pwd)' && echo '=== SATELLITE ===' && python3 satellite/satellite.py --config config.json; exec bash"
        sleep 1
        gnome-terminal -- bash -c "cd '$(pwd)' && echo '=== TURBINE ===' && python3 turbine/turbine.py --config config.json; exec bash"
        sleep 1
        gnome-terminal -- bash -c "cd '$(pwd)' && echo '=== CONTROL STATION ===' && python3 control_station/station.py --config config.json; exec bash"

    elif command -v xterm &>/dev/null; then
        xterm -title "SATELLITE" -e "cd '$(pwd)' && python3 satellite/satellite.py --config config.json" &
        sleep 1
        xterm -title "TURBINE" -e "cd '$(pwd)' && python3 turbine/turbine.py --config config.json" &
        sleep 1
        xterm -title "CONTROL STATION" -e "cd '$(pwd)' && python3 control_station/station.py --config config.json" &

    else
        # Fallback — run all three in background, station in foreground
        echo "[*] No GUI terminal found — running in background (satellite + turbine) and foreground (station)..."
        python3 satellite/satellite.py --config config.json &
        SAT_PID=$!
        echo "[*] Satellite started (PID $SAT_PID)"
        sleep 1

        python3 turbine/turbine.py --config config.json &
        TUR_PID=$!
        echo "[*] Turbine started (PID $TUR_PID)"
        sleep 1

        echo "[*] Starting Control Station (foreground)..."
        python3 control_station/station.py --config config.json

        # Cleanup on exit
        kill $SAT_PID $TUR_PID 2>/dev/null
    fi

else
    # Unknown OS fallback — background processes
    echo "[*] Unknown OS — running satellite and turbine in background, station in foreground..."
    python3 satellite/satellite.py --config config.json &
    SAT_PID=$!
    sleep 1

    python3 turbine/turbine.py --config config.json &
    TUR_PID=$!
    sleep 1

    python3 control_station/station.py --config config.json

    kill $SAT_PID $TUR_PID 2>/dev/null
fi

echo "======================================================"
echo "  All nodes launched."
echo "  Satellite : ports 8001, 8002"
echo "  Turbine   : ports 7001, 7002, 7003, 7004, 7005"
echo "  Station   : port  9001"
echo "======================================================"