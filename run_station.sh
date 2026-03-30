#!/usr/bin/env bash
# Run on Machine 3 (control station / space station)
cd "$(dirname "$0")"
python3 control_station/station.py --config config.json
