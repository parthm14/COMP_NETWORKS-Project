#!/usr/bin/env bash
# Run on Machine 1 (turbine)
cd "$(dirname "$0")"
python3 turbine/turbine.py --config config.json
