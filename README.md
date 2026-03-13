# BTC Dashboard Utilities

This repo now includes a Hyperliquid liquidation collector powered by the Hypedexer data feed.

## Hyperliquid Liquidation Collector

Script: `scripts/hyperliquid_liq_collector.py`

### Features
- Connects to Hypedexer WebSocket (`wss://api.hypedexer.com/ws/v1/liquidation`) using your API key
- Subscribes to the `all_liquidations` channel
- Aggregates liquidation events in 5-minute Beijing-time windows
- Writes CSV snapshots to `data/hyperliquid_liquidations/YYYY-MM-DD.csv`
- Keeps only the latest 3 days of CSV files

Sample output: `samples/hyperliquid_liquidations_sample.csv`

### Requirements
- Python 3.10+
- `websockets`, `python-dateutil`

Install dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the collector
```bash
export HYPEDEXER_API_KEY="hl_live_xxx"
python scripts/hyperliquid_liq_collector.py
```

Files are written/rotated automatically. Stop the script with `Ctrl+C`.
