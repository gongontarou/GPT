# Bybit Funding Backtest

This repository contains a simple script to backtest a delta-neutral carry basket strategy on Bybit perpetual futures.

## Files

- `bt_config.py` – configuration for the backtest period, leverage, and filters.
- `bybit_funding_backtest.py` – main backtest script using Bybit public API.

## Usage

Install requirements:

```bash
pip install httpx==0.27.0 pandas scipy tqdm
```

Then run:

```bash
python3 bybit_funding_backtest.py
```

The script fetches funding data and prints performance statistics for the configured period.
