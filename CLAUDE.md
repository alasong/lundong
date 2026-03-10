# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A 股热点轮动预测系统 (A-Share Sector Rotation Prediction System) - A machine learning-based system for analyzing and predicting sector rotation patterns in the Chinese A-share stock market.

## Quick Start

### Setup

```bash
cd /home/song/lundong
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Environment Configuration

Create `.env` file:
```bash
TUSHARE_TOKEN=your_tushare_token_here
DASHSCOPE_API_KEY=your_dashscope_key_here  # Optional, for LLM features
DATABASE_URL=sqlite:///data/stock.db
LOG_LEVEL=INFO
```

### Running the System

```bash
# Full daily workflow (data collection + analysis + prediction)
python src/main.py --mode daily --train

# Quick analysis (use existing data)
python src/main.py --mode quick

# Train model only
python src/main.py --mode train

# Collect historical data
python src/main.py --mode history --start-date 20230101 --end-date 20231231

# Collect daily data
python src/main.py --mode data
```

### Testing

```bash
# Run simple test
python test_simple.py

# Run with pytest
pytest tests/
```

## Architecture

```
src/
├── main.py              # Entry point
├── config.py            # Configuration (Settings class)
├── runner.py            # Workflow orchestrator (SimpleRunner)
│
├── agents/              # Agent layer
│   ├── base_agent.py    # Base agent class
│   ├── data_agent.py    # Data collection agent
│   ├── analysis_agent.py # Hotspot analysis agent
│   └── predict_agent.py # Prediction agent
│
├── analysis/            # Core analysis
│   ├── hotspot_detector.py    # Hotspot detection
│   ├── rotation_analyzer.py   # Rotation pattern analysis
│   └── pattern_learner.py     # Pattern learning
│
├── data/                # Data layer
│   ├── tushare_client.py       # Tushare API client (East Money)
│   ├── tushare_ths_client.py   # Tushare THS client (Tonghuashun)
│   ├── data_collector.py       # Data collection orchestrator
│   └── feature_engineer.py     # Feature engineering
│
├── models/              # Prediction models
│   └── predictor.py     # XGBoost-based predictor
│
├── evaluation/          # Model evaluation
│   └── metrics.py       # Evaluation metrics
│
├── learning/            # Learning module
│   └── rotation_learner.py # Rotation pattern learner
│
├── core/                # Core utilities
│   └── settings.py      # Core settings
│
└── utils/               # Utilities
    └── logger.py        # Logging utilities
```

## Key Components

### Data Sources

| Source | Client Class | Purpose |
|--------|-------------|---------|
| Tushare Pro (East Money) | `TushareClient` | East Money sector/concept data |
| Tushare Pro (Tonghuashun) | `TushareTHSClient` | Tonghuashun sector/index data |

### Data Collection Modes

| Task | Description | Command |
|------|-------------|---------|
| `lists` | Collect sector/concept lists | `agent.run(task="lists")` |
| `daily` | Collect daily data | `agent.run(task="daily")` |
| `history` | Collect historical data | `agent.run(task="history", start_date, end_date)` |
| `basic` | Collect basic data | `agent.run(task="basic")` |

### Running Modes

| Mode | Description | Command |
|------|-------------|---------|
| `daily` | Daily workflow | `--mode daily --train` |
| `quick` | Quick analysis | `--mode quick` |
| `train` | Train model | `--mode train` |
| `data` | Data collection | `--mode data` |
| `history` | Historical data collection | `--mode history --start-date X --end-date Y` |

## Data Directory Structure

```
data/
├── raw/          # Raw CSV data from Tushare
├── processed/    # Processed data
├── features/     # Feature engineering results
├── models/       # Trained models
└── results/      # Analysis results
```

## Configuration

Key settings in `src/config.py`:

```python
class Settings(BaseSettings):
    # API tokens
    tushare_token: str
    dashscope_api_key: str

    # Data paths
    data_dir: str = "data"
    raw_data_dir: str = "data/raw"

    # Hotspot weights
    hotspot_weights: dict = {
        "price_strength": 0.30,
        "money_strength": 0.25,
        "sentiment_strength": 0.20,
        "persistence": 0.15,
        "market_position": 0.10,
    }

    # Prediction horizons
    prediction_horizons: dict = {
        "short_term": 1,
        "mid_term": 5,
        "long_term": 20,
    }
```

## Development

### Code Style

- Python 3.12+
- Type hints recommended
- Loguru for logging

### Pre-commit Hooks

```bash
# Install pre-commit
pip install pre-commit
pre-commit install

# Run manually
pre-commit run --all-files
```

## Tushare API Reference

### East Money Interfaces (dc_*)
- `dc_concept()` - Concept list (5000 concepts)
- `dc_index()` - Sector list (5000 sectors)
- `dc_member()` - Sector constituents
- `dc_daily()` - Sector daily quotes (primary)

### Tonghuashun Interfaces (ths_*)
- `ths_index()` - THS index list
- `ths_industry()` - THS industry classification
- `ths_daily()` - THS daily quotes

### Backup Interfaces
- `index_daily()` - Index daily quotes (backup for dc_daily)

## Troubleshooting

### Missing TUSHARE_TOKEN
```bash
# Check .env file
cat .env | grep TUSHARE_TOKEN

# Set environment variable
export TUSHARE_TOKEN=your_token
```

### dc_daily Returns Empty Data
The system automatically falls back to `index_daily()` interface when `dc_daily()` returns empty data.

### Permission Issues
Ensure Tushare account has >= 5000 points for accessing sector/concept data.

## Related Documentation

- `README.md` - Project overview and quick start
- `ARCHITECTURE.md` - Detailed architecture documentation
- `QUICKSTART.md` - Quick start guide
- `TUSHARE_QUICKSTART.md` - Tushare setup guide

## Disclaimer

This system is for research and educational purposes only. It does not constitute investment advice. The stock market involves risks; invest cautiously.
