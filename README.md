# Financial Stress Test Generator

A machine learning system for forecasting company and portfolio behavior under extreme economic conditions.

## Team Members
1. Novia Vijay Dsilva
2. Sanika Anant Chaudhari
3. Parth Sanjay Saraykar
4. Sushmitha Sudharsan
5. Priyanka Senthil Kumar
6. Sailee Ritesh Choudhari

## Project Overview

This system generates synthetic macroeconomic scenarios and predicts financial outcomes under stress conditions such as recession, inflation spikes, and market freezes.

## Setup Instructions

### Prerequisites
- Python 3.8 or higher
- Git
- pip

### Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd financial-stress-test-generator
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys
```

5. Initialize DVC:
```bash
dvc init
```

## Project Structure
```
financial-stress-test-generator/
├── data/
│   ├── raw/              # Raw data from sources
│   ├── processed/        # Cleaned and transformed data
│   └── scenarios/        # Generated stress scenarios
├── notebooks/            # Jupyter notebooks for exploration
├── src/                  # Source code
│   ├── data/            # Data loading and preprocessing
│   ├── models/          # Model implementations
│   ├── evaluation/      # Evaluation metrics
│   └── visualization/   # Dashboard and plots
├── tests/               # Unit tests
├── models/              # Saved model artifacts
├── configs/             # Configuration files
└── results/             # Output results
```

## Data Sources

- **FRED**: Federal Reserve Economic Data
- **World Bank**: Global economic indicators
- **Yahoo Finance**: Company financial data
- **IMF**: International economic outlook

## Usage

### Data Collection
```bash
python src/data/data_loader.py
```

### Model Training
```bash
python src/models/train.py
```

### Run Dashboard
```bash
streamlit run dashboard/app.py
```

## Development Status

- [ ] Data Collection
- [ ] Data Preprocessing
- [ ] Scenario Generation Model
- [ ] Predictive Modeling
- [ ] Anomaly Detection
- [ ] Dashboard Development
- [ ] Deployment

## License

Academic Project - For Educational Purposes Only

## Contact

For questions or issues, please contact the team members.