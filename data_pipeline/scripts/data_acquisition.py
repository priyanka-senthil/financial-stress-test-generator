"""
Data Acquisition Module
Handles fetching data from various sources
"""

import os
import pandas as pd
import logging
from fredapi import Fred
import yfinance as yf
from datetime import datetime
from pathlib import Path
import yaml
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataAcquisition:
    """Class to handle data acquisition from multiple sources"""
    
    def __init__(self, config_path=None):
        """Initialize with configuration"""
        # Determine project root
        self.project_root = Path(__file__).resolve().parent.parent.parent
        
        # Load environment variables from project root
        env_path = self.project_root / '.env'
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            logger.info(f"Loaded .env from {env_path}")
        else:
            logger.warning(f".env file not found at {env_path}")
        
        # Load config
        if config_path is None:
            config_path = self.project_root / 'configs' / 'config.yaml'
        
        self.config = self._load_config(config_path)
        
        # Get API key
        self.fred_api_key = os.getenv('FRED_API_KEY')
        
        if not self.fred_api_key:
            logger.error("FRED_API_KEY not found in environment variables")
            logger.error(f"Checked .env file at: {env_path}")
            logger.error("Please ensure .env file exists in project root with FRED_API_KEY=your_key")
            raise ValueError("FRED_API_KEY not found")
        
        logger.info(f"FRED API key loaded successfully (length: {len(self.fred_api_key)})")
        
        self.fred = Fred(api_key=self.fred_api_key)
        self.output_dir = self.project_root / 'data_pipeline' / 'data' / 'raw'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("DataAcquisition initialized successfully")
    
    def _load_config(self, config_path):
        """Load configuration file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config from {config_path}: {e}")
            raise
    
    def fetch_macro_data(self):
        """Fetch macroeconomic data from FRED"""
        logger.info("Starting macroeconomic data collection")
        
        indicators = {
            'GDP': 'GDP',
            'CPI': 'CPIAUCSL',
            'UNEMPLOYMENT': 'UNRATE',
            'FEDERAL_FUNDS': 'FEDFUNDS',
            'TREASURY_10Y': 'DGS10',
            'TREASURY_2Y': 'DGS2',
        }
        
        start_date = self.config['data']['start_date']
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        macro_data = {}
        failed_indicators = []
        
        for name, series_id in indicators.items():
            try:
                data = self.fred.get_series(series_id, start_date, end_date)
                macro_data[name] = data
                logger.info(f"✓ Fetched {name}: {len(data)} observations")
            except Exception as e:
                logger.error(f"✗ Failed to fetch {name}: {str(e)}")
                failed_indicators.append(name)
        
        if not macro_data:
            raise Exception("No macroeconomic data collected")
        
        # Combine into DataFrame
        macro_df = pd.DataFrame(macro_data)
        
        # Save to CSV
        output_path = self.output_dir / 'fred_macroeconomic_data.csv'
        macro_df.to_csv(output_path)
        logger.info(f"✓ Saved macroeconomic data to {output_path}")
        
        # Log summary
        logger.info(f"Macro data shape: {macro_df.shape}")
        logger.info(f"Date range: {macro_df.index[0]} to {macro_df.index[-1]}")
        
        if failed_indicators:
            logger.warning(f"Failed indicators: {failed_indicators}")
        
        return str(output_path)
    
    def fetch_company_data(self, ticker, sector):
        """Fetch data for a single company"""
        logger.info(f"Fetching data for {ticker} ({sector})")
        
        try:
            stock = yf.Ticker(ticker)
            
            # Historical prices
            hist_prices = stock.history(period='max')
            
            if hist_prices.empty:
                raise ValueError(f"No price data available for {ticker}")
            
            # Financial statements
            financials = stock.financials
            quarterly_financials = stock.quarterly_financials
            balance_sheet = stock.balance_sheet
            cashflow = stock.cashflow
            
            # Save data
            company_dir = self.output_dir / 'companies'
            company_dir.mkdir(exist_ok=True)
            
            hist_prices.to_csv(company_dir / f'{ticker}_prices.csv')
            
            if not financials.empty:
                financials.to_csv(company_dir / f'{ticker}_financials_annual.csv')
            if not quarterly_financials.empty:
                quarterly_financials.to_csv(company_dir / f'{ticker}_financials_quarterly.csv')
            if not balance_sheet.empty:
                balance_sheet.to_csv(company_dir / f'{ticker}_balance_sheet.csv')
            if not cashflow.empty:
                cashflow.to_csv(company_dir / f'{ticker}_cashflow.csv')
            
            logger.info(f"✓ Saved data for {ticker} ({len(hist_prices)} price points)")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error fetching {ticker}: {str(e)}")
            return False
    
    def fetch_all_companies(self):
        """Fetch data for all companies in config"""
        companies = self.config['data']['companies']
        logger.info(f"Fetching data for {len(companies)} companies")
        
        success_count = 0
        failed_companies = []
        
        for ticker, sector in companies.items():
            if self.fetch_company_data(ticker, sector):
                success_count += 1
            else:
                failed_companies.append(ticker)
        
        logger.info(f"✓ Successfully collected data for {success_count}/{len(companies)} companies")
        
        if failed_companies:
            logger.warning(f"Failed companies: {failed_companies}")
        
        if success_count == 0:
            raise Exception("No company data collected")
        
        return success_count


def acquire_macro_data(**context):
    """Airflow task function for macro data acquisition"""
    acquirer = DataAcquisition()
    output_path = acquirer.fetch_macro_data()
    if context:
        context['task_instance'].xcom_push(key='macro_data_path', value=output_path)
    return output_path


def acquire_company_data(**context):
    """Airflow task function for company data acquisition"""
    acquirer = DataAcquisition()
    success_count = acquirer.fetch_all_companies()
    if context:
        context['task_instance'].xcom_push(key='companies_collected', value=success_count)
    return success_count


if __name__ == "__main__":
    # Test the module
    logger.info("="*60)
    logger.info("TESTING DATA ACQUISITION MODULE")
    logger.info("="*60)
    
    try:
        acquirer = DataAcquisition()
        
        # Test macro data
        logger.info("\n" + "="*60)
        logger.info("COLLECTING MACROECONOMIC DATA")
        logger.info("="*60)
        acquirer.fetch_macro_data()
        
        # Test company data
        logger.info("\n" + "="*60)
        logger.info("COLLECTING COMPANY DATA")
        logger.info("="*60)
        acquirer.fetch_all_companies()
        
        logger.info("\n" + "="*60)
        logger.info("✓ ALL DATA COLLECTION COMPLETE!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n✗ Data collection failed: {e}")
        raise