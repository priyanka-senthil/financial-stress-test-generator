"""
Data loading module for Financial Stress Test Generator
"""

import os
import pandas as pd
import numpy as np
from fredapi import Fred
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import yaml

# Load environment variables
load_dotenv()

class DataLoader:
    """Class to handle all data loading operations"""
    
    def __init__(self, config_path='configs/config.yaml'):
        """Initialize data loader with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.fred_api_key = os.getenv('FRED_API_KEY')
        if not self.fred_api_key:
            raise ValueError("FRED_API_KEY not found in environment variables")
        
        self.fred = Fred(api_key=self.fred_api_key)
        self.data_dir = Path('data/raw')
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def collect_macro_data(self):
        """Collect macroeconomic data from FRED"""
        print("Collecting macroeconomic data from FRED...")
        
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
        for name, series_id in indicators.items():
            try:
                data = self.fred.get_series(series_id, start_date, end_date)
                macro_data[name] = data
                print(f"✓ Collected {name}: {len(data)} observations")
            except Exception as e:
                print(f"✗ Error collecting {name}: {str(e)}")
        
        # Combine into DataFrame
        macro_df = pd.DataFrame(macro_data)
        
        # Save to CSV
        output_path = self.data_dir / 'fred_macroeconomic_data.csv'
        macro_df.to_csv(output_path)
        print(f"\n✓ Saved macroeconomic data to {output_path}")
        
        return macro_df
    
    def collect_company_data(self, ticker, sector):
        """Collect data for a single company"""
        print(f"\nCollecting data for {ticker} ({sector})...")
        
        try:
            stock = yf.Ticker(ticker)
            
            # Historical prices
            hist_prices = stock.history(period='max')
            
            # Financial statements
            financials = stock.financials
            quarterly_financials = stock.quarterly_financials
            balance_sheet = stock.balance_sheet
            cashflow = stock.cashflow
            
            # Save data
            company_dir = self.data_dir / 'companies'
            company_dir.mkdir(exist_ok=True)
            
            hist_prices.to_csv(company_dir / f'{ticker}_prices.csv')
            financials.to_csv(company_dir / f'{ticker}_financials_annual.csv')
            quarterly_financials.to_csv(company_dir / f'{ticker}_financials_quarterly.csv')
            balance_sheet.to_csv(company_dir / f'{ticker}_balance_sheet.csv')
            cashflow.to_csv(company_dir / f'{ticker}_cashflow.csv')
            
            print(f"✓ Successfully collected data for {ticker}")
            return True
            
        except Exception as e:
            print(f"✗ Error collecting {ticker}: {str(e)}")
            return False
    
    def collect_all_companies(self):
        """Collect data for all companies in config"""
        companies = self.config['data']['companies']
        
        print(f"\nCollecting data for {len(companies)} companies...")
        success_count = 0
        
        for ticker, sector in companies.items():
            if self.collect_company_data(ticker, sector):
                success_count += 1
        
        print(f"\n✓ Successfully collected data for {success_count}/{len(companies)} companies")
    
    def validate_collection(self):
        """Validate that data has been collected"""
        print("\n" + "="*50)
        print("DATA COLLECTION SUMMARY")
        print("="*50)
        
        # Check macroeconomic data
        macro_file = self.data_dir / 'fred_macroeconomic_data.csv'
        if macro_file.exists():
            df = pd.read_csv(macro_file, index_col=0)
            print(f"\n✓ Macroeconomic Data:")
            print(f"  - File: {macro_file}")
            print(f"  - Time periods: {len(df)}")
            print(f"  - Indicators: {df.shape[1]}")
            print(f"  - Date range: {df.index[0]} to {df.index[-1]}")
        else:
            print("\n✗ Macroeconomic data not found")
        
        # Check company data
        company_dir = self.data_dir / 'companies'
        if company_dir.exists():
            price_files = list(company_dir.glob('*_prices.csv'))
            print(f"\n✓ Company Data:")
            print(f"  - Companies collected: {len(price_files)}")
            
            for file in price_files:
                df = pd.read_csv(file, index_col=0)
                ticker = file.stem.replace('_prices', '')
                print(f"  - {ticker}: {len(df)} trading days ({df.index[0]} to {df.index[-1]})")
        else:
            print("\n✗ Company data directory not found")

def main():
    """Main function to run data collection"""
    print("="*50)
    print("FINANCIAL STRESS TEST - DATA COLLECTION")
    print("="*50)
    
    loader = DataLoader()
    
    # Collect macroeconomic data
    macro_df = loader.collect_macro_data()
    
    # Collect company data
    loader.collect_all_companies()
    
    # Validate collection
    loader.validate_collection()
    
    print("\n" + "="*50)
    print("DATA COLLECTION COMPLETE!")
    print("="*50)

if __name__ == "__main__":
    main()