"""
Data Preprocessing Module
Handles data cleaning, transformation, and feature engineering
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import yaml
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataPreprocessor:
    """Class to handle data preprocessing operations"""
    
    def __init__(self, config_path=None):
        """Initialize preprocessor"""
        # Determine project root
        self.project_root = Path(__file__).resolve().parent.parent.parent.parent
        
        # Load environment variables
        env_path = self.project_root / '.env'
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
        
        # Load config
        if config_path is None:
            config_path = self.project_root / 'configs' / 'config.yaml'
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.raw_dir = self.project_root / 'data_pipeline' / 'data' / 'raw'
        self.processed_dir = self.project_root / 'data_pipeline' / 'data' / 'processed'
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("DataPreprocessor initialized")
    
    def handle_missing_values(self, df, method='forward_fill'):
        """Handle missing values in dataframe"""
        logger.info(f"Handling missing values using {method}")
        
        missing_before = df.isnull().sum().sum()
        logger.info(f"Missing values before: {missing_before}")
        
        if method == 'forward_fill':
            df = df.fillna(method='ffill')
            df = df.fillna(method='bfill')  # Handle leading NaNs
        elif method == 'interpolate':
            df = df.interpolate(method='linear', limit_direction='both')
        else:
            raise ValueError(f"Unknown method: {method}")
        
        missing_after = df.isnull().sum().sum()
        logger.info(f"Missing values after: {missing_after}")
        
        return df
    
    def detect_outliers(self, series, threshold=3):
        """Detect outliers using z-score method"""
        if len(series) < 2:
            return pd.Series([False] * len(series), index=series.index)
        
        mean = series.mean()
        std = series.std()
        
        if std == 0:
            return pd.Series([False] * len(series), index=series.index)
        
        z_scores = np.abs((series - mean) / std)
        return z_scores > threshold
    
    def handle_outliers(self, df, threshold=3):
        """Handle outliers in numerical columns"""
        logger.info(f"Detecting outliers with threshold={threshold}")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        outlier_counts = {}
        
        for col in numeric_cols:
            outliers = self.detect_outliers(df[col], threshold)
            outlier_count = outliers.sum()
            outlier_counts[col] = int(outlier_count)
            
            if outlier_count > 0:
                logger.info(f"Found {outlier_count} outliers in {col}")
                # Cap outliers at threshold * std from mean
                mean = df[col].mean()
                std = df[col].std()
                if std > 0:
                    df[col] = df[col].clip(
                        lower=mean - threshold * std,
                        upper=mean + threshold * std
                    )
        
        total_outliers = sum(outlier_counts.values())
        logger.info(f"Total outliers handled: {total_outliers}")
        
        return df, outlier_counts
    
    def create_lagged_features(self, df, lags=[1, 3, 6, 12]):
        """Create lagged features for time series"""
        logger.info(f"Creating lagged features: {lags}")
        
        lagged_df = df.copy()
        
        for col in df.columns:
            for lag in lags:
                lagged_df[f'{col}_lag_{lag}'] = df[col].shift(lag)
        
        logger.info(f"Created {len(df.columns) * len(lags)} lagged features")
        return lagged_df
    
    def create_rolling_features(self, df, windows=[3, 6, 12]):
        """Create rolling window features"""
        logger.info(f"Creating rolling features with windows: {windows}")
        
        rolling_df = df.copy()
        
        for col in df.columns:
            for window in windows:
                rolling_df[f'{col}_ma_{window}'] = df[col].rolling(window=window).mean()
                rolling_df[f'{col}_std_{window}'] = df[col].rolling(window=window).std()
        
        logger.info(f"Created {len(df.columns) * len(windows) * 2} rolling features")
        return rolling_df
    
    def preprocess_macro_data(self):
        """Preprocess macroeconomic data"""
        logger.info("Starting macroeconomic data preprocessing")
        
        # Load raw data
        input_path = self.raw_dir / 'fred_macroeconomic_data.csv'
        
        if not input_path.exists():
            raise FileNotFoundError(f"Raw data not found at {input_path}. Run data acquisition first.")
        
        df = pd.read_csv(input_path, index_col=0, parse_dates=True)
        logger.info(f"Loaded data shape: {df.shape}")
        
        # Handle missing values
        df = self.handle_missing_values(df, method=self.config['preprocessing']['handle_missing'])
        
        # Handle outliers
        df, outlier_counts = self.handle_outliers(
            df, 
            threshold=self.config['preprocessing']['outlier_threshold']
        )
        
        # Create lagged features
        df_lagged = self.create_lagged_features(df)
        
        # Create rolling features
        df_featured = self.create_rolling_features(df_lagged)
        
        # Drop rows with NaN (from lagging/rolling)
        initial_rows = len(df_featured)
        df_featured = df_featured.dropna()
        dropped_rows = initial_rows - len(df_featured)
        logger.info(f"Dropped {dropped_rows} rows due to NaN from feature engineering")
        
        # Save processed data
        output_path = self.processed_dir / 'macro_data_processed.csv'
        df_featured.to_csv(output_path)
        logger.info(f"✓ Saved processed macro data to {output_path}")
        logger.info(f"Final shape: {df_featured.shape}")
        
        # Save preprocessing metadata
        metadata = {
            'input_shape': list(df.shape),
            'output_shape': list(df_featured.shape),
            'outliers_detected': outlier_counts,
            'features_created': df_featured.shape[1] - df.shape[1]
        }
        
        return str(output_path), metadata
    
    def preprocess_company_data(self, ticker):
        """Preprocess individual company data"""
        logger.info(f"Preprocessing data for {ticker}")
        
        # Load price data
        input_path = self.raw_dir / 'companies' / f'{ticker}_prices.csv'
        
        if not input_path.exists():
            logger.warning(f"Data not found for {ticker} at {input_path}")
            return None
        
        df = pd.read_csv(input_path, index_col=0, parse_dates=True)
        
        # Calculate returns
        df['returns'] = df['Close'].pct_change()
        df['log_returns'] = np.log(df['Close'] / df['Close'].shift(1))
        
        # Calculate volatility
        df['volatility_30d'] = df['returns'].rolling(window=30).std()
        
        # Handle missing values
        df = self.handle_missing_values(df)
        
        # Save processed data
        output_dir = self.processed_dir / 'companies'
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / f'{ticker}_processed.csv'
        df.to_csv(output_path)
        
        logger.info(f"✓ Saved processed data for {ticker}")
        return str(output_path)
    
    def preprocess_all_companies(self):
        """Preprocess all company data"""
        companies = self.config['data']['companies']
        logger.info(f"Preprocessing {len(companies)} companies")
        
        processed_files = []
        
        for ticker in companies.keys():
            try:
                output_path = self.preprocess_company_data(ticker)
                if output_path:
                    processed_files.append(output_path)
            except Exception as e:
                logger.error(f"Error preprocessing {ticker}: {e}")
        
        logger.info(f"✓ Successfully preprocessed {len(processed_files)} companies")
        return processed_files


def preprocess_macro(**context):
    """Airflow task for macro data preprocessing"""
    preprocessor = DataPreprocessor()
    output_path, metadata = preprocessor.preprocess_macro_data()
    if context:
        context['task_instance'].xcom_push(key='macro_processed_path', value=output_path)
        context['task_instance'].xcom_push(key='preprocessing_metadata', value=metadata)
    return output_path


def preprocess_companies(**context):
    """Airflow task for company data preprocessing"""
    preprocessor = DataPreprocessor()
    processed_files = preprocessor.preprocess_all_companies()
    if context:
        context['task_instance'].xcom_push(key='company_processed_files', value=processed_files)
    return len(processed_files)


if __name__ == "__main__":
    # Test the module
    logger.info("="*60)
    logger.info("TESTING DATA PREPROCESSING MODULE")
    logger.info("="*60)
    
    try:
        preprocessor = DataPreprocessor()
        
        # Test macro data preprocessing
        logger.info("\n" + "="*60)
        logger.info("PREPROCESSING MACROECONOMIC DATA")
        logger.info("="*60)
        output_path, metadata = preprocessor.preprocess_macro_data()
        logger.info(f"Metadata: {metadata}")
        
        # Test company data preprocessing
        logger.info("\n" + "="*60)
        logger.info("PREPROCESSING COMPANY DATA")
        logger.info("="*60)
        processed_files = preprocessor.preprocess_all_companies()
        
        logger.info("\n" + "="*60)
        logger.info("✓ ALL PREPROCESSING COMPLETE!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n✗ Preprocessing failed: {e}")
        raise