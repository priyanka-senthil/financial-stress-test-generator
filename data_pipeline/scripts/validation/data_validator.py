"""
Data Validation Module
Validates data quality, schema, and detects anomalies
"""

import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from datetime import datetime
import yaml
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataValidator:
    """Class to validate data quality and detect anomalies"""
    
    def __init__(self, config_path=None):
        """Initialize validator"""
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
        
        self.processed_dir = self.project_root / 'data_pipeline' / 'data' / 'processed'
        self.statistics_dir = self.project_root / 'data_pipeline' / 'data' / 'statistics'
        self.schema_dir = self.project_root / 'data_pipeline' / 'data' / 'schemas'
        
        self.statistics_dir.mkdir(parents=True, exist_ok=True)
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("DataValidator initialized")
    
    def generate_schema(self, df, name):
        """Generate and save data schema"""
        logger.info(f"Generating schema for {name}")
        
        schema = {
            'name': name,
            'shape': list(df.shape),
            'columns': {},
            'generated_at': datetime.now().isoformat()
        }
        
        for col in df.columns:
            col_info = {
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percentage': float(df[col].isnull().sum() / len(df) * 100)
            }
            
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info.update({
                    'min': float(df[col].min()) if not pd.isna(df[col].min()) else None,
                    'max': float(df[col].max()) if not pd.isna(df[col].max()) else None,
                    'mean': float(df[col].mean()) if not pd.isna(df[col].mean()) else None,
                    'std': float(df[col].std()) if not pd.isna(df[col].std()) else None
                })
            
            schema['columns'][col] = col_info
        
        # Save schema
        schema_path = self.schema_dir / f'{name}_schema.json'
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)
        
        logger.info(f"✓ Schema saved to {schema_path}")
        return schema
    
    def generate_statistics(self, df, name):
        """Generate comprehensive statistics"""
        logger.info(f"Generating statistics for {name}")
        
        stats = {
            'basic_stats': df.describe().to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.astype(str).to_dict(),
            'shape': list(df.shape),
            'memory_usage': float(df.memory_usage(deep=True).sum() / 1024**2),  # MB
            'generated_at': datetime.now().isoformat()
        }
        
        # Correlation matrix for numeric columns
        numeric_df = df.select_dtypes(include=[np.number])
        if not numeric_df.empty and len(numeric_df.columns) > 1:
            stats['correlations'] = numeric_df.corr().to_dict()
        
        # Save statistics
        stats_path = self.statistics_dir / f'{name}_statistics.json'
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        logger.info(f"✓ Statistics saved to {stats_path}")
        return stats
    
    def detect_anomalies(self, df, name):
        """Detect data anomalies"""
        logger.info(f"Detecting anomalies in {name}")
        
        anomalies = {
            'name': name,
            'detected_at': datetime.now().isoformat(),
            'issues': []
        }
        
        # Check for missing values
        missing = df.isnull().sum()
        if missing.any():
            for col, count in missing[missing > 0].items():
                anomalies['issues'].append({
                    'type': 'missing_values',
                    'column': col,
                    'count': int(count),
                    'percentage': float(count / len(df) * 100)
                })
        
        # Check for infinite values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            inf_count = np.isinf(df[col]).sum()
            if inf_count > 0:
                anomalies['issues'].append({
                    'type': 'infinite_values',
                    'column': col,
                    'count': int(inf_count)
                })
        
        # Check for duplicate rows
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            anomalies['issues'].append({
                'type': 'duplicate_rows',
                'count': int(duplicates)
            })
        
        # Check for constant columns
        for col in df.columns:
            if df[col].nunique() == 1:
                anomalies['issues'].append({
                    'type': 'constant_column',
                    'column': col,
                    'value': str(df[col].iloc[0])
                })
        
        # Statistical anomalies (outliers)
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            outliers = ((df[col] < (Q1 - 3 * IQR)) | (df[col] > (Q3 + 3 * IQR))).sum()
            
            if outliers > 0:
                anomalies['issues'].append({
                    'type': 'statistical_outliers',
                    'column': col,
                    'count': int(outliers),
                    'percentage': float(outliers / len(df) * 100)
                })
        
        anomalies['total_issues'] = len(anomalies['issues'])
        logger.info(f"Detected {anomalies['total_issues']} anomaly types")
        
        return anomalies
    
    def validate_macro_data(self):
        """Validate processed macro data"""
        logger.info("Validating macro data")
        
        # Load data
        data_path = self.processed_dir / 'macro_data_processed.csv'
        
        if not data_path.exists():
            raise FileNotFoundError(f"Processed data not found at {data_path}. Run preprocessing first.")
        
        df = pd.read_csv(data_path, index_col=0, parse_dates=True)
        
        # Generate schema and statistics
        schema = self.generate_schema(df, 'macro_data')
        stats = self.generate_statistics(df, 'macro_data')
        
        # Detect anomalies
        anomalies = self.detect_anomalies(df, 'macro_data')
        
        validation_report = {
            'dataset': 'macro_data',
            'validated_at': datetime.now().isoformat(),
            'schema': schema,
            'statistics_summary': {
                'shape': stats['shape'],
                'memory_usage_mb': stats['memory_usage'],
                'missing_values': sum(stats['missing_values'].values())
            },
            'anomalies': anomalies,
            'is_valid': anomalies['total_issues'] == 0
        }
        
        # Save validation report
        report_path = self.statistics_dir / 'macro_data_validation_report.json'
        with open(report_path, 'w') as f:
            json.dump(validation_report, f, indent=2, default=str)
        
        logger.info(f"✓ Validation report saved to {report_path}")
        logger.info(f"Validation result: {'PASS' if validation_report['is_valid'] else 'FAIL'}")
        
        if not validation_report['is_valid']:
            logger.warning(f"Found {anomalies['total_issues']} data quality issues")
        
        return validation_report


def validate_data(**context):
    """Airflow task for data validation"""
    validator = DataValidator()
    validation_report = validator.validate_macro_data()
    
    if context:
        context['task_instance'].xcom_push(key='validation_report', value=validation_report)
        context['task_instance'].xcom_push(key='is_valid', value=validation_report['is_valid'])
    
    # Don't raise exception for minor issues, just log them
    if not validation_report['is_valid']:
        logger.warning(f"Data validation completed with {validation_report['anomalies']['total_issues']} issues")
    
    return validation_report['is_valid']


if __name__ == "__main__":
    # Test the module
    logger.info("="*60)
    logger.info("TESTING DATA VALIDATION MODULE")
    logger.info("="*60)
    
    try:
        validator = DataValidator()
        validation_report = validator.validate_macro_data()
        
        logger.info("\n" + "="*60)
        logger.info("✓ DATA VALIDATION COMPLETE!")
        logger.info(f"Status: {'PASS' if validation_report['is_valid'] else 'FAIL WITH WARNINGS'}")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n✗ Validation failed: {e}")
        raise