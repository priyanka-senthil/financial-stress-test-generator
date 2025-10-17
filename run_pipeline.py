"""
Financial Stress Test Pipeline - Direct Execution
Run the complete pipeline without Airflow
"""
import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from data_pipeline.scripts.data_acquisition import DataAcquisition
from data_pipeline.scripts.preprocessing.data_preprocessor import DataPreprocessor
from data_pipeline.scripts.validation.data_validator import DataValidator

def main():
    print("="*70)
    print("FINANCIAL STRESS TEST PIPELINE")
    print("="*70)
    
    # Step 1: Data Acquisition
    print("\n[1/3] Data Acquisition...")
    acquirer = DataAcquisition()
    acquirer.fetch_macro_data()
    acquirer.fetch_all_companies()
    print("✓ Data acquisition complete")
    
    # Step 2: Preprocessing
    print("\n[2/3] Data Preprocessing...")
    preprocessor = DataPreprocessor()
    preprocessor.preprocess_macro_data()
    preprocessor.preprocess_all_companies()
    print("✓ Preprocessing complete")
    
    # Step 3: Validation
    print("\n[3/3] Data Validation...")
    validator = DataValidator()
    report = validator.validate_macro_data()
    print(f"✓ Validation complete: {'PASS' if report['is_valid'] else 'WARNINGS'}")
    
    print("\n" + "="*70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("="*70)

if __name__ == "__main__":
    main()