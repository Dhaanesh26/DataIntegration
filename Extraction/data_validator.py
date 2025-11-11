import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataValidator:
    """
    Data validation module for ETL pipeline quality checks.
    Validates extracted data before loading to ensure data integrity.
    """
    
    def __init__(self):
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'checks_passed': [],
            'checks_failed': [],
            'warnings': []
        }
    
    def validate_csv_file(self, filepath: str, expected_columns: Optional[List[str]] = None) -> bool:
        """
        Validate CSV file structure and content.
        
        Args:
            filepath: Path to CSV file
            expected_columns: List of expected column names
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            df = pd.read_csv(filepath, delimiter='|')
            logger.info(f"Loaded file: {filepath} with {len(df)} rows")
            
            # Check if file is empty
            if df.empty:
                self.validation_results['checks_failed'].append(
                    f"File {filepath} is empty"
                )
                return False
            
            self.validation_results['checks_passed'].append(
                f"File {filepath} contains {len(df)} rows"
            )
            
            # Validate column structure
            if expected_columns:
                if list(df.columns) != expected_columns:
                    self.validation_results['checks_failed'].append(
                        f"Column mismatch in {filepath}. Expected: {expected_columns}, Got: {list(df.columns)}"
                    )
                    return False
                
                self.validation_results['checks_passed'].append(
                    f"Column structure validated for {filepath}"
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating file {filepath}: {str(e)}")
            self.validation_results['checks_failed'].append(
                f"Error reading {filepath}: {str(e)}"
            )
            return False
    
    def check_null_values(self, filepath: str, critical_columns: List[str]) -> bool:
        """
        Check for null values in critical columns.
        
        Args:
            filepath: Path to CSV file
            critical_columns: List of columns that should not contain nulls
            
        Returns:
            True if no nulls found in critical columns, False otherwise
        """
        try:
            df = pd.read_csv(filepath, delimiter='|')
            
            for col in critical_columns:
                if col not in df.columns:
                    self.validation_results['warnings'].append(
                        f"Column {col} not found in {filepath}"
                    )
                    continue
                
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    self.validation_results['checks_failed'].append(
                        f"{null_count} null values found in critical column '{col}' in {filepath}"
                    )
                    return False
            
            self.validation_results['checks_passed'].append(
                f"No null values in critical columns for {filepath}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error checking nulls in {filepath}: {str(e)}")
            self.validation_results['checks_failed'].append(
                f"Error checking nulls: {str(e)}"
            )
            return False
    
    def check_data_types(self, filepath: str, type_schema: Dict[str, str]) -> bool:
        """
        Validate data types of columns.
        
        Args:
            filepath: Path to CSV file
            type_schema: Dictionary mapping column names to expected types
            
        Returns:
            True if data types match schema, False otherwise
        """
        try:
            df = pd.read_csv(filepath, delimiter='|')
            
            for col, expected_type in type_schema.items():
                if col not in df.columns:
                    self.validation_results['warnings'].append(
                        f"Column {col} not found in {filepath}"
                    )
                    continue
                
                # Attempt to convert to expected type
                try:
                    if expected_type == 'int':
                        pd.to_numeric(df[col], errors='raise')
                    elif expected_type == 'float':
                        pd.to_numeric(df[col], errors='raise', downcast='float')
                    elif expected_type == 'datetime':
                        pd.to_datetime(df[col], errors='raise')
                except Exception as conv_error:
                    self.validation_results['checks_failed'].append(
                        f"Column {col} in {filepath} cannot be converted to {expected_type}"
                    )
                    return False
            
            self.validation_results['checks_passed'].append(
                f"Data types validated for {filepath}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error checking data types in {filepath}: {str(e)}")
            self.validation_results['checks_failed'].append(
                f"Error checking data types: {str(e)}"
            )
            return False
    
    def check_row_count_threshold(self, filepath: str, min_rows: int = 1, max_rows: Optional[int] = None) -> bool:
        """
        Check if row count is within expected threshold.
        
        Args:
            filepath: Path to CSV file
            min_rows: Minimum expected rows
            max_rows: Maximum expected rows (optional)
            
        Returns:
            True if row count is within threshold, False otherwise
        """
        try:
            df = pd.read_csv(filepath, delimiter='|')
            row_count = len(df)
            
            if row_count < min_rows:
                self.validation_results['checks_failed'].append(
                    f"{filepath} has {row_count} rows, below minimum threshold of {min_rows}"
                )
                return False
            
            if max_rows and row_count > max_rows:
                self.validation_results['checks_failed'].append(
                    f"{filepath} has {row_count} rows, above maximum threshold of {max_rows}"
                )
                return False
            
            self.validation_results['checks_passed'].append(
                f"Row count {row_count} is within threshold for {filepath}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error checking row count in {filepath}: {str(e)}")
            self.validation_results['checks_failed'].append(
                f"Error checking row count: {str(e)}"
            )
            return False
    
    def get_validation_report(self) -> Dict:
        """
        Get comprehensive validation report.
        
        Returns:
            Dictionary containing validation results
        """
        self.validation_results['total_checks'] = (
            len(self.validation_results['checks_passed']) + 
            len(self.validation_results['checks_failed'])
        )
        self.validation_results['success_rate'] = (
            len(self.validation_results['checks_passed']) / 
            self.validation_results['total_checks'] * 100 
            if self.validation_results['total_checks'] > 0 else 0
        )
        
        return self.validation_results
    
    def print_report(self):
        """Print validation report to console."""
        report = self.get_validation_report()
        
        print("\n" + "="*60)
        print("DATA VALIDATION REPORT")
        print("="*60)
        print(f"Timestamp: {report['timestamp']}")
        print(f"Total Checks: {report['total_checks']}")
        print(f"Success Rate: {report['success_rate']:.2f}%")
        print("\n✓ PASSED CHECKS:")
        for check in report['checks_passed']:
            print(f"  • {check}")
        
        if report['checks_failed']:
            print("\n✗ FAILED CHECKS:")
            for check in report['checks_failed']:
                print(f"  • {check}")
        
        if report['warnings']:
            print("\n⚠ WARNINGS:")
            for warning in report['warnings']:
                print(f"  • {warning}")
        
        print("="*60 + "\n")


# Example usage
if __name__ == "__main__":
    validator = DataValidator()
    
    # Example validation workflow
    filepath = "order_extract.csv"
    
    # Validate file structure
    validator.validate_csv_file(filepath)
    
    # Check for nulls in critical columns
    validator.check_null_values(filepath, critical_columns=['order_id', 'customer_id'])
    
    # Validate row count
    validator.check_row_count_threshold(filepath, min_rows=1)
    
    # Print comprehensive report
    validator.print_report()
