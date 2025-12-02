"""
CSV Importer Service.
Handles reading and validating CSV files from Lodgify exports.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Any

# Configure logging
logger = logging.getLogger(__name__)


class CSVImporter:
    """
    Service for importing CSV files containing reservation data.
    
    Handles:
    - Finding the latest CSV file in a directory
    - Loading CSV data into structured format
    - Validating required columns
    """
    
    def __init__(self, directory_path: str):
        """
        Initialize CSV importer with directory path.
        
        Args:
            directory_path: Path to directory containing CSV files
        """
        self.directory = Path(directory_path)
        
        if not self.directory.exists():
            logger.warning(f"CSV directory does not exist: {self.directory}")
        else:
            logger.info(f"CSVImporter initialized with directory: {self.directory}")
    
    def get_latest_csv(self) -> Optional[Path]:
        """
        Scan directory for files ending in .csv and return the newest by modification time.
        
        Returns:
            Path to the latest CSV file, or None if no CSV files found
            
        Example:
            >>> importer = CSVImporter("/path/to/csvs")
            >>> latest = importer.get_latest_csv()
            >>> if latest:
            ...     print(f"Latest CSV: {latest}")
        """
        if not self.directory.exists():
            logger.warning(f"Directory does not exist: {self.directory}")
            return None
        
        # Find all CSV files
        csv_files = list(self.directory.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in directory: {self.directory}")
            return None
        
        # Sort by modification time (newest first)
        csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        
        latest = csv_files[0]
        logger.info(f"Found {len(csv_files)} CSV file(s). Latest: {latest.name} (modified: {latest.stat().st_mtime})")
        
        return latest
    
    def load_csv(self, path: Path) -> List[Dict[str, Any]]:
        """
        Load CSV file using pandas and convert to list of dictionaries.
        
        Ensures all keys exist even if empty, and handles missing values gracefully.
        
        Args:
            path: Path to CSV file
            
        Returns:
            List of dictionaries, one per row
            
        Raises:
            FileNotFoundError: If CSV file does not exist
            ValueError: If CSV cannot be parsed
            
        Example:
            >>> importer = CSVImporter("/path/to/csvs")
            >>> csv_path = importer.get_latest_csv()
            >>> rows = importer.load_csv(csv_path)
            >>> print(f"Loaded {len(rows)} rows")
        """
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")
        
        try:
            logger.info(f"Loading CSV file: {path}")
            
            # Read CSV with pandas
            df = pd.read_csv(path)
            
            logger.info(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
            logger.debug(f"Columns: {list(df.columns)}")
            
            # Convert DataFrame to list of dicts
            rows = df.to_dict(orient="records")
            
            # Ensure all keys exist even if empty (fill NaN with empty string)
            for row in rows:
                for key in df.columns:
                    if key not in row or pd.isna(row[key]):
                        row[key] = ""
                    else:
                        # Convert pandas types to native Python types
                        value = row[key]
                        if pd.isna(value):
                            row[key] = ""
                        elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                            row[key] = str(value)
                        else:
                            row[key] = value
            
            logger.info(f"Successfully loaded {len(rows)} rows from CSV")
            return rows
            
        except Exception as e:
            logger.error(f"Error loading CSV file {path}: {str(e)}", exc_info=True)
            raise ValueError(f"Failed to load CSV file: {str(e)}") from e
    
    def validate_columns(self, rows: List[Dict[str, Any]], required_cols: List[str]) -> None:
        """
        Validate that all required columns are present in the CSV data.
        
        Args:
            rows: List of row dictionaries from CSV
            required_cols: List of required column names
            
        Raises:
            ValueError: If any required column is missing
            
        Example:
            >>> importer = CSVImporter("/path/to/csvs")
            >>> rows = importer.load_csv(path)
            >>> required = ["Id", "Name", "DateArrival"]
            >>> importer.validate_columns(rows, required)
        """
        if not rows:
            raise ValueError("No rows to validate")
        
        # Get columns from first row
        available_cols = set(rows[0].keys())
        required_set = set(required_cols)
        
        missing_cols = required_set - available_cols
        
        if missing_cols:
            error_msg = (
                f"Missing required columns in CSV: {', '.join(sorted(missing_cols))}. "
                f"Available columns: {', '.join(sorted(available_cols))}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Column validation passed. All {len(required_cols)} required columns present")

