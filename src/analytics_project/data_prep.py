from pathlib import Path
import pandas as pd
import sys
#from analytics_project.utils.logger import logger
from utils_logger import logger
from data_scrubber import DataScrubber
print("sys.path:", sys.path)

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

def prep_dataset(filename: str, required_columns: list = None) -> None:
    """
    Generic data prep function for any dataset.
    
    Parameters:
        filename (str): Name of the dataset (without extension).
    """
    raw_path = RAW_DIR / f"{filename}.csv"
    output_path = PROCESSED_DIR / f"{filename}_clean.csv"

    logger.info(f"Processing {raw_path.name}...")

    try:
        scrubber = (
            DataScrubber.from_csv(raw_path)
            .standardize_column_names()
            .strip_whitespace()
            .drop_empty_rows()
            .drop_duplicates()
        )
        if required_columns:
            initial_count = len(scrubber.df)
            for col in required_columns:
                # Replace empty strings with NA
                scrubber.df[col] = scrubber.df[col].replace('', pd.NA)
                scrubber.df[col] = scrubber.df[col].replace(r'^\s*$', pd.NA, regex=True)
                # Drop rows where this column is null
                scrubber.df = scrubber.df[scrubber.df[col].notna()]
            removed = initial_count - len(scrubber.df)
            logger.info(f"Removed {removed} rows with empty required columns: {required_columns}")
        
        scrubber = scrubber.drop_empty_rows().drop_duplicates()
        scrubber.to_csv(output_path, index=False)
        logger.info(f"Cleaned data saved to {output_path.name}")
    except Exception as e:
        logger.error(f"Failed to process {filename}: {e}", exc_info=True)

def main() -> None:
    """
    Main function to prepare all datasets.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Define required columns for each dataset
    datasets = [
        ("customers_data", ["customerid"]),
        ("products_data", ["productid"]),
        ("sales_data", ["transactionid"])
    ]

    for dataset, required_cols in datasets:
        prep_dataset(dataset, required_cols)

    logger.info(f"Data prep complete. Clean files written to: {PROCESSED_DIR}")

if __name__ == "__main__":
    main()
