from pathlib import Path
import logging

from data_scrubber import DataScrubber

import sys
print ("sys.path:",sys.path)

import pandas as pd
#Configure logging
logging.basicConfig(level=logging.INFO,format="%(asctime)s-%(levelname)s -%(message)s")

# Defining Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT/ "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT/ "data" /"processed"
def prep_dataset(filename: str)-> None:
    """ Generic data prep function for any dataset."""
    raw_path = RAW_DIR/ f"{filename}.csv"
    output_path = PROCESSED_DIR/f"{filename}_clean.csv"
    logging.info(f"Processing {raw_path.name}..") 
    
    df = pd.read_csv(raw_path)
    scrubber = (
        DataScrubber.from_csv(raw_path)
        .standardize_column_names()
        .strip_whitespace()
        .drop_empty_rows()
        .drop_duplicates()
    )
    
    scrubber.to_csv(output_path,index=False)
    logging.info(f"Cleaned data saved to {output_path.name}")
    
def main() ->None:
    PROCESSED_DIR.mkdir(parents=True,exist_ok = True)
    
    for dataset in ["customers_data","products_data","sales_data"]:
        prep_dataset(dataset)
        
        logging.info(f"Data prep complete. Clean files written to: {PROCESSED_DIR}")
        
if __name__ == "__main__":
 main()
        