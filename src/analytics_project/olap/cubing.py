import pathlib
import sqlite3
import pandas as pd
from utils_logger import logger

# Global constants for paths and key directories
THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
DW_DIR: pathlib.Path = THIS_DIR
print(f"DW_DIR is {DW_DIR}")

PACKAGE_DIR: pathlib.Path = DW_DIR.parent
print(f"PACKAGE_DIR is {PACKAGE_DIR}")

SRC_DIR: pathlib.Path = PACKAGE_DIR.parent
print(f"SRC_DIR is {SRC_DIR}")

PROJECT_ROOT_DIR: pathlib.Path = SRC_DIR.parent
print(f"ROOT_DIR is {PROJECT_ROOT_DIR}")

# Data directories
DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
print(f"DATA_DIR is {DATA_DIR}")

WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "warehouse"
print(f"WAREHOUSE DIR IS: {WAREHOUSE_DIR}")

# Database location
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "smart_sales.db"
conn = sqlite3.connect(DB_PATH)
print(f"DB_PATH is {DB_PATH}")

# OLAP output directory
OLAP_OUTPUT_DIR: pathlib.Path = WAREHOUSE_DIR / "olap_cubing_outputs"
print(f"OLAP_OUTPUT_DIR is {OLAP_OUTPUT_DIR}")

# Make OLAP directory if it does not exist
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sales_df = pd.read_sql_query("SELECT * FROM sales LIMIT 5", conn)
print(sales_df.columns)
print(sales_df.head())
conn.close()


def ingest_sales_data_from_dw() -> pd.DataFrame:
    """Ingest sales data from SQLite data warehouse."""
    try:
        conn = sqlite3.connect(DB_PATH)
        sales_df = pd.read_sql_query("SELECT * FROM sales", conn)
        conn.close()
        logger.info("Sales data successfully loaded from SQLite data warehouse")
        return sales_df
    except Exception as e:
        logger.error(f"Error loading data from data warehouse: {e}")
        raise


def create_olap_cube(sales_df: pd.DataFrame, dimensions: list, metrics: dict) -> pd.DataFrame:
    """Create an OLAP cube by aggregating data.

    Args:
        sales_df (pd.DataFrame): The sales data.
        dimensions (list): List of column names to group by.
        metrics (dict): Dictionary of aggregation functions for metrics.

    Returns:
        pd.DataFrame: The multidimensional OLAP cube.
    """
    try:
        # Group by specified dimensions
        grouped = sales_df.groupby(dimensions)

        # Perform the aggregations
        cube = grouped.agg(metrics).reset_index()

        # Add a list of sale IDs for traceability
        cube["sale_ids"] = grouped["sales_id"].apply(list).reset_index(drop=True)

        # Generate explicit column names
        explicit_columns = generate_column_names(dimensions, metrics)
        explicit_columns.append("sale_ids")
        cube.columns = explicit_columns

        logger.info(f"OLAP cube created with dimensions: {dimensions}")
        return cube
    except Exception as e:
        logger.error(f"Error creating OLAP cube: {e}")
        raise
    
def generate_column_names(dimensions: list, metrics: dict) -> list:
    """Generate explicit column names for OLAP cube, ensuring no trailing underscores.

    Args:
        dimensions (list): List of dimension columns.
        metrics (dict): Dictionary of metrics with aggregation functions.

    Returns:
        list: Explicit column names.
    """
    # Start with dimensions
    column_names = dimensions.copy()

    # Add metrics with their aggregation suffixes
    for column, agg_funcs in metrics.items():
        if isinstance(agg_funcs, list):
            for func in agg_funcs:
                column_names.append(f"{column}_{func}")
        else:
            column_names.append(f"{column}_{agg_funcs}")

    # Remove trailing underscores from all column names
    column_names = [col.rstrip("_") for col in column_names]
    logger.info(f"Generated column names for OLAP cube: {column_names}")

    return column_names

def write_cube_to_csv(cube: pd.DataFrame, filename:str) -> None:
    """ Write the OLAP cube to a CSV file"""
    try:
        output_path = OLAP_OUTPUT_DIR.joinpath(filename)
        cube.to_csv(output_path, index =False)
        logger.info(f"OLAP cube saved to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving OLAP cube to CSV file:{e}")
        raise
def main():
    logger.info("Starting OLAPCubing process..")
    
    # Step 1 : Ingest Sales data
    sales_df = ingest_sales_data_from_dw()
    if sales_df.empty:
        logger.warning(
            "WARNING: The sales table is empty."
            "THe OLAP column will contain only column headers"
            "Fix: Prepare raw data and run ETL step to load the data warehouse."
        )
        
    # Step 2a: Convert to datetime safely
    sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"], errors="coerce")

# Step 2b: Drop rows where conversion failed (i.e., sales_date is NaT)
    sales_df = sales_df.dropna(subset=["sale_date"])
    
    bad_date_count = sales_df["sale_date"].isna().sum()
    logger.warning(f"Removed {bad_date_count} rows with invalid sales_date values.")


        
    # Step 2d: Add additional columns for time-based dimensions
    sales_df["sales_date" ]= pd.to_datetime(sales_df["sale_date"])
    sales_df["DayofWeek"] = sales_df["sale_date"].dt.day_name()
    sales_df["Month"] = sales_df["sale_date"].dt.month
    sales_df["Year"] = sales_df["sale_date"].dt.year
    
    #Step 3: Define dimensions and metrics for the cube
    dimensions= ["DayofWeek","product_id","customer_id"]
    metrics = {"sale_amount": ["sum","mean"],"sales_id":"count"}
    
    # Step 4: Create the cube
    olap_cube = create_olap_cube(sales_df,dimensions,metrics)
    
    # step 5: Save the cube to CSV file
    write_cube_to_csv(olap_cube,"multidimensional_olap_cube.csv")
    
    logger.info("OLAP Cubing process completed successfully")
    
    logger.info(f"Please see outputs in {OLAP_OUTPUT_DIR}")
    
if __name__ == "__main__":
    main()