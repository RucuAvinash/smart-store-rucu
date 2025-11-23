import pathlib
import sqlite3
import pandas as pd
from utils_logger import logger


# Paths (same as before)
THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
DW_DIR: pathlib.Path = THIS_DIR
PACKAGE_DIR: pathlib.Path = DW_DIR.parent
SRC_DIR: pathlib.Path = PACKAGE_DIR.parent
PROJECT_ROOT_DIR: pathlib.Path = SRC_DIR.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "warehouse"
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "smart_sales.db"
OLAP_OUTPUT_DIR: pathlib.Path = WAREHOUSE_DIR / "olap_cubing_outputs"
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def write_cube_to_csv(cube: pd.DataFrame, filename:str) -> None:
    """ Write the OLAP cube to a CSV file"""
    try:
        output_path = OLAP_OUTPUT_DIR.joinpath(filename)
        cube.to_csv(output_path, index =False)
        logger.info(f"OLAP cube saved to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving OLAP cube to CSV file:{e}")
        raise


def goal_customer_lifetime_value(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Calculate Customer Lifetime Value (CLV) with customer names.

    Parameters
    ----------
    conn : sqlite3.Connection
        Connection to the smart_sales.db database

    Returns
    -------
    pd.DataFrame
        Aggregated CLV per customer with names, sorted descending
    """
    # SQL join between sales and customer tables
    query = """
        SELECT c.customer_id,
               c.name,
               SUM(s.sale_amount) AS CustomerLifetimeValue
        FROM sales s
        JOIN customer c
          ON s.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name
        ORDER BY CustomerLifetimeValue DESC
    """
    result = pd.read_sql_query(query, conn)
    return result


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)

    # Run CLV with customer names
    olap_cube = goal_customer_lifetime_value(conn)

    # Save to CSV
    output_file = OLAP_OUTPUT_DIR / "HV_customer_olap_cube.csv"
    write_cube_to_csv(olap_cube, output_file)

    logger.info("OLAP Cubing process completed successfully")
    logger.info(f"Please see outputs in {OLAP_OUTPUT_DIR}")

    conn.close()