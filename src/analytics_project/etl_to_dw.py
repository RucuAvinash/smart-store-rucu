import pandas as pd
import sqlite3
import pathlib
import sys
from datetime import datetime, timedelta

# Project Root Setup
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
print(f'Project root {PROJECT_ROOT}')

#PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = PROJECT_ROOT / "data" / "dw"
DB_PATH = DW_DIR / "smart_sales.db"
PREPARED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

# Utility Functions
def drop_dupes(df: pd.DataFrame, key: str) -> pd.DataFrame:
    return df.drop_duplicates(subset=[key], keep="first").copy()

# Normalising fields in the customer table
def norm_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "customerid": "customer_id",
            "name": "name",
            "region": "region",
            "joindate": "join_date",
        }
    )
    
    # Cleaning customer data
    df=df[["customer_id","name","region","join_date"]].copy()  # Select only relevant fields and work on a copy
    df["customer_id"] = pd.to_numeric(df["customer_id"],errors="coerce").astype("Int64") #convert field to integer
    df=df.dropna(subset=["customer_id"]) #drop rows with na values
    df=drop_dupes(df,"customer_id") # drop duplicate rows
    return df

    # Normalising fields in products table
def norm_products(df:pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "productid" : "product_id",
            "productname": "product_name",
            "category" : "category",
        }
        
    )
    # Cleaning products data
    df = df[["product_id","product_name","category"]].copy()
    df["product_id"] = pd.to_numeric(df["product_id"],errors="coerce").astype("Int64")
    df = df.dropna(subset=["product_id"])
    df = drop_dupes(df, "product_id")
    return df
   
   # Normalising fields in the sales table
def norm_sales(df:pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "transactionid" : "sales_id",
            "customerid"    : "customer_id",
            "productid" : "product_id",
            "saledate" : "sale_date",
            "saleamount" : "sale_amount",
            
        }
    )
    
    # Cleaning sales data
    columns = ["sales_id","customer_id","product_id","sale_amount","sale_date"]
    for c in columns:
        df = df[columns].copy()
    for c in ["sales_id","customer_id","product_id"]:
        df[c] = pd.to_numeric(df[c], errors= "coerce").astype("Int64")
    df["sale_amount"] = pd.to_numeric(df["sale_amount"], errors="coerce").astype(float)
    df = df.dropna(subset=["customer_id","product_id","sale_amount"])
   
    if df["sales_id"].isna().any or df["sales_id"].duplicated().any():
        df = df.reset_index(drop=True)
        df["sales_id"] = (df.index +1).astype("Int64")
        
    return df

def generate_date_dimension(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Generate date dimension table with various date attributes.

    Args:
    start_date(str): in 'YYYY-MM-DD' format
    end_date(str) : in 'YYYY-MM-DD' format
     
    Returns:
    pd.DataFrame: Date dimension dataframe
"""
     
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end   = datetime.strptime(end_date,'%Y-%m-%d')

# Generate date range
    date_range = pd.date_range(start=start, end=end, freq='D')
    
# Create dimension dataframe with only requested fields
    dim_date = pd.DataFrame({
        'date_id': date_range.strftime('%Y%m%d').astype(int), #20241127
        'full_date' : date_range.strftime('%m/%d/%Y'),
        'year' : date_range.year,
        'month' : date_range.month,
        'month_name': date_range.strftime('%B'),
        'day' : date_range.day,
        'week': date_range.isocalendar().week
        
    })
    
    return dim_date

def create_schema(cursor:sqlite3.Cursor) -> None:
    
    cursor.execute("DROP TABLE IF EXISTS sales")
    cursor.execute("DROP TABLE IF EXISTS customer")
    cursor.execute("DROP TABLE IF EXISTS product")
    cursor.execute("DROP TABLE IF EXISTS dim_date")
    # Create dim_date table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_date(
        date_id INTEGER PRIMARY KEY,
        full_date TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        month_name TEXT NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL
    )
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer (
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        region TEXT,
        join_date TEXT
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        category TEXT
    )
    """)
    
    
    cursor.execute("""
      CREATE TABLE IF NOT EXISTS sales (
        sales_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        sale_amount REAL NOT NULL,
        sale_date TEXT,
        FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
        FOREIGN KEY (product_id) REFERENCES product (product_id)
        
    )
    """)
# Delete the existing records from table
def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    for table in ["sales","customer", "product", "dim_date"]: # delete sales first due to foreign key
        print (f" the table being deleted is: {table}")
        cursor.execute(f"DELETE FROM {table}")
        
def insert_dim_date(df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert date dimension data into dim_date table."""
    df.to_sql("dim_date", cursor.connection, if_exists = "append", index =False)
    print(f"[INFO] Inserted {len(df)} records into dim_data table")
    
def insert_customers(df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    df.to_sql("customer", cursor.connection, if_exists="append", index=False)
    
def insert_products(df: pd.DataFrame, cursor:sqlite3.Cursor) -> None:
    df.to_sql("product",cursor.connection, if_exists= "append",index=False)
    
def insert_sales(df: pd.DataFrame, cursor:sqlite3.Cursor) -> None:
    """ Insert sales data and populate date_id foreign key."""
    
    initial_count = len(df)
    print(f"[INFO] Processing {initial_count} sales records")
   
   # Get valid customer_ids from customer table
    cursor.execute("SELECT customer_id FROM customer")
    valid_customer_ids = set(row[0] for row in cursor.fetchall())
 
    # Get valid product_ids from product table
    cursor.execute("SELECT product_id from product")
    valid_product_ids = set(row[0] for row in cursor.fetchall())
   
    # Filter sales to only include valid foreign keys
    df = df[df['customer_id'].isin(valid_customer_ids)].copy()
    after_customer_filter = len(df)
   ## print(f"[INFO] After customer filter: {after_customer_filter} records")
    
    # Insert valid sales
    if len(df) >0:
        df.to_sql("sales",cursor.connection, if_exists = "append",index=False)
        print(f"[INFO] Successfully inserted {len(df)} records to sales table")
    else:
        print("[WARNING] No valid sales records to insert")
def print_table_row_counts(cursor: sqlite3.Cursor, tables: list[str]) -> None:
    print("Table row counts:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table} : {count}")

# Main ETL Function
def load_data_to_db() -> None:
    DW_DIR.mkdir(parents=True, exist_ok=True)
    
      
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    create_schema(cursor)
   
    
    # Generate and load date dimension(covering 2024 through 2025)
    print("[INFO] Generating date dimension ...")
    dim_date_df = generate_date_dimension('2024-01-01', '2025-11-01')
    insert_dim_date(dim_date_df, cursor)

    # Load customers
    customer_file_path = PREPARED_DATA_DIR / "customers_data_clean.csv"
    if not customer_file_path.exists():
        raise FileNotFoundError(f"Missing file: {customer_file_path}")
    print(f"[INFO] Loading file: {customer_file_path.name}")
    customers_df = pd.read_csv(customer_file_path)  #read the customer .csv file
    customers_df = norm_customers(customers_df) # calling the normalising function
    insert_customers(customers_df, cursor) # calling the inserting records function
    
    # Load products
    product_file_path = PREPARED_DATA_DIR/ "products_data_clean.csv"
    if not product_file_path.exists():
        raise FileNotFoundError(f"Missing file:{product_file_path}")
    print(f"[INFO] Loading file: {product_file_path.name}")
    products_df = pd.read_csv(product_file_path)  # read the product .csv file
    products_df = norm_products(products_df) # calling the normalising function
    insert_products(products_df,cursor) # calling the inserting record function
   
   # Load sales
    sales_file_path = PREPARED_DATA_DIR/"sales_data_clean.csv"
    if not sales_file_path.exists():
        raise FileNotFoundError(f"Missing file:{sales_file_path}")
    print(f"[INFO] Loading file: {sales_file_path}")
    sales_df = pd.read_csv(sales_file_path) # read the sales.csv
    sales_df = norm_sales(sales_df) # calling the normalising function
    insert_sales(sales_df,cursor) # calling the inserting record function
    
    conn.commit()
    print_table_row_counts(cursor, ["dim_date","customer", "product", "sales"])

    conn.close()
    print("\n[success] ETL Process completed successfully!")

# Entry Point
if __name__ == "__main__":
 load_data_to_db()
    