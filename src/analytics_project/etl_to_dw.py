import pandas as pd
import sqlite3
import pathlib
import sys

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
   
    if df["sales_id"].isna().any or df["sale_id"].duplicated().any():
        df = df.reset_index(drop=True)
        df["sales_id"] = (df.index +1).astype("Int64")
        
    return df

def create_schema(cursor: sqlite3.Cursor) -> None:
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
    for table in ["customer", "product", "sales"]:
        print (f" the table being deleted is: {table}")
        cursor.execute(f"DELETE FROM {table}")

def insert_customers(df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    df.to_sql("customer", cursor.connection, if_exists="append", index=False)
    
def insert_products(df: pd.DataFrame, cursor:sqlite3.Cursor) -> None:
    df.to_sql("product",cursor.connection, if_exists= "append",index=False)
    
def insert_sales(df: pd.DataFrame, cursor:sqlite3.Cursor) -> None:
    df.to_sql("sales", cursor.connection,if_exists= "append",index=False)
    
def print_table_row_counts(cursor: sqlite3.Cursor, tables: list[str]) -> None:
    print("[VALIDATION] Table row counts:")
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
    delete_existing_records(cursor)

    customer_file_path = PREPARED_DATA_DIR / "customers_data_clean.csv"
    if not customer_file_path.exists():
        raise FileNotFoundError(f"Missing file: {customer_file_path}")
    print(f"[INFO] Loading file: {customer_file_path.name}")
    customers_df = pd.read_csv(customer_file_path)  #read the customer .csv file
    customers_df = norm_customers(customers_df) # calling the normalising function
    insert_customers(customers_df, cursor) # calling the inserting records function
    
    product_file_path = PREPARED_DATA_DIR/ "products_data_clean.csv"
    if not product_file_path.exists():
        raise FileNotFoundError(f"Missing file:{product_file_path}")
    print(f"[INFO] Loading file: {product_file_path.name}")
    products_df = pd.read_csv(product_file_path)  # read the product .csv file
    products_df = norm_products(products_df) # calling the normalising function
    insert_products(products_df,cursor) # calling the inserting record function
   
    sales_file_path = PREPARED_DATA_DIR/"sales_data_clean.csv"
    if not sales_file_path.exists():
        raise FileNotFoundError(f"Missing file:{sales_file_path}")
    print(f"[INFO] Loading file: {sales_file_path}")
    sales_df = pd.read_csv(sales_file_path) # read the sales.csv
    sales_df = norm_sales(sales_df) # calling the normalising function
    insert_sales(sales_df,cursor) # calling the inserting record function
    
    conn.commit()
    print_table_row_counts(cursor, ["customer", "product", "sales"])
    # print("[VALIDATION] Row counts:")
    # for r in cursor.execute("SELECT 'customer', COUNT(*) FROM customer"):
    #     print(r)

    conn.close()

# Entry Point
if __name__ == "__main__":
    load_data_to_db()
    