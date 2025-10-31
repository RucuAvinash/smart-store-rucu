📊 Project P2: BI Python – Reading Raw Data into pandas DataFrames
This module verifies the initial setup of your Business Intelligence (BI) Python project by reading raw CSV files into pandas DataFrames and logging the process. It ensures that your data pipeline is correctly configured before deeper analysis begins.
📁 Directory Structure
analytics_project/
├── data/
│   └── raw/
│       ├── customers_data.csv
│       ├── products_data.csv
│       └── sales_data.csv
├── src/
│   └── analytics_project/
│       └── data_prep.py
├── utils_logger.py
└── README.md


🧠 What This Script Does
- Initializes a project-wide logger via utils_logger.py
- Defines reusable paths to the raw data directory from utils_logger.
- Reads three CSV files:
- customers_data.csv
- products_data.csv
- sales_data.csv
- Logs:
- Start and success of each file read
- File name and DataFrame preview
- Shape of each DataFrame
- Errors if files are missing or unreadable
🚀 How to Run
Ensure your working directory is set to the project root and the data/raw/ folder contains the required CSV files.
cd C:\Repos\smart-store-rucu\smart-store-rucu\src\analytics_project
py data_prep.py


🛠️ Requirements
- Python 3.8+
- pandas
- pathlib (standard library)
Install dependencies:
pip install pandas


🧾 Logging
Logging is handled via utils_logger.py, which provides consistent formatting and centralized control across modules.
📌 Notes
- If any CSV file is missing or unreadable, the script logs the error and continues without crashing.
- This module is designed for setup verification and can be extended for further data cleaning or transformation.
👤 Author
- Your Rucmanidevi Sethu
- Created for Project P2: BI Python – Reading Raw Data into pandas DataFrames
