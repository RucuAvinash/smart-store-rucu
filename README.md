📊 Project P2: BI Python – Reading Raw Data into pandas DataFrames
This module verifies the initial setup of Business Intelligence (BI) Python project by reading raw CSV files into pandas DataFrames and logging the process.
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



## WORKFLOW 1. Set Up Your Machine

Proper setup is critical.
Complete each step in the following guide and verify carefully.

- [SET UP MACHINE](./SET_UP_MACHINE.md)

---

## WORKFLOW 2. Set Up Your Project

After verifying your machine is set up, set up a new Python project by copying this template.
Complete each step in the following guide.

- [SET UP PROJECT](./SET_UP_PROJECT.md)

It includes the critical commands to set up your local environment (and activate it):

```shell
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```



## WORKFLOW 3.

Git Pull from GitHub
```shell
git pull
```

###  Run Checks as You Work


```shell
uv sync --extra dev --extra docs --upgrade
uv cache clean
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
git add .
uv run pytest
```


###  Execute

In VS Code terminal, run:

```shell
uv run python -m analytics_project.data_prep
```

You should see:

- Log messages in the terminal
- Data files loaded and read
- DataFrame sizes

If this works, your project is ready! If not, check:

- Are you in the right folder? (All terminal commands are to be run from the root project folder.)
- Did you run the full `uv sync --extra dev --extra docs --upgrade` command?
- Are there any error messages? (ask for help with the exact error)

---

###  Git add-commit-push to GitHub

Anytime we make working changes to code is a good time to git add-commit-push to GitHub.

1. Stage your changes with git add.
2. Commit your changes with a useful message in quotes.
3. Push your work to GitHub.

```shell
git add .
git commit -m "describe your change in quotes"
git push -u origin main
```

This will trigger the GitHub Actions workflow and publish your documentation via GitHub Pages.

### Project Commands

To run Python script

```shell
uv run python -m analytics_project.demo_module_basics
```

To run Jupyter Notebook

```shell
uv run jupyter lab
```

To run prepare_customers_data.py

```shell
python3 src/analytics_project/data_preparation/prepare_customers_data.py
```

To run prepare_products_data.py

```shell
python3 src/analytics_project/data_preparation/prepare_products_data.py
```

To run prepare_sales_data.py

```shell
python3 src/analytics_project/data_preparation/prepare_sales_data.py
```

To run data_scrubber.py

```shell
python3 src/analytics_project/data_scrubber.py
```
