"""
utils/data_scrubber.py

Reusable utility class for performing common data cleaning and 
preparation tasks on a pandas DataFrame.

This class provides methods for:
- Checking data consistency
- Removing duplicates
- Handling missing values
- Filtering outliers
- Renaming and reordering columns
- Formatting strings
- Parsing date fields

Use this class to perform similar cleaning operations across multiple files.  
You are not required to use this class, but it shows how we can organize 
reusable data cleaning logic - or you can use the logic examples in your own code.

Example:
    from utils.data_scrubber import DataScrubber
    scrubber = DataScrubber(df)
    df = scrubber.remove_duplicate_records().handle_missing_data(fill_value="N/A")

"""

import io
import pandas as pd
from typing import Dict, Tuple, Union, List
from pathlib import Path
from utils_logger import logger

class DataScrubber:
    def __init__(self, df: pd.DataFrame):
        """
        Initialize the DataScrubber with a DataFrame.
        
        Parameters:
            df (pd.DataFrame): The DataFrame to be scrubbed.
        """
        self.df = df
    @classmethod
    def from_csv(cls,path: Union[str,Path]) -> "DataScrubber":
        df = pd.read_csv(path)
        return cls(df)
    def to_csv(self,path: Union[str,Path], index:bool =False) -> None:
        """
        Save the cleaned dataframe to a csv file.
        parameters:
        path(str or path): destination path for the csv file
        index(bool): whether to include the index in the output file'
        """
        self.df.to_csv(path,index=index)

    def check_data_consistency_before_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        """
        Check data consistency before cleaning by calculating counts of null and duplicate entries.
        
        Returns:
            dict: Dictionary with counts of null values and duplicate rows.
        """
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def check_data_consistency_after_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        """
        Check data consistency after cleaning to ensure there are no null or duplicate entries.
        
        Returns:
            dict: Dictionary with counts of null values and duplicate rows, expected to be zero for each.
        """
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        assert null_counts.sum() == 0, "Data still contains null values after cleaning."
        assert duplicate_count == 0, "Data still contains duplicate records after cleaning."
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def convert_column_to_new_data_type(self, column: str, new_type: type) -> pd.DataFrame:
        """
        Convert a specified column to a new data type.
        
        Parameters:
            column (str): Name of the column to convert.
            new_type (type): The target data type (e.g., 'int', 'float', 'str').
        
        Returns:
            pd.DataFrame: Updated DataFrame with the column type converted.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df[column] = self.df[column].astype(new_type)
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def drop_columns(self, columns: List[str]) -> pd.DataFrame:
        """
        Drop specified columns from the DataFrame.
        
        Parameters:
            columns (list): List of column names to drop.
        
        Returns:
            pd.DataFrame: Updated DataFrame with specified columns removed.

        Raises:
            ValueError: If a specified column is not found in the DataFrame.
        """
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df.drop(columns=columns)
        return self.df

    def filter_column_outliers(self, column: str, lower_bound: Union[float, int], upper_bound: Union[float, int]) -> pd.DataFrame:
        """
        Filter outliers in a specified column based on lower and upper bounds.
        
        Parameters:
            column (str): Name of the column to filter for outliers.
            lower_bound (float or int): Lower threshold for outlier filtering.
            upper_bound (float or int): Upper threshold for outlier filtering.
        
        Returns:
            pd.DataFrame: Updated DataFrame with outliers filtered out.
 
        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df = self.df[(self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)]
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def format_column_strings_to_lower_and_trim(self, column: str) -> pd.DataFrame:
        """
        Format strings in a specified column by converting to lowercase and trimming whitespace.
        
        Parameters:
            column (str): Name of the column to format.
        
        Returns:
            pd.DataFrame: Updated DataFrame with formatted string column.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df[column] = self.df[column].str.lower().str.strip()
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        
    def format_column_strings_to_upper_and_trim(self, column: str) -> pd.DataFrame:
        """
        Format strings in a specified column by converting to uppercase and trimming whitespace.
        
        Parameters:
            column (str): Name of the column to format.
        
        Returns:
            pd.DataFrame: Updated DataFrame with formatted string column.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
        
            self.df[column] = self.df[column]
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def handle_missing_data(self, drop: bool = False, fill_value: Union[None, float, int, str] = None) -> pd.DataFrame:
        """
        Handle missing data in the DataFrame.
        
        Parameters:
            drop (bool, optional): If True, drop rows with missing values. Default is False.
            fill_value (any, optional): Value to fill in for missing entries if drop is False.
        
        Returns:
            pd.DataFrame: Updated DataFrame with missing data handled.
        """
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    def inspect_data(self) -> Tuple[str, str]:
        """
        Inspect the data by providing DataFrame information and summary statistics.
        
        Returns:
            tuple: (info_str, describe_str), where `info_str` is a string representation of DataFrame.info()
                   and `describe_str` is a string representation of DataFrame.describe().
        """
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        info_str = buffer.getvalue()  # Retrieve the string content of the buffer

        # Capture the describe output as a string
        describe_str = self.df.describe().to_string()  # Convert DataFrame.describe() output to a string
        return info_str, describe_str

    def parse_dates_to_add_standard_datetime(self, column: str) -> pd.DataFrame:
        """
        Parse a specified column as datetime format and add it as a new column named 'StandardDateTime'.
        
        Parameters:
            column (str): Name of the column to parse as datetime.
        
        Returns:
            pd.DataFrame: Updated DataFrame with a new 'StandardDateTime' column containing parsed datetime values.

        Raises:
            ValueError: If the specified column not found in the DataFrame.
        """
        try:
            self.df['StandardDateTime'] = pd.to_datetime(self.df[column])
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def remove_duplicate_records(self) -> pd.DataFrame:
        """
        Remove duplicate rows from the DataFrame.
        
        Returns:
            pd.DataFrame: Updated DataFrame with duplicates removed.

        """
        self.df = self.df.drop_duplicates()
        return self.df

    def rename_columns(self, column_mapping: Dict[str, str]) -> "DataScrubber":
        """
        Rename columns in the DataFrame based on a provided mapping.

        Parameters:
            column_mapping (Dict[str, str]): Dictionary where keys are old column names and values are new names.

        Returns:
            DataScrubber: The updated instance with renamed columns.

        Raises:
            ValueError: If any specified column is not found in the DataFrame.
            RuntimeError: If renaming fails due to unexpected structure.
        """
        try:
            missing = set(column_mapping.keys()) - set(self.df.columns)
            if missing:
                raise ValueError(f"Missing columns in DataFrame: {', '.join(missing)}")

            self.df = self.df.rename(columns=column_mapping)
            logger.info(f"Renamed columns: {column_mapping}")
            return self
        except ValueError as ve:
            logger.error(f"Column validation failed: {ve}")
            raise
        except Exception as e:
            logger.error(f"Failed to rename columns: {e}", exc_info=True)
            raise RuntimeError("Column renaming failed due to unexpected error.") from e
    def reorder_columns(self, columns: List[str]) -> "DataScrubber":
        """
        Reorder DataFrame columns to match the specified list.

        Parameters:
            columns (List[str]): Desired column order.

        Returns:
            DataScrubber: The updated instance with reordered columns.

        Raises:
            ValueError: If any specified column is missing from the DataFrame.
            RuntimeError: If reordering fails due to unexpected structure.
        """
        try:
            missing = set(columns) - set(self.df.columns)
            if missing:
                raise ValueError(f"Missing columns in DataFrame: {', '.join(missing)}")

            self.df = self.df.loc[:, columns]
            logger.info(f"Reordered columns: {columns}")
            return self
        except ValueError as ve:
            logger.error(f"Column validation failed: {ve}")
            raise
        except Exception as e:
            logger.error(f"Failed to reorder columns: {e}", exc_info=True)
            raise RuntimeError("Column reordering failed due to unexpected error.") from e
    def standardize_column_names(self) -> "DataScrubber":
        """
        Standardize column names by:
        - Stripping leading/trailing whitespace
        - Converting to lowercase
        - Replacing spaces with underscores

        Returns:
            DataScrubber: The updated instance with standardized column names.

        Raises:
            RuntimeError: If column renaming fails due to unexpected structure.
        """
        try:
            original_columns = list(self.df.columns)
            self.df.columns = [
                col.strip().lower().replace(" ", "_") if isinstance(col, str) else col
                for col in self.df.columns
            ]
            logger.info(f"Standardized column names: {original_columns} → {list(self.df.columns)}")
            return self
        except Exception as e:
            logger.error(f"Failed to standardize column names: {e}", exc_info=True)
            raise RuntimeError("Column name standardization failed.") from e
    def strip_whitespace(self) -> "DataScrubber":
        """
        Strip leading and trailing whitespace from all string cells in the DataFrame.

        Returns:
            DataScrubber: The updated instance with whitespace stripped.

        Raises:
            RuntimeError: If the operation fails due to unexpected data types or structure.
        """
        try:
            self.df = self.df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            logger.info("Whitespace stripped successfully.")
            return self
        except Exception as e:
            logger.error(f"Failed to strip whitespace: {e}", exc_info=True)
            raise RuntimeError("Whitespace stripping failed due to unexpected data format.") from e

    def drop_empty_rows(self) -> "DataScrubber":
        """
        Drop rows that are completely empty (all values are NaN).

        Returns:
            DataScrubber: The updated instance with empty rows removed.

        Raises:
            RuntimeError: If the operation fails due to unexpected data format or structure.
        """
        try:
            original_count = len(self.df)
            self.df.dropna(how="all", inplace=True)
            removed = original_count - len(self.df)
            logger.info(f"Removed {removed} completely empty rows.")
            return self
        except Exception as e:
            logger.error(f"Failed to drop empty rows: {e}", exc_info=True)
            raise RuntimeError("Empty row removal failed due to unexpected data format.") from e
    def drop_duplicates(self) -> "DataScrubber":
        """
        Drop duplicate rows from the DataFrame.

        Returns:
            DataScrubber: The updated instance with duplicates removed.

        Raises:
            RuntimeError: If the operation fails due to unexpected data format or structure.
        """
        try:
            original_count = len(self.df)
            self.df = self.df.drop_duplicates()
            removed = original_count - len(self.df)
            logger.info(f"Removed {removed} duplicate rows.")
            return self
        except Exception as e:
            logger.error(f"Failed to drop duplicates: {e}", exc_info=True)
            raise RuntimeError("Duplicate removal failed due to unexpected data format.") from e