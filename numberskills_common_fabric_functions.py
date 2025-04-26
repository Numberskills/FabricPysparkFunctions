from notebookutils import mssparkutils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.window import Window


from pyspark.sql import DataFrame
from typing import List, Tuple

def rename_dataframe_columns(df: DataFrame, column_renames: List[Tuple[str, str]]) -> DataFrame:
    """
    Renames columns in a PySpark DataFrame.

    Parameters:
        df (DataFrame): The PySpark DataFrame to rename columns in.
        column_renames (List[Tuple[str, str]]): A list of tuples where each tuple contains
                                               the old column name and the new column name.

    Returns:
        DataFrame: A new DataFrame with renamed columns.
    """
    for old_name, new_name in column_renames:
        df = df.withColumnRenamed(old_name, new_name)
    return df

def fix_date_columns(df: DataFrame, date_threshold: str):
    for column in df.columns:
        # Check if the column type is DateType
        if isinstance(df.schema[column].dataType, DateType):
            # Apply fix if necessary
            df = df.withColumn(column, when(col(column) >= date_threshold, col(column)).otherwise(None))
    return df

#def getLastSubFolder(folderPath):
#    """
#    Docstring: This function takes a folder path and returns the last sub folder
#    """
#    folders = mssparkutils.fs.ls(folderPath)
#    return folders[-1].name

def load_delta_table_from_lakehouse(table_name: str, lakehouse_name: str, columns: list = None) -> DataFrame:
    import sempy.fabric as fabric
    from pyspark.sql.functions import col, regexp_replace
    # import mssparkutils

    workspace_id = fabric.get_notebook_workspace_id()
    lakehouse_object = mssparkutils.lakehouse.get(lakehouse_name, workspace_id)

    delta_table_path = lakehouse_object['properties']['abfsPath'] + "/Tables/" + table_name

    return spark.read.format("delta").load(delta_table_path)

def write_delta_table_to_lakehouse(table_name: str, lakehouse_name: str, data_frame: DataFrame):
    import sempy.fabric as fabric
    from pyspark.sql.functions import col, regexp_replace

    workspace_id = fabric.get_notebook_workspace_id()
    lakehouse_object = mssparkutils.lakehouse.get(lakehouse_name, workspace_id)

    delta_table_path = lakehouse_object['properties']['abfsPath'] + "/Tables/" + table_name
    data_frame.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(delta_table_path)


def get_delta_table_max_column_value(delta_table_path: str, column_name: str):
	from pyspark.sql.functions import max
	from pyspark.sql.utils import AnalysisException

	# Initialize max_value
	max_value = 0

	try:
		# Read the Delta table
		df = spark.read.format("delta").load(delta_table_path)

		# Get the maximum value of the column
		max_value = df.select(max(column_name)).collect()[0][0]

	except AnalysisException as e:
		# Handle the case where the table doesn't exist
		print(f"Table not found: {e}")
		max_value = 0

	return max_value


def getLastSubFolder(folderPath):
    """
    This function takes a folder path and returns the last subfolder, 
    excluding any folder that starts with "Temp_".
    """
    # List all folders in the specified path
    folders = mssparkutils.fs.ls(folderPath)
    
    # Filter out folders that start with "Temp_"
    filtered_folders = [folder for folder in folders if not str.lower(folder.name).startswith("temp_")]
    
    # Return the last folder name from the filtered list
    if filtered_folders:
        return filtered_folders[-1].name
    else:
        return None  # Return None if no valid folders exist

def getLatestFolder(rootpath):
    """
    Docstring: This function takes a folder path and return the latest timestamp folder
    """
    # Function body: Code that performs the desired operation

    # Gets the last year folder
    folder = rootpath + "/" + getLastSubFolder(rootpath)
    # Gets the last month folder
    folder = folder + "/" + getLastSubFolder(folder)
    # Gets the last day folder
    folder = folder + "/" + getLastSubFolder(folder)
    # Gets the files in the folder
    folder = folder + "/" + getLastSubFolder(folder)

    return folder

def getLatestDateFolder(rootpath):
    """
    Docstring: This function takes a folder path and return the latest timestamp folder
    """
    # Function body: Code that performs the desired operation

    # Gets the last year folder
    folder = rootpath + "/" + getLastSubFolder(rootpath)
    # Gets the last month folder
    folder = folder + "/" + getLastSubFolder(folder)
    # Gets the last day folder
    folder = folder + "/" + getLastSubFolder(folder)

    return folder


def getFirstDateOfPreviousMonth(monthsAgo: int):
  
  date_condition = spark.sql(f"SELECT trunc(date_sub(trunc(current_date(), 'month'), {monthsAgo}), 'month') as start_of_last_month").collect()[0]['start_of_last_month']
  date_condition_string = date_condition.strftime('%Y-%m-%d')  # Formatting to 'YYYY-MM-DD'


  return date_condition_string

def getLatestFile(rootpath, file_extension=None):
    """
    Docstring: This function takes a folder path and optionally a file extension filter,
    then returns the latest file with the specified extension (if provided) in the deepest folder.
    """
    # Get the last year folder
    folder = rootpath + "/" + getLastSubFolder(rootpath)
    # Get the last month folder
    folder = folder + "/" + getLastSubFolder(folder)
    # Get the last day folder
    folder = folder + "/" + getLastSubFolder(folder)
    # Get the files in the folder
    files = mssparkutils.fs.ls(folder)
    
    # Filter files by the specified file extension (if provided)
    if file_extension:
        files = [file for file in files if file.name.endswith(f".{file_extension}")]
    
    # Sort files by their name
    sorted_files = sorted(files, key=lambda x: x.name)
    
    # Return the latest file (last element in the sorted array)
    if sorted_files:
        latest_file = sorted_files[-1]
        return latest_file.path
    else:
        return None  # Return None if no files match the filter

def add_surrogate_key(df, col_name):
    """
    DocString: This functions returns the dataframe with a surrogate key starting with 1
    """
    df = df.withColumn(col_name, monotonically_increasing_id()+1) 
    return df

def generate_select_statement(df):
    # Get column names
    columns = df.columns
    
    # Sort column names alphabetically
    columns = sorted(columns)

    # Generate select statement for each column
    select_statements = [f"df['{col}'].alias('{col.replace('_', '')}')" for col in columns]
    
    # Join select statements
    select_text = ",\n".join(select_statements)
    
    return select_text

# Example usage:
# df = spark.sql("SELECT * FROM MamLakehouse.itemledgerentry")
# select_text = generate_select_statement(df)
# print(select_text)


def process_date_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Process each date column in the DataFrame.
    For dates before 1582-10-15, replace with null and cast the column to DateType.

    :param df: The input DataFrame
    :param columns: A list of column names to be processed
    :return: DataFrame with processed date columns
    """
    for column_name in columns:
        df = df.withColumn(
            column_name, 
            when(col(column_name) < "1582-10-15", None).otherwise(col(column_name)).cast(DateType())
        )
    return df

# Usage example
# df = process_date_columns(df, ["Purchase_Date", "Another_Date_Column"])


default_debug_level = 3
def debug_display(df: DataFrame, level: int = default_debug_level):
    if show_debug:
        if level >= 1:
            # Display the count of rows in the DataFrame
            print(f"Count of rows: {df.count()}")
        if level >= 2:
            # Show the DataFrame
            display(df)
        if level == 3:
            # Print the schema of the DataFrame
            df.printSchema()



# Functions
from datetime import datetime

def create_folder_structure():
    """
    Def: Returns a timestamp
    """
    # Get current timestamp
    current_time = datetime.now()

    # Create folder structure
    folder_structure = "{}/{:02d}/{:02d}".format(
        current_time.year,
        current_time.month,
        current_time.day
    )

    return folder_structure

# Example usage:
folder_path = create_folder_structure()
print("Folder structure:", folder_path)

from datetime import datetime

def create_timestamp_string():
    # Get current timestamp
    current_time = datetime.now()

    # Create timestamp string
    timestamp_string = "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:03d}".format(
        current_time.year,
        current_time.month,
        current_time.day,
        current_time.hour,
        current_time.minute,
        current_time.second,
        current_time.microsecond // 1000  # Convert microseconds to milliseconds
    )

    return timestamp_string


def getDataFrameFromJson(filePath):
    return spark.read.option("multiline", "false").json(filePath)

def getDataFrameFromParquet(filePath):
    return spark.read.parquet(filePath)
 

def replace_spaces_in_column_names(df: DataFrame, custom_char: str):
    # Get the current column names
    columns = df.columns
    
    # Create a dictionary to map old column names to new column names
    new_columns = {col: col.replace(" ", custom_char) for col in columns}
    
    # Rename the columns using withColumnRenamed
    for old_col, new_col in new_columns.items():
        df = df.withColumnRenamed(old_col, new_col)
    
    return df

# Example usage:
# Suppose df is your DataFrame
# Replace spaces with "_" in column names
# new_df = replace_spaces_in_column_names(df, "_")

import pandas as pd
def read_excel_to_dataframe(file_path: str, sheet_name: str) -> pd.DataFrame:
    # Read Excel file into DataFrame
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name
    )
    
    return spark.createDataFrame(df)

from pyspark.sql import DataFrame
import re

def clean_column_names(df: DataFrame) -> DataFrame:
    # Define a regex pattern to match invalid characters
    pattern = re.compile(r'[ ,;{}()\n\t=]+')

    # Create a dictionary with old and new column names
    new_columns = {col: re.sub(pattern, '_', col) for col in df.columns}

    # Rename the columns using the DataFrame `withColumnRenamed` method
    for old_name, new_name in new_columns.items():
        df = df.withColumnRenamed(old_name, new_name)

    return df

def generate_col_select_statement(df, aliasName):
    # Get column names
    columns = df.columns

    # Sort column names alphabetically
    columns = sorted(columns)
    
    # Generate select statement for each column
    select_statements = [f"col('{aliasName}.{col}').alias('{col.replace('_', '')}')" for col in columns]
    
    # Join select statements
    select_text = f"df.alias('{aliasName}').select(" + ",\n\t".join(select_statements) + "\n" + ")"
    
    return select_text


def generate_col_select_statement2(df, aliasName):
    # Get column names
    columns = df.columns
    
    # Generate select statement for each column
    select_statements = [
        f"col('{aliasName}.`{c}`').alias('{c.split(':')[-1]}')"
        for c in columns
    ]
    
    # Join select statements
    select_text = f"df.alias('{aliasName}').select(" + ",\n\t".join(select_statements) + "\n)"
    
    return select_text

def generate_json_select_statement(df):
    # Get the schema of the nested struct
    struct_fields = df.schema["value_exploded"].dataType.fields

    select_statement = 'df_select = df_exploded.select(\n'
    for field in struct_fields:
        column_name = field.name
        select_statement += f'    col("value_exploded.{column_name}").alias("{column_name}"),\n'
    select_statement = select_statement.rstrip(',\n') + '\n)'
    print(select_statement)

from pyspark.sql.window import Window
def add_surrogate_key_new(df, col_name):
    """
    DocString: This function returns the dataframe with a surrogate key starting with 1.
    """
    # Create a window specification
    window_spec = Window.orderBy(df.columns[0])  # Ordering by the first column (you can customize this as needed)

    # Add the surrogate key column using row_number
    df = df.withColumn(col_name, row_number().over(window_spec))

    # Reorder columns to make the surrogate key the first column
    columns = [col_name] + [col for col in df.columns if col != col_name]
    df = df.select(*columns)

    return df


def add_surrogate_key_new2(df, col_name, startNumber=1):
    """
    DocString: This function returns the dataframe with a surrogate key starting with the given startNumber (default is 1).
    """
    # Create a window specification
    window_spec = Window.orderBy(df.columns[0])  # Ordering by the first column (you can customize this as needed)

    # Add the surrogate key column using row_number, adjusted by startNumber
    df = df.withColumn(col_name, row_number().over(window_spec) + (lit(startNumber) - 1))

    # Reorder columns to make the surrogate key the first column
    columns = [col_name] + [col for col in df.columns if col != col_name]
    df = df.select(*columns)

    return df


from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

def print_pandas_dataframe_select_statement(df, alias='B'):
    # Define any custom type casting based on the column names (e.g., DoubleType for Amount)
    cast_columns = ['Amount']  # Add more columns here if necessary
    
    # Replace spaces in column names with appropriate formatting for aliasing and casting
    select_expr = []
    for column in df.columns:
        # Clean column name by removing extra spaces
        clean_col = column.strip().replace(" ", "")
        
        # Check if column needs casting
        if column in cast_columns:
            select_expr.append(f"col('{alias}.{column}').cast(DoubleType()).alias('{clean_col}')")
        else:
            select_expr.append(f"col('{alias}.{column}').alias('{clean_col}')")
    
    # Join the expressions to form the full select statement
    select_statement = f"sdf.alias('{alias}').select(\n\t" + ",\n\t".join(select_expr) + "\n)"
    
    # Print the select statement
    print(select_statement)

def generate_dynamic_merge_script(df_source, table_name, surrogate_key, primary_key):
    """
    Generates and prints a merge script for the target_table using df_source as the source dataframe.

    :param df_source: The source dataframe (e.g., df_final)
    :param surrogate_key: The surrogate key to be used in the insert operation
    :param primary_key: The primary key column for the matching condition
    :return: The merge statement as a printed string
    """
    
    # Define fixed values for the target table and source dataframe
    target_table_name = "target"
    df_source_name = "source"
    
    # Get the list of columns from the dataframe
    columns = df_source.columns
    
    # Construct the update part (exclude surrogate_key and "SilverInsertedAt")
    update_columns = [col for col in columns if col != surrogate_key and col != primary_key and col != "SilverInsertedAt"]
    update_set = ',\n    '.join([f'"{target_table_name}.{col}": "{df_source_name}.{col}"' for col in update_columns])

    # Construct the insert part (include "SilverInsertedAt" and all other columns)
    insert_columns = [col for col in columns]
    insert_values = ',\n    '.join([f'"{col}": "{df_source_name}.{col}"' for col in insert_columns])
    # insert_values += f',\n    "{surrogate_key}": "{df_source_name}.{surrogate_key}"'  # Add surrogate key in insert

    # Generate the matching condition based on the primary key
    matching_condition = f"{target_table_name}.{primary_key} = {df_source_name}.{primary_key}"

    # Generate the merge script
    merge_script = f"""
from delta.tables import DeltaTable
from pyspark.sql.functions import from_utc_timestamp, current_timestamp, col

# Load the target Delta table
target_table = DeltaTable.forPath(spark, "Tables/{table_name}")

target_table.alias("{target_table_name}").merge(
    df_final.alias("{df_source_name}"),
    "{matching_condition}"
).whenMatchedUpdate(set={{
    {update_set}
}}).whenNotMatchedInsert(values={{
    {insert_values}
}}).execute()
"""

    # Generate the Create Delta Table if doesn't exist script
    generate_delta_table_script = f"""
from delta.tables import DeltaTable
from pyspark.sql.functions import from_utc_timestamp, current_timestamp
import os

# Path to the Delta table
delta_table_path = "Tables/{table_name}"

# Check if the Delta table exists
if not DeltaTable.isDeltaTable(spark, delta_table_path):
    print(f"Table at {{delta_table_path}} does not exist. Creating a new Delta table.")
    df_with_sk = add_surrogate_key_new(df_select, "{surrogate_key}")
    df_with_sk.write.format("delta").mode("overwrite").save(delta_table_path)

"""

    # Generate the get max sk script
    get_max_sk_script = f"""
# Load the Delta table as a DataFrame to get the current max BudgetFile_SK
df_for_max_sk = spark.read.format("delta").load("Tables/{table_name}")

# Get the maximum surrogate key
max_sk = df_for_max_sk.agg({{"{surrogate_key}": "max"}}).collect()[0][0]
if max_sk is None:
    max_sk = 0  # If the table is empty, start from 0
"""

    # Build source dataframe script
    build_source_dataframe_script = f"""
debug_display(df_select)

# Perform a left anti join to get only new (unmatched) rows
unmatched_rows = df_select.alias("source").join(
    df_for_max_sk.alias("target"),
    col("source.{primary_key}") == col("target.{primary_key}"),
    how="left_anti"  # This returns only rows in source that are not in target
)
df_unmatched_with_sk = add_surrogate_key_new2(unmatched_rows, "{surrogate_key}", max_sk+1).select(
    col("{primary_key}").alias("unmatched_{primary_key}"), 
    col("{surrogate_key}")
).alias("new_rows")

debug_display(df_unmatched_with_sk)

df_final = df_select.alias("source").join(
    df_unmatched_with_sk,
    col("source.{primary_key}") == col("new_rows.unmatched_{primary_key}"),
    how="left_outer"  # This ensures that matched rows keep their existing Surrogate Key
).withColumn(
    "{surrogate_key}", 
    col("new_rows.{surrogate_key}")
).drop(
    "unmatched_{primary_key}"
)
debug_display(df_final)
"""

    # Print the generated merge script
    print("### Generate Delta Table Script START")
    print(generate_delta_table_script)
    print("### Generate Delta Table Script END")
    print("### Get Max SK script START")
    print(get_max_sk_script)
    print("### Get Max SK script END")
    print("### Build source dataframe START")
    print(build_source_dataframe_script)
    print("### Build source dataframe END")
    print("### Merge Script START")
    print(merge_script)
    print("### Merge Script END")
    
    
    
def merge_delta_table(dataframe, table_name, join_columns, exclude_update_columns,surrogate_key,is_return_result=0):
    """
    Merge a DataFrame into a Delta table and handle surrogate key generation.

    Args:
        dataframe: Input DataFrame to be merged.
        table_name: Name of the Delta table  like this format <Lakehouse name>.<table name>
        join_columns: Comma-separated string of column names used for merging.
        exclude_update_columns: Comma-separated string of column names to exclude from updates.
        surrogate_key: surrogate_key column of the delta table
        is_return_result: 1 when function need to return the result of the merge, 0 when return value is not needed. 

    Returns:
        Latest table change history metrics
    """    
    # create table path
    table_path = f"abfss://{fabric.resolve_workspace_name()}@onelake.dfs.fabric.microsoft.com/{table_name.split('.')[0]}.Lakehouse/Tables/{table_name.split('.')[1]}"

    # Parse the comma-separated strings into lists
    join_column_list = [col.strip() for col in join_columns.split(",")]
    exclude_update_column_list = [col.strip() for col in exclude_update_columns.split(",")]
    surrogate_key_list = [col.strip() for col in surrogate_key.split(",")]
    exclude_update_column_list = exclude_update_column_list + surrogate_key_list
    # Construct the merge condition
    merge_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in join_column_list])

    # check if the delta table is alreay exist
    if DeltaTable.isDeltaTable(spark, table_path):
        # Generate surrogate_key column value
        max_id_df = spark.sql(f"SELECT MAX({surrogate_key}) as max_id FROM {table_name}")
        max_id = max_id_df.collect()[0]['max_id']
        max_id = max_id or 0
        dataframe_with_id = (
            dataframe
            .withColumn(surrogate_key, row_number().over(Window.orderBy(lit(1))) + max_id)
        )
        # Create the update set dictionary, excluding specified columns
        update_set = {
            col: f"source.`{col}`"
            for col in dataframe_with_id.columns
            if col not in exclude_update_column_list
        }        
        # Load the Delta table
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("target").merge(
            dataframe_with_id.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            set=update_set
        ).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    else:
        # create table in case table not present
        dataframe_with_id = (
            dataframe
            .withColumn(surrogate_key,row_number().over(Window.orderBy(lit(1))))
        )
        dataframe_with_id.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").saveAsTable(table_name)
        print(table_path+" Table created") 

    if is_return_result==1: return delta_table.history().orderBy("version", ascending=False).first()["operationMetrics"]    

