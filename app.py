import json
import glob
import os
import sys
import pandas as pd

def get_column_names(schemas, table_name, sorting_key = 'column_position'):
        """
        Extract ordered column names for a table from the loaded schema.

        Expected schema shape (common format used in this project):
            schemas -> { <table_name>: [ { 'column_name': ..., 'column_position': ... }, ... ], ... }

        Parameters:
            schemas (dict): mapping of table names to their column definitions
            table_name (str): name of the table whose columns we want
            sorting_key (str): key inside each column dict used to determine order

        Returns:
            list: ordered list of column names (strings)

        Notes:
            - If `table_name` is not present in `schemas` a KeyError will be raised by the call site.
            - Each item in the column list is expected to be a dict containing at least
                the keys 'column_name' and the sorting_key (default 'column_position').
        """
        column_details = schemas[table_name]
        # Sort column definitions by the provided sorting key (e.g. column_position)
        sorted_columns = sorted(column_details, key=lambda x: x[sorting_key])
        # Extract the column_name from each column definition
        column_names = [col['column_name'] for col in sorted_columns]
        return column_names


def read_csv(file, schemas):
        """
        Read a source CSV file into a pandas DataFrame and assign column names

        Parameters:
            file (str): path to the source file (expected shape: .../<table_name>/part-...)
            schemas (dict): loaded schema mapping used to obtain column names

        Returns:
            pandas.DataFrame: DataFrame with columns named according to the schema

        Notes:
            - This function assumes the source files have no header row (header=None).
        """
        # Extract file and table name from the path. Using split('/') because calling code
        # constructs paths in the same style; change to os.path if you need cross-platform handling.
        file_parts = file.split('/')
        file_name = file_parts[-1]
        table_name = file_parts[-2]

        # Lookup ordered column names for this table and read the CSV
        columns = get_column_names(schemas, table_name)
        df = pd.read_csv(file, header=None, names=columns)
        return df

def to_json(df, tgt_base_path, table_name, file_name):
        """
        Write the given DataFrame to newline-delimited JSON at the target path.

        Parameters:
            df (pandas.DataFrame): Data to write
            tgt_base_path (str): base directory where per-table folders will be created
            table_name (str): name of the table (subdirectory under tgt_base_path)
            file_name (str): output filename (typically the source part file name)

        Behavior:
            - Ensures the table directory exists, then writes JSON with one JSON object
                per line (orient='records', lines=True) which is convenient for streaming
                and many downstream tools.
        """
        json_tgt_path = f"{tgt_base_path}/{table_name}/{file_name}"
        os.makedirs(f'{tgt_base_path}/{table_name}', exist_ok=True)
        df.to_json(json_tgt_path, orient='records', lines=True)

def file_format_converter(src_base_dir, tgt_base_path, table_name):
        """
        Convert all source partition files for a single table from CSV to JSON.

        Parameters:
            src_base_dir (str): directory containing the source `schemas.json` and table subfolders
            tgt_base_path (str): directory where converted JSON files will be written
            table_name (str): name of the table to convert

        Raises:
            NameError: if no source files are found for the requested table

        Notes:
            - The function currently opens the schemas file directly; callers may want to
                manage schema loading and caching for performance if converting many tables.
        """
        # Load table schemas from the source base directory. This uses json.load on an
        # open file handle; it will raise FileNotFoundError if the file doesn't exist.
        schemas = json.load(open(f'{src_base_dir}/schemas.json', 'r'))

        # Find source partition files for the requested table
        files = glob.glob(f'{src_base_dir}/{table_name}/part-*')
        if len(files) == 0:
                # Upstream code expects a NameError here to indicate missing source files
                raise NameError(f'No source files found for table: {table_name}')

        # Process each partition file and convert it to JSON
        for file in files:
                df = read_csv(file, schemas)
                file_name = file.split('/')[-1]
                to_json(df, tgt_base_path, table_name, file_name)

def process_all_tables(table_names = None):
    """
    Process one or more tables by converting their source partitions to JSON.

    This function reads required configuration from environment variables:
      - SRC_BASE_DIR: base directory containing `schemas.json` and source data
      - TGT_BASE_PATH: base directory where converted JSON files will be written

    Parameters:
      table_names (iterable|None): iterable of table names to process. If None,
        all tables declared in `schemas.json` will be processed.

    Behavior:
      - For each table, calls `file_format_converter`. If a table has no source
        files, a NameError is caught and the table is skipped (processing continues).
    """
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    tgt_base_path = os.environ.get('TGT_BASE_PATH')
    # Load schemas to determine available tables when not explicitly provided
    schemas = json.load(open(f'{src_base_dir}/schemas.json', 'r'))
    if not table_names:
        table_names = schemas.keys()
    for table_name in table_names:
        try:
            print(f'Processing table: {table_name}')
            file_format_converter(src_base_dir, tgt_base_path, table_name)
        except NameError as e:
            # Missing source files for this table — report and continue with others
            print(f'Error processing table {table_name}')
            pass

if __name__ == "__main__":
    # Optional argument: JSON-encoded list of table names to process
    ds_names = json.loads(sys.argv[1]) if len(sys.argv) > 1 else None
    process_all_tables(ds_names)