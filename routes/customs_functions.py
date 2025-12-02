from flask import send_from_directory
import polars as pl
import config
from datetime import datetime
import glob

# Load the dataset with filtering on a company code column value
def load_data(folder_path: str, filter_column: str, filter_value, columns: list, amount_column: str, start_date, end_date,
              company_code, year, document_number="", bank=False) -> pl.DataFrame:

    # Path to the Excel files
    files = glob.glob(folder_path+company_code+"/"+year+"/*")

    start_date = datetime.strptime(start_date, "%d/%m/%Y")
    end_date = datetime.strptime(end_date, "%d/%m/%Y")

    # Read and merge with authoritative schema from config.expected_dtypes
    df_list = []
    mismatched_files = []
    expected_map = getattr(config, 'expected_dtypes', None)

    # helper to map string names to polars dtypes
    dtype_map = {
        'Utf8': pl.Utf8,
        'Float64': pl.Float64,
        'Int64': pl.Int64,
        'Int32': pl.Int32,
        'Date': pl.Date,
        'Time': pl.Time,
    }

    for f in files:
        try:
            df_file = pl.read_excel(f)
        except Exception:
            # unreadable file -> create empty frame with expected columns if available
            if expected_map:
                empty_schema = {col: dtype_map.get(expected_map.get(col, 'Utf8'), pl.Utf8) for col in columns}
                df_list.append(pl.DataFrame(schema=empty_schema))
            else:
                df_list.append(pl.DataFrame({c: pl.Series([], dtype=pl.Utf8) for c in columns}))
            mismatched_files.append(f)
            continue

        df_file = df_file.select(columns)

        # if expected schema provided, attempt to coerce and record mismatches
        file_had_mismatch = False
        if expected_map:
            for col, expected_name in expected_map.items():
                if col not in df_file.columns:
                    continue
                expected_dtype = dtype_map.get(expected_name, None)
                if expected_dtype is None:
                    continue
                actual_dtype = df_file.schema.get(col)
                # compare by name where possible
                if actual_dtype != expected_dtype:
                    print(f"Schema mismatch in file {f} for column {col}: expected {expected_dtype}, got {actual_dtype}")
                    file_had_mismatch = True
                    try:
                        # try reasonable casts
                        if expected_dtype == pl.Date:
                            df_file = df_file.with_columns(pl.col(col).str.strptime(pl.Date, "%d/%m/%Y").alias(col))
                            print(f"  - coerced column {col} to Date")
                        elif expected_dtype == pl.Time:
                            df_file = df_file.with_columns(pl.col(col).str.strptime(pl.Time, "%H:%M:%S").alias(col))
                            print(f"  - coerced column {col} to Time")
                        else:
                            df_file = df_file.with_columns(pl.col(col).cast(expected_dtype).alias(col))
                            print(f"  - coerced column {col} to {expected_dtype}")
                    except Exception:
                        # cast failed; leave the column as-is but mark mismatch
                        print(f"  - failed to coerce column {col}")
                        pass

        if file_had_mismatch:
            mismatched_files.append(f)

        df_list.append(df_file)

    # report mismatched files (non-blocking)
    if mismatched_files:
        print("Files with schema mismatches (attempted coercion, continuing):")
        for mf in mismatched_files:
            print(" -", mf)

    # concatenate
    df_polars = pl.concat(df_list) if df_list else pl.DataFrame()

    if df_polars.schema["Entry Date"] != pl.Date:
        df_polars = df_polars.with_columns(
            pl.col("Entry Date").str.to_date(format="%d/%m/%Y").alias("Entry Date")
        )
    
    if (df_polars.schema["Time of Entry"] != pl.Time or df_polars.schema["Time of Entry"] != "datetime[ms]") and df_polars.schema["Time of Entry"] == pl.String:
        df_polars = df_polars.with_columns(
            pl.col("Time of Entry").str.to_time(format="%H:%M:%S").alias("Time of Entry")
        )

    df_polars = df_polars.with_columns(pl.col("Désignation").replace("#N/A", ""))
    df_polars = df_polars.with_columns(pl.col(config.offset_account_column_name).cast(pl.Utf8))
    df_polars = df_polars.with_columns(pl.col(config.SYSCOHADA_column_in_main_data).cast(pl.Utf8))
    df_polars = df_polars.with_columns(pl.col("G/L Account").cast(pl.Utf8))
    df_polars = df_polars.with_columns(pl.col("Fiscal Year").cast(pl.Utf8))
    df_polars = df_polars.with_columns(pl.col(amount_column).cast(pl.Float64).fill_null(0))
    df_polars = df_polars.sort(config.posting_date_column_name, descending=False)
    df_polars = df_polars.filter((pl.col(config.posting_date_column_name) >= start_date) & (pl.col(config.posting_date_column_name) <= end_date))

    # Replace empty strings with NULL, then apply fill logic
    df_polars = df_polars.with_columns([
        pl.when(pl.col("Text") == "")
        .then(None)  # Convert empty strings to null
        .otherwise(pl.col("Text"))
        .alias("Text"),

        pl.when(pl.col("Reference") == "")
        .then(None)
        .otherwise(pl.col("Reference"))
        .alias("Reference")
    ])

    if document_number != "":
        return df_polars.filter((pl.col(filter_column) == filter_value) & (pl.col("Document Number") == document_number))
    else:
        if bank:
            return df_polars.filter((pl.col(filter_column) == filter_value) & (pl.col(config.SYSCOHADA_column_in_main_data).is_not_null()) &
                                                                           (pl.col(config.SYSCOHADA_column_in_main_data).is_in(config.bnk_gls)))
        else:
            return df_polars.filter((pl.col(filter_column) == filter_value) & (pl.col(config.SYSCOHADA_column_in_main_data).is_not_null()))



# Load the dataset with filtering on a company code column value
def load_bp_data(folder_path: str, filter_column: str, filter_value, columns: list, start_date, end_date,
              company_code, year, bp_type) -> pl.DataFrame:
    # Path to the Excel files
    files = glob.glob(folder_path+company_code+"/"+year+"/*")

    start_date = datetime.strptime(start_date, "%d/%m/%Y")
    end_date = datetime.strptime(end_date, "%d/%m/%Y")

    # Read and merge
    df_list = [pl.read_excel(f) for f in files]
    df_polars = pl.concat(df_list)

    # df_pandas = pd.read_csv(folder_path, usecols=columns, encoding="latin1")

    df_polars = df_polars.with_columns(pl.col(bp_type).cast(pl.Utf8))
    df_polars = df_polars.with_columns(pl.col("Amount in local currency").cast(pl.Float64).fill_null(0))
    df_polars = df_polars.sort(config.posting_date_column_name, descending=False)
    df_polars = df_polars.filter((pl.col(config.posting_date_column_name) >= start_date) & (pl.col(config.posting_date_column_name) <= end_date))

    # Replace empty strings with NULL, then apply fill logic
    df_polars = df_polars.with_columns([
        pl.when(pl.col("Text") == "")
        .then(None)  # Convert empty strings to null
        .otherwise(pl.col("Text"))
        .alias("Text"),

        pl.when(pl.col("Reference") == "")
        .then(None)
        .otherwise(pl.col("Reference"))
        .alias("Reference")
    ])

    return df_polars.filter((pl.col(filter_column) == filter_value) & (pl.col(bp_type).is_not_null()))


# Load initial balance and code journal mapping datasets
def load_initial_balance_mapping_data(initial_balance_file_path: str, debit_column_label: str,
                                      credit_column_label: str, company_code, year, bank=False) -> pl.DataFrame:
    # Load Initial Balance data
    df_initial_balance = pl.read_excel(f"{initial_balance_file_path} {company_code} {year}.xlsx")
    df_initial_balance = df_initial_balance.with_columns(pl.col("Numéro de compte IFRS").cast(pl.Utf8))
    df_initial_balance = df_initial_balance.with_columns(pl.col(debit_column_label).cast(pl.Float64).fill_null(0))
    df_initial_balance = df_initial_balance.with_columns(pl.col(credit_column_label).cast(pl.Float64).fill_null(0))
    df_initial_balance = df_initial_balance.with_columns(pl.col(config.SYSCOHADA_column_in_initial_balance).cast(pl.Utf8).fill_null("OHADA VIDES"))

    if bank:
        return df_initial_balance.filter(pl.col(config.SYSCOHADA_column_in_initial_balance).is_in(config.bnk_gls))
    else:
        return df_initial_balance


# Load vendors initial balance
def load_bp_initial_balance(initial_balance_file_path: str, balance_column_label: str, company_code, year, bp_type) -> pl.DataFrame:
    # Load Initial Balance data
    df_initial_balance = pl.read_excel(f"{initial_balance_file_path} {company_code} {year}.xlsx")
    df_initial_balance = df_initial_balance.with_columns(pl.col(bp_type).cast(pl.Utf8))
    df_initial_balance = df_initial_balance.with_columns(pl.col(balance_column_label).cast(pl.Float64).fill_null(0))

    return df_initial_balance

# Fetch general balance mapping data from static Plan Comptable OHADA
def fetch_general_balance_mapping_data():
    df_mapping = pl.read_excel(config.general_balance_mapping_file_path)
    df_mapping = df_mapping.rename({"Numéro de Compte": "Code", "Nom du Compte": "Description"})
    mapping = dict(zip(df_mapping["Code"].to_list(), df_mapping["Description"].to_list()))
    return mapping