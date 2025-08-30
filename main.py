import polars as pl

import src.build_table_files as btf


def main() -> None:
    """
    Main entry point for building EV charger dimension and fact tables.

    This function reads the raw EV charger data from a CSV file, builds
    all dimension and fact tables, and exports them to the output
    directory. Basic error handling is provided for missing files and
    schema issues.

    Raises:
        FileNotFoundError: If the input CSV file does not exist.
        pl.exceptions.SchemaError: If the input data schema is invalid.

    Returns:
        None: The function writes output tables to the "data_file_output" directory.
    """
    try:
        ev_charger_data = pl.read_csv("raw_data/fast_chargers.csv")
        btf.build_tables(ev_charger_data)
    except FileNotFoundError as e:
        print(f"An error occurred: {e}")
    except pl.exceptions.SchemaError as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()