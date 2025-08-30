import polars as pl

import src.transform_data as td

def build_tables(data: pl.DataFrame) -> None:
    """
    Builds all dimension and fact tables from raw EV charger data and exports them as CSV files.

    This function performs the following steps sequentially:
        1. Generates the station dimension table and writes it to CSV.
        2. Generates the operator dimension table and writes it to CSV.
        3. Generates the EV charger fact table by joining raw data with the
           dimension tables and writes it to CSV.

    Args:
        data (pl.DataFrame): Input DataFrame containing raw EV charger data,
            including columns for stations, operators, and chargers.

    Returns:
        None: All output tables are written as CSV files to the
        "data_file_output" directory.
    """
    dim_station_data = td.dim_station(data)
    dim_operator_data = td.dim_operator(data)
    build_dim_station(dim_station_data)
    build_dim_operator(dim_operator_data)
    build_fact_ev_charger(data, dim_operator_data, dim_station_data)


def build_dim_station(data: pl.DataFrame) -> None:
    """
    Builds the station dimension table from raw data and exports it as a CSV file.

    This function takes a DataFrame containing raw station information, transforms
    it into a structured dimension table (including station IDs, names, locations,
    and number of charging stations), and writes the result to a CSV file for downstream use.

    Args:
        data (pl.DataFrame): Input DataFrame containing raw station data.

    Returns:
        None: The output CSV is saved to "data_file_output/dim_station.csv".
    """
    data.write_csv("data_file_output/dim_station.csv")


def build_dim_operator(data: pl.DataFrame) -> None:
    """
    Builds the operator dimension table from raw data and exports it as a CSV file.

    This function takes a DataFrame containing raw operator information, transforms
    it into a structured dimension table (including operator IDs and names), and
    writes the result to a CSV file for downstream use in the fact table.

    Args:
        data (pl.DataFrame): Input DataFrame containing raw operator data.

    Returns:
        None: The output CSV is saved to "data_file_output/dim_operator.csv".
    """
    data.write_csv("data_file_output/dim_operator.csv")


def build_fact_ev_charger(data: pl.DataFrame, dim_operator: pl.DataFrame, dim_station: pl.DataFrame) -> None:
    """
    Builds the EV charger fact table from raw data and exports it as a CSV file.

    This function takes a DataFrame containing raw charger data, joins it with
    the operator and station dimension tables, transforms it into a structured
    fact table (including charger IDs, plug counts, and foreign keys), and writes
    the result to a CSV file for downstream analytics.

    Args:
        data (pl.DataFrame): Input DataFrame containing raw charger data.
        dim_operator (pl.DataFrame): Operator dimension table with "operator_id"
            and "operator_name" columns.
        dim_station (pl.DataFrame): Station dimension table with "station_id",
            "station_name", "lat", "long", and "num_of_charging_stations" columns.

    Returns:
        None: The output CSV is saved to "data_file_output/fact_ev_charger.csv".
    """
    fact_ev_charger = td.fact_ev_charger(data, dim_operator, dim_station)
    fact_ev_charger.write_csv("data_file_output/fact_ev_charger.csv")