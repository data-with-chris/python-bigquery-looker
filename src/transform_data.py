import polars as pl

def dim_station(data: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw station data into a structured station dimension table.

    This function selects relevant station information, renames the columns to
    standardised names, assigns a unique station ID, and returns the resulting
    dimension table.

    Args:
        data (pl.DataFrame): Input DataFrame containing raw station data with columns
        such as "Station name", "Station address", "Latitude", "Longitude",
        and "Number of station".

    Returns:
        pl.DataFrame: A DataFrame representing the station dimension table with the
        following columns:
            - station_id (int): Unique identifier for the station.
            - station_name (str): Name of the station.
            - num_of_charging_stations (int): Number of charging stations available.
            - lat (float): Latitude of the station.
            - long (float): Longitude of the station.
    """
    columns = data.select(
        "Station name",
        "Station address",
        "Latitude",
        "Longitude",
        "Number of station"
    )
    renamed_columns = columns.rename({
        "Station name": "station_name",
        "Station address": "address",
        "Latitude": "lat",
        "Longitude": "long",
        "Number of station": "num_of_charging_stations"
    })
    dim_station_data = renamed_columns.with_columns(
        pl.arange(1, renamed_columns.height + 1).alias("station_id")
    ).select(
        "station_id",
        "station_name",
        "num_of_charging_stations",
        "lat",
        "long")
    return dim_station_data


def dim_operator(data: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw operator data into a structured operator dimension table.

    This function extracts unique operators, standardises column names, adds
    an "Unknown" operator entry, handles null values, and assigns unique
    operator IDs.

    Args:
        data (pl.DataFrame): Input DataFrame containing raw operator data with
        a column named "Operator".

    Returns:
        pl.DataFrame: A DataFrame representing the operator dimension table with
        the following columns:
            - operator_id (int): Unique identifier for the operator.
            - operator_name (str): Name of the operator, cleaned and standardised.
    """
    selected_columns = data.select(
        "Operator"
    ).unique(subset="Operator")
    renamed_columns = selected_columns.rename({
        "Operator": "operator_name"
    })
    unknown = pl.DataFrame({"operator_name": ["Unknown"]})
    renamed_columns = pl.concat([renamed_columns, unknown])
    dim_operator_data = renamed_columns.fill_null("None").with_columns(
        pl.arange(1, renamed_columns.height + 1).alias("operator_id"),
        pl.col("operator_name").str.strip_chars()
    )
    return dim_operator_data


def fact_ev_charger(data: pl.DataFrame, dim_operator_table: pl.DataFrame, dim_station_table: pl.DataFrame) -> pl.DataFrame:
    """
    Builds the EV charger fact table by combining station and operator dimensions.

    This function joins raw charger data with the operator and station dimension
    tables, renames key columns, assigns IDs, handles null operator values, and
    ensures uniqueness for charger records.

    Args:
        data (pl.DataFrame): Input DataFrame containing raw charger data with columns
            such as "ObjId", "Operator", "Station name", "Number of plugs",
            "CHAdeMO", "CCS/SAE", and "Tesla(Fast)".
        dim_operator_table (pl.DataFrame): Dimension table containing unique operators
            with columns "operator_id" and "operator_name".
        dim_station_table (pl.DataFrame): Dimension table containing unique stations
            with columns "station_id", "station_name", "num_of_charging_stations",
            "lat", and "long".

    Returns:
        pl.DataFrame: A DataFrame representing the EV charger fact table with the
        following columns:
            - charger_id (str | int): Unique identifier for the charger.
            - station_id (int): Foreign key referencing the station dimension.
            - operator_id (int): Foreign key referencing the operator dimension.
            - chademo_plug_count (int): Number of CHAdeMO plugs available.
            - ccs_sae_plug_count (int): Number of CCS/SAE plugs available.
            - tesla_plug_count (int): Number of Tesla plugs available.
            - total_plug_count (int): Total number of plugs at the charger.
    """
    temp_dataframe = data.join(
        dim_operator_table,
        how="left",
        left_on="Operator",
        right_on="operator_name",
        suffix="_op"
    ).join(
        dim_station_table,
        how="left",
        left_on="Station name",
        right_on="station_name",
        suffix="_st"
    )
    temp_dataframe_renamed_columns = temp_dataframe.rename({
    "ObjId": "charger_id",
    "Number of plugs": "total_plug_count",
    "CHAdeMO": "chademo_plug_count",
    "CCS/SAE": "ccs_sae_plug_count",
    "Tesla(Fast)": "tesla_plug_count"
    })
    fact_ev_charger_data = (temp_dataframe_renamed_columns.select(
    "charger_id",
    "station_id",
    "operator_id",
    "chademo_plug_count",
    "ccs_sae_plug_count",
    "tesla_plug_count",
    "total_plug_count")
    .with_columns(
        pl.col("operator_id").fill_null(
            dim_operator_table.filter(pl.col("operator_name") == "Unknown")["operator_id"][0]
        )
    ).unique(subset="charger_id").sort("charger_id", descending=False))
    return fact_ev_charger_data