from dagster_duckdb_pandas import DuckDBPandasIOManager

from dagster import Definitions, load_assets_from_modules


from . import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": DuckDBPandasIOManager(database="TD_DATALAKE/DATALAKE/3_PRODUCTION_ZONE/database.duckdb")
    },
)
