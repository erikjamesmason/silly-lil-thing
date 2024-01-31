from dagster import Definitions, load_assets_from_modules

from .assets import iris_assets, pokemon_assets
from .resources import resources

all_assets = load_assets_from_modules([iris_assets, pokemon_assets])

bigquery_resource = resources.bigquery_resource
duckdb_resource = resources.duckdb_resource

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": duckdb_resource,
        "bigquery": bigquery_resource,
    },
)
