from dagster import (
    EnvVar,
    file_relative_path
)

from dagster_gcp import BigQueryResource
from dagster_duckdb import DuckDBResource

bigquery_resource = {
    "bigquery": BigQueryResource(
        project=EnvVar("BIGQUERY_PROJECT_ID"),
        location="us-west1",
        gcp_credentials=EnvVar("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS"),
    )
}

duckdb_database = "data/db/pokemon.duckdb"
# /Users/erikmason/Github/silly-lil-thing/silly_lil_thing/data/db
duckdb_resource = DuckDBResource(
    database=duckdb_database,
)
