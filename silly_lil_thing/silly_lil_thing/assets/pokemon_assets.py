import pandas as pd
import requests 
import io 
from dagster_gcp import BigQueryResource
from dagster_duckdb import DuckDBResource

from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext
)

logger = get_dagster_logger()

@asset(
    group_name='pokemon',
    compute_kind='Response'
)
def pokemon(context: AssetExecutionContext)->requests.Response:
    """
    """
    pokemon_response = requests.get('https://pokeapi.co/api/v2/pokemon?limit=2000')

    context.add_output_metadata(
        metadata={
            "status": pokemon_response.status_code,
            "url": pokemon_response.url
        },
    )

    return pokemon_response

@asset(
    group_name='pokemon',
    compute_kind='Response'
)
def pokemon_types(context: AssetExecutionContext)->requests.Response:
    """
    """
    response_types = requests.get('https://pokeapi.co/api/v2/type')

    return response_types

@asset(
    group_name='pokemon',
    compute_kind='Pandas'
)
def pokemon_df(context: AssetExecutionContext, pokemon)->pd.DataFrame:
    """
    """
    pokemon_df = pd.json_normalize(pokemon)

    context.add_output_metadata(
        metadata={
            "dataframe shape": pokemon_df.shape[0],
            "dataframe schema": str(pokemon_df.info())
        },
    )

    return pokemon_df

@asset(
    group_name='pokemon',
    compute_kind='Pandas'
)
def pokemon_types_df(context: AssetExecutionContext, pokemon_types)->pd.DataFrame:
    """
    """
    pokemon_types_df = pd.json_normalize(pokemon_types)

    context.add_output_metadata(
        metadata={
            "dataframe shape": pokemon_types_df.shape[0],
            "dataframe schema": pokemon_types_df.info()
        },
    )

    return pokemon_types_df

@asset(
    deps=[pokemon_df, pokemon_types_df],
    group_name='pokemon',
    compute_kind='DuckDB'
)
def pokemon_data(context: AssetExecutionContext, duckdb: DuckDBResource):

    with duckdb.get_connection() as duck_conn:
        duck_conn.execute(
            """create or replace table pokemon 
             as (
                    select * from pokemon_df)
            """
        )

        nrows = duck_conn.execute("SELECT COUNT(*) FROM pokemon").fetchone()[0]  # type: ignore

        metadata = duck_conn.execute(
            "select * from duckdb_tables() where table_name = 'pokemon'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )