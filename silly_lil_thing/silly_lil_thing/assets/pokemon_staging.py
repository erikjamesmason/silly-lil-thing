import pandas as pd
import requests
import io
import asyncio
import aiohttp

from dagster_gcp import BigQueryResource
from dagster_duckdb import DuckDBResource

from dagster import asset, get_dagster_logger, AssetExecutionContext, MetadataValue

from ..helpers import pokemon_helper

logger = get_dagster_logger()


@asset(group_name="pokemon_staging", compute_kind="Requests")
async def pokemon_staging_data(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    with duckdb.get_connection() as duck_conn:
        pokemon_names_list = duck_conn.execute("select name from pokemon;").fetchall()

    async with aiohttp.ClientSession() as session:
        tasks = [
            pokemon_helper.fetch_pokemon(session, pokemon_name[0])
            for pokemon_name in pokemon_names_list
        ]
        pokemon_responses = await asyncio.gather(*tasks)

        df_async_pokemon_response = pd.json_normalize(pokemon_responses)

        return df_async_pokemon_response
    
@asset(group_name="pokemon_staging", compute_kind="DuckDB")
def pokemon_staging_table(
    context: AssetExecutionContext,
    pokemon_staging_data: pd.DataFrame,
    duckdb: DuckDBResource,
) -> None:
    pokemon_staging_data = pokemon_staging_data

    logger.info(f"type is {type(pokemon_staging_data)}")
    with duckdb.get_connection() as duck_conn:
        duck_conn.query(
            """create or replace table pokemon_staging 
             as (
                    select * from pokemon_staging_data)
            """
        )

        nrows = duck_conn.execute("SELECT COUNT(*) FROM pokemon_staging").fetchone()[0]  # type: ignore

        metadata = duck_conn.execute(
            "select * from duckdb_tables() where table_name = 'pokemon_staging'"
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
    

