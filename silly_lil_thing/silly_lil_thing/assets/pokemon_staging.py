import pandas as pd
import requests
import io
import asyncio
import aiohttp

from dagster_gcp import BigQueryResource
from dagster_duckdb import DuckDBResource

from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
    AutoMaterializePolicy,
)

from ..helpers import pokemon_helper
from . import pokemon_assets

logger = get_dagster_logger()


@asset(
    group_name="pokemon_staging",
    compute_kind="Requests",
    deps=[pokemon_assets.pokemon_data],
)
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


@asset(
    group_name="pokemon_staging", compute_kind="DuckDB", deps=[pokemon_staging_table]
)
def pokemon_staging_df(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> pd.DataFrame:
    df_pokemon = None

    with duckdb.get_connection() as duck_conn:
        df_pokemon = duck_conn.query(
            """
                    select * from pokemon_staging)
            """
        ).df()

    return df_pokemon


@asset(group_name="pokemon_staging", compute_kind="Pandas")
def pokemon_abilities_df(
    context: AssetExecutionContext, duckdb: DuckDBResource, pokemon_staging_df
) -> pd.DataFrame:
    df_ability = pd.DataFrame()

    for i in range(len(pokemon_staging_df)):
        temp_df = pd.json_normalize(pokemon_staging_df["abilities"].iloc[i])
        temp_df["pokemon_name"] = pokemon_staging_df["name"].iloc[i]
        temp_df["pokemon_id"] = pokemon_staging_df["id"].iloc[i]
        df_ability = pd.concat([df_ability, temp_df], axis=0)

    return df_ability


@asset(group_name="pokemon_staging", compute_kind="DuckDB")
def pokemon_abilities(
    context: AssetExecutionContext,
    pokemon_abilities_df: pd.DataFrame,
    duckdb: DuckDBResource,
) -> None:
    pokemon_abilities_df = pokemon_abilities_df

    logger.info(f"type is {type(pokemon_abilities_df)}")
    with duckdb.get_connection() as duck_conn:
        duck_conn.query(
            """create or replace table pokemon_abilities 
             as (
                    select * from pokemon_abilities_df)
            """
        )

        nrows = duck_conn.execute("SELECT COUNT(*) FROM pokemon_abilities").fetchone()[
            0
        ]  # type: ignore

        metadata = duck_conn.execute(
            "select * from duckdb_tables() where table_name = 'pokemon_abilities'"
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


@asset(group_name="pokemon_staging", compute_kind="Pandas")
def pokemon_game_indices_df(
    context: AssetExecutionContext, duckdb: DuckDBResource, pokemon_staging_df
) -> pd.DataFrame:
    df_game_indices = pd.DataFrame()

    for i in range(len(pokemon_staging_df)):
        temp_df = pd.json_normalize(pokemon_staging_df["game_indices"].iloc[i])
        temp_df["pokemon_name"] = pokemon_staging_df["name"].iloc[i]
        temp_df["pokemon_id"] = pokemon_staging_df["id"].iloc[i]
        df_game_indices = pd.concat([df_game_indices, temp_df], axis=0)

    return df_game_indices


@asset(group_name="pokemon_staging", compute_kind="DuckDB")
def pokemon_game_indices(
    context: AssetExecutionContext,
    pokemon_game_indices_df: pd.DataFrame,
    duckdb: DuckDBResource,
) -> None:
    pokemon_game_indices_df = pokemon_game_indices_df

    logger.info(f"type is {type(pokemon_game_indices_df)}")
    with duckdb.get_connection() as duck_conn:
        duck_conn.query(
            """create or replace table pokemon_game_indices
             as (
                    select * from pokemon_game_indices_df)
            """
        )

        nrows = duck_conn.execute("SELECT COUNT(*) FROM pokemon_game_indices").fetchone()[
            0
        ]  # type: ignore

        metadata = duck_conn.execute(
            "select * from duckdb_tables() where table_name = 'pokemon_game_indices'"
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
