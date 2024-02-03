import pandas as pd
import requests
import io
from dagster_gcp import BigQueryResource
from dagster_duckdb import DuckDBResource

from dagster import asset, get_dagster_logger, AssetExecutionContext, MetadataValue

logger = get_dagster_logger()


@asset(group_name="pokemon", compute_kind="Requests")
def pokemon(context: AssetExecutionContext) -> requests.Response:
    """ """
    pokemon_response = requests.get("https://pokeapi.co/api/v2/pokemon?limit=2000")

    context.add_output_metadata(
        metadata={"status": pokemon_response.status_code, "url": pokemon_response.url},
    )

    return pokemon_response


@asset(group_name="pokemon", compute_kind="Requests")
def pokemon_types(context: AssetExecutionContext) -> requests.Response:
    """ """
    response_types = requests.get("https://pokeapi.co/api/v2/type")

    return response_types


@asset(group_name="pokemon", compute_kind="Pandas")
def pokemon_df(
    context: AssetExecutionContext, pokemon: requests.Response
) -> pd.DataFrame:
    """ """
    pokemon_df = pd.json_normalize(pokemon.json()["results"])

    context.add_output_metadata(
        metadata={
            "dataframe shape": list(pokemon_df.shape),
            "preview": MetadataValue.md(pokemon_df.head().to_markdown()),
            "dataframe schema": str(pokemon_df.columns),
        },
    )

    return pokemon_df


@asset(group_name="pokemon", compute_kind="Pandas")
def pokemon_types_df(
    context: AssetExecutionContext, pokemon_types: requests.Response
) -> pd.DataFrame:
    """ """
    pokemon_types_df = pd.json_normalize(pokemon_types.json()["results"])

    context.add_output_metadata(
        metadata={
            "dataframe shape": list(pokemon_types_df.shape),
            "dataframe schema": str(pokemon_types_df.columns),
        },
    )

    return pokemon_types_df


@asset(group_name="pokemon", compute_kind="DuckDB")
def pokemon_data(
    context: AssetExecutionContext,
    pokemon_df: pd.DataFrame,
    duckdb: DuckDBResource,
) -> None:
    pokemon_df = pokemon_df

    logger.info(f"type is {type(pokemon_df)}")
    with duckdb.get_connection() as duck_conn:
        duck_conn.query(
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


@asset(group_name="pokemon", compute_kind="DuckDB")
def pokemon_types_data(
    context: AssetExecutionContext,
    pokemon_types_df: pd.DataFrame,
    duckdb: DuckDBResource,
) -> None:
    pokemon_types_df = pokemon_types_df

    logger.info(f"type is {type(pokemon_types_df)}")
    with duckdb.get_connection() as duck_conn:
        duck_conn.query(
            """create or replace table pokemon_types 
             as (
                    select * from pokemon_types_df)
            """
        )

        nrows = duck_conn.execute("SELECT COUNT(*) FROM pokemon_types").fetchone()[0]  # type: ignore

        metadata = duck_conn.execute(
            "select * from duckdb_tables() where table_name = 'pokemon_types'"
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
