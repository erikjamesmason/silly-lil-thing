import pandas as pd
import requests 
import io 
from dagster_gcp import BigQueryResource

from dagster import asset


@asset
def iris_data(bigquery: BigQueryResource) -> None:

    url = "https://docs.dagster.io/assets/iris.csv"

    request = requests.get(url).content
    iris_df = pd.read_csv(io.StringIO(request.decode('utf-8')),
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],)

    with bigquery.get_client() as client:
        job = client.load_table_from_dataframe(
            dataframe=iris_df,
            destination="iris.iris_data",
        )
        job.result()

