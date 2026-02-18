"""`dlt` pipeline to ingest NYC taxi data from the Zoomcamp REST API."""

import dlt
import requests


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net"


@dlt.resource(name="nyc_taxi_trips")
def nyc_taxi_trips():
    """Yield NYC taxi trips, paging until the API returns an empty page."""
    page = 1
    while True:
        response = requests.get(
            f"{BASE_URL}/data_engineering_zoomcamp_api",
            params={"page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        # Stop when the API returns an empty page
        if not data:
            break

        for row in data:
            yield row

        page += 1


@dlt.source
def nyc_taxi_rest_api_source():
    """Define dlt source composed of the NYC taxi trips resource."""
    return nyc_taxi_trips()


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    # `refresh="drop_sources"` clears data and state on each run; remove once stable.
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)  # noqa: T201

