"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: float
  - name: taxi_type
    type: string
  - name: payment_type
    type: string
@bruin"""

import os
import json
import pandas as pd


def generate_month_range(start_date: str, end_date: str):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append((current.year, current.month))
        current = (current + pd.DateOffset(months=1)).replace(day=1)
    return months


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    months = generate_month_range(start_date, end_date)
    all_data = []

    for taxi_type in taxi_types:
        for year, month in months:
            month_str = f"{month:02d}"
            url = (
                f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
            )
            try:
                df = pd.read_parquet(url)

                # Normalize pickup/dropoff datetimes
                if "tpep_pickup_datetime" in df.columns:
                    df["pickup_datetime"] = df["tpep_pickup_datetime"]
                    df["dropoff_datetime"] = df["tpep_dropoff_datetime"]
                elif "lpep_pickup_datetime" in df.columns:
                    df["pickup_datetime"] = df["lpep_pickup_datetime"]
                    df["dropoff_datetime"] = df["lpep_dropoff_datetime"]

                # Normalize location IDs
                if "PULocationID" in df.columns:
                    df["pickup_location_id"] = df["PULocationID"]
                    df["dropoff_location_id"] = df["DOLocationID"]
                else:
                    df["pickup_location_id"] = None
                    df["dropoff_location_id"] = None

                # Normalize fare_amount
                if "fare_amount" not in df.columns:
                    df["fare_amount"] = None

                # Taxi type
                df["taxi_type"] = taxi_type

                # Payment type
                if "payment_type" in df.columns:
                    df["payment_type"] = df["payment_type"].astype(str)
                else:
                    df["payment_type"] = None

                # Keep only required columns
                df = df[
                    [
                        "pickup_datetime",
                        "dropoff_datetime",
                        "pickup_location_id",
                        "dropoff_location_id",
                        "fare_amount",
                        "taxi_type",
                        "payment_type",
                    ]
                ]

                all_data.append(df)
                print(f"Loaded {taxi_type} {year}-{month_str}")

            except Exception as e:
                print(f"Skipping {taxi_type} {year}-{month_str}: {e}")

    if not all_data:
        return pd.DataFrame(
            columns=[
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "fare_amount",
                "taxi_type",
                "payment_type",
            ]
        )

    final_dataframe = pd.concat(all_data, ignore_index=True)
    return final_dataframe
