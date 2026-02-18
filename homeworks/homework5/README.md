# Question 1
Answer: .bruin.yml and pipeline/ with pipeline.yml and assets/
See [my module05](/module05/)

# Question 2
Answer: time_interval - incremental based on a time column
See [staging file](/module05/pipeline/assets/staging/trips.sql)

# Question 3
(For this repo the command is bruin run module05/pipeline/pipeline.yml --var 'taxi_types=["yellow"]')

Answer: bruin run --var 'taxi_types=["yellow"]'

# Question 4
(For this repo the command is bruin run module05/pipeline/assets/ingestion/trips.py --downstream)

Answer: bruin run ingestion/trips.py --downstream

# Question 5
columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null

Answer: not_null: true

# Question 6
Answer: bruin lineage

# Question 7
Example for this repo: bruin run my-taxi-pipeline/pipeline/pipeline.yml --start-date 2022-01-01 --end-date 2022-02-01 --full-refresh
Answer: --full-refresh