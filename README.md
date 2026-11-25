# Weather ETL Pipeline

An Apache Airflow pipeline that pulls hourly weather data for several cities from the
Open-Meteo API, cleans and validates it, and loads daily summaries into SQLite.

## What it does
1. Creates the target table if needed.
2. Extracts hourly temperature and precipitation per city (one mapped task per city).
3. Transforms each city's data: removes duplicate hours, drops readings with no
   temperature, and computes daily avg/min/max temperature and total precipitation.
4. Validates all cities (correct day, enough hourly readings, plausible temperatures,
   no duplicate cities).
5. Loads the rows with delete-then-insert in a single transaction.
6. Removes raw files after a successful run.

## Architecture
```
create_table (setup) -> list_cities -> extract[city] -> transform[city]
                                              -> validate -> load -> summarize
                                                                  -> cleanup_files (teardown)
```

## Airflow concepts used
| Concept | Where |
|---|---|
| TaskFlow API and XComs | All tasks pass small metadata (file paths, counts), never data frames |
| Data interval scheduling | `CronDataIntervalTimetable`, every date comes from `data_interval_start` |
| Dynamic task mapping | `extract` and `transform` expand over the city list |
| Retries and backoff | `default_args` with exponential backoff and timeouts |
| Idempotent load | Delete and insert in one transaction, files named by city and date |
| Connections and hooks | `HttpHook` and `SqliteHook`, no credentials in code |
| Setup/teardown | Table creation and raw file cleanup |
| Concurrency control | `max_active_tis_per_dag` on the API task |

## Setup
1. Install Docker and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).
2. Clone the repo and copy the environment file:
   ```bash
   cp .env.example .env
   ```
3. Start Airflow:
   ```bash
   astro dev start
   ```
4. Open http://localhost:8080.
5. If you did not define the API connection in `.env`, create it in the UI
   (Admin -> Connections): ID `open_meteo_api`, type HTTP, host `https://api.open-meteo.com`.
6. Create the data folders inside the container:
   ```bash
   astro dev bash
   mkdir -p /tmp/etl/raw /tmp/etl/clean
   ```

## Running it
- Unpause `weather_etl`, then use **Trigger -> Backfill** for a few recent days.
- Check the results:
  ```bash
  astro dev bash
  sqlite3 /tmp/warehouse.db "SELECT * FROM weather_daily ORDER BY day, city;"
  ```
- Rerunning the same days leaves the row count unchanged (idempotent).

## Design decisions
- **Idempotent:** raw/clean files are named by city and date, and the load replaces the day in one transaction, so reruns and backfills are safe.
- **Data interval, not `now()`:** a backfilled run processes its own day.
- **Small XComs:** only file paths and counts travel between tasks.
- **Validation before load:** bad or incomplete data fails the run instead of reaching the table.
- **Per-city mapping:** one failed city is retried alone and does not stop the others.
- **Missing values:** readings without a temperature are dropped, and missing precipitation is treated as 0 (an explicit assumption).

## Known limitations
- Local files and SQLite are for learning. Production would use shared storage (S3) and Postgres.
- The Open-Meteo endpoint serves limited history, so backfills only work for recent dates.
- Alerts only appear in the Airflow logs.
- `/tmp` data disappears when the container is rebuilt.
