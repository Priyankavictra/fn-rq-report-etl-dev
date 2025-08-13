# RQ Trade-In Report ETL Azure Function

## Overview
This project is an Azure Function App for automating the ETL (Extract, Transform, Load) process of Verizon trade-in activity reports. It fetches data from an external API, processes and converts date/time fields to both UTC and EST, and loads the results into a SQL Server database.

## Folder Structure
- `rqtradein_etl.py` — Main ETL logic (API fetch, transformation, DB load)
- `rq_tradein_report/function_app.py` — Azure Function entry point (timer trigger)
- `rq_tradein_report/function.json` — Azure Function configuration
- `requirements.txt` — Python dependencies
- `local.settings.json` — Local development settings (not used in production)

## Key Features
- Timer-triggered ETL (runs every 5 minutes by default)
- Converts `PostTime` and `ResponseTime` to both UTC (ISO) and EST
- Handles table creation and schema migration
- Logs ETL results and errors

## Environment Variables
Set these in Azure Function App Configuration:
- `CONNECTION_STRING` — SQL Server connection string
- `APPLICATIONINSIGHTS_CONNECTION_STRING` — (optional) for telemetry

## How to Deploy
1. Zip and deploy the project to Azure Functions (Python runtime).
2. Set required environment variables in Azure Portal.
3. Ensure ODBC Driver 17 for SQL Server is available.

## How to Run Locally
1. Install dependencies: `pip install -r requirements.txt`
2. Set up `local.settings.json` with your connection string.
3. Start the function host: `func host start`

## Output
- Database tables: `api.RQTradeinReportStaging`, `api.RQTradeinReport`
- Columns: UTC and EST versions of time fields

## Support
For issues, contact the project maintainer or open an issue in your repository.
