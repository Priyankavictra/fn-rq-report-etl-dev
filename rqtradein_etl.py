
"""
RQTradeInETL: ETL process for Trade-In Activity Report
Handles API fetch, staging/target table creation, timezone conversion, and merge logic.
"""
import requests
import pyodbc
import logging
import os
import json

class RQTradeInETL:
    def run_etl(self, params):
        try:
            conn = self.get_sql_connection()
            self.create_tables_if_not_exist(conn)
            data = self.fetch_api_data(params)
            self.load_staging_data(conn, data)
            result = self.merge_to_target(conn)
            conn.close()
            return result
        except Exception as e:
            logging.error("ETL process failed: %s", e)
            raise
    def create_tables_if_not_exist(self, conn):
        cursor = conn.cursor()
        # Handle migration from old table name to new table name
        cursor.execute(
            """
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReport_staging' AND TABLE_SCHEMA = 'api')
AND NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReportStaging' AND TABLE_SCHEMA = 'api')
BEGIN
    EXEC sp_rename 'api.RQTradeinReport_staging', 'RQTradeinReportStaging'
END
            """
        )
        conn.commit()
        # Create staging table (all columns as VARCHAR(255)), add TradeInDateEST, PostTimeEST, ResponseTimeEST
        cursor.execute(
            """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReportStaging' AND TABLE_SCHEMA = 'api')
BEGIN
    CREATE TABLE api.RQTradeinReportStaging (
        SaleInvoiceID VARCHAR(255),
        TradeInTransactionID VARCHAR(255),
        InvoiceIDByStore VARCHAR(255),
        InvoiceID VARCHAR(255),
        TradeInStatus VARCHAR(255),
        ItemID VARCHAR(255),
        ManufacturerModel VARCHAR(255),
        SerialNumber VARCHAR(255),
        StoreName VARCHAR(255),
        RegionName VARCHAR(255),
        TradeInDate VARCHAR(255),
        TradeInDateEST VARCHAR(255),
        PhoneRebateAmount VARCHAR(255),
        PromotionValue VARCHAR(255),
        PreDeviceValueAmount VARCHAR(255),
        PrePromotionValueAmount VARCHAR(255),
        TrackingNumber VARCHAR(255),
        OriginalTradeInvoiceID VARCHAR(255),
        OrderNumber VARCHAR(255),
        CreditApplicationNum VARCHAR(255),
        LocationCode VARCHAR(255),
        MasterOrderNumber VARCHAR(255),
        SequenceNumber VARCHAR(255),
        PromoValue VARCHAR(255),
        OrganicPrice VARCHAR(255),
        ComputedPrice VARCHAR(255),
        TradeInMobileNumber VARCHAR(255),
        SubmissionId VARCHAR(255),
        TradeInEquipMake VARCHAR(255),
        TradeInEquipCarrier VARCHAR(255),
        DeviceSku VARCHAR(255),
        TradeInDeviceId VARCHAR(255),
        LobType VARCHAR(255),
        OrderType VARCHAR(255),
        PurchaseDeviceId VARCHAR(255),
        TradeInAmount VARCHAR(255),
        AmountUsed VARCHAR(255),
        AmountPending VARCHAR(255),
        PromoCompletion VARCHAR(255),
        PostTime VARCHAR(255),
        PostTimeEST VARCHAR(255),
        ResponseTime VARCHAR(255),
        ResponseTimeEST VARCHAR(255),
        MobileNumber VARCHAR(255),
        ETLRowInsertedEST DATETIME DEFAULT GETDATE()
    )
END
            """
        )
        # Create target table if not exists, using appropriate datatypes
        cursor.execute(
            """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReport' AND TABLE_SCHEMA = 'api')
BEGIN
    CREATE TABLE api.RQTradeinReport (
        SaleInvoiceID INT,
        TradeInTransactionID INT,
        InvoiceIDByStore VARCHAR(50),
        InvoiceID VARCHAR(50),
        TradeInStatus VARCHAR(50),
        ItemID INT,
        ManufacturerModel VARCHAR(100),
        SerialNumber VARCHAR(100),
        StoreName VARCHAR(100),
        RegionName VARCHAR(100),
        TradeInDate DATETIME,
        TradeInDateEST DATETIME,
        PhoneRebateAmount DECIMAL(18,2),
        PromotionValue DECIMAL(18,2),
        PreDeviceValueAmount DECIMAL(18,2),
        PrePromotionValueAmount DECIMAL(18,2),
        TrackingNumber VARCHAR(100),
        OriginalTradeInvoiceID VARCHAR(50),
        OrderNumber VARCHAR(50),
        CreditApplicationNum VARCHAR(50),
        LocationCode VARCHAR(50),
        MasterOrderNumber VARCHAR(50),
        SequenceNumber INT,
        PromoValue DECIMAL(18,2),
        OrganicPrice DECIMAL(18,2),
        ComputedPrice DECIMAL(18,2),
        TradeInMobileNumber VARCHAR(20),
        SubmissionId VARCHAR(50),
        TradeInEquipMake VARCHAR(50),
        TradeInEquipCarrier VARCHAR(50),
        DeviceSku VARCHAR(50),
        TradeInDeviceId VARCHAR(50),
        LobType VARCHAR(50),
        OrderType VARCHAR(50),
        PurchaseDeviceId VARCHAR(50),
        TradeInAmount DECIMAL(18,2),
        AmountUsed DECIMAL(18,2),
        AmountPending DECIMAL(18,2),
        PromoCompletion VARCHAR(50),
        PostTime DATETIME,
        PostTimeEST DATETIME,
        ResponseTime DATETIME,
        ResponseTimeEST DATETIME,
        MobileNumber VARCHAR(20),
        ETLRowInsertedEST DATETIME DEFAULT GETDATE(),
        ETLRowUpdatedEST DATETIME
    )
END
            """
        )
        conn.commit()
    def merge_to_target(self, conn):
        cursor = conn.cursor()
        try:
            merge_columns = [
                'SaleInvoiceID', 'TradeInTransactionID', 'InvoiceIDByStore', 'InvoiceID', 'TradeInStatus', 'ItemID', 'ManufacturerModel', 'SerialNumber', 'StoreName', 'RegionName', 'TradeInDate',
                'TradeInDateEST', 'PostTime', 'PostTimeEST', 'ResponseTime', 'ResponseTimeEST',
                'PhoneRebateAmount', 'PromotionValue', 'PreDeviceValueAmount', 'PrePromotionValueAmount', 'TrackingNumber', 'OriginalTradeInvoiceID', 'OrderNumber', 'CreditApplicationNum',
                'LocationCode', 'MasterOrderNumber', 'SequenceNumber', 'PromoValue', 'OrganicPrice', 'ComputedPrice', 'TradeInMobileNumber', 'SubmissionId', 'TradeInEquipMake', 'TradeInEquipCarrier',
                'DeviceSku', 'TradeInDeviceId', 'LobType', 'OrderType', 'PurchaseDeviceId', 'TradeInAmount', 'AmountUsed', 'AmountPending', 'PromoCompletion', 'MobileNumber'
            ]
            update_assignments = [f"target.{col} = source.{col}" for col in merge_columns]
            update_assignments.append("target.ETLRowUpdatedEST = GETDATE()")
            update_set = ',\n                '.join(update_assignments)
            insert_cols = ', '.join(merge_columns + ['ETLRowInsertedEST'])
            insert_vals = ', '.join([f"source.{col}" for col in merge_columns] + ['GETDATE()'])
            merge_sql = f"""
WITH DedupedSource AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY SaleInvoiceID ORDER BY TradeInDate DESC) AS rn
FROM api.RQTradeinReportStaging
)
MERGE api.RQTradeinReport AS target
USING (SELECT * FROM DedupedSource WHERE rn = 1) AS source
ON target.SaleInvoiceID = source.SaleInvoiceID
WHEN MATCHED THEN
UPDATE SET
{update_set}
WHEN NOT MATCHED THEN
INSERT (
{insert_cols}
)
VALUES (
{insert_vals}
);
"""
            cursor.execute(merge_sql)
            conn.commit()
            # Removed problematic SQL update for PostTime/ResponseTime format
            # Get counts
            inserted = cursor.execute("SELECT COUNT(*) FROM api.RQTradeinReport WHERE ETLRowInsertedEST = CONVERT(date, GETDATE())").fetchval()
            updated = cursor.execute("SELECT COUNT(*) FROM api.RQTradeinReport WHERE ETLRowUpdatedEST = CONVERT(date, GETDATE())").fetchval()
            logging.info("Merge complete. Inserted: %s, Updated: %s", inserted, updated)
            # Delete all records from staging table except those with today's date
            cursor.execute("DELETE FROM api.RQTradeinReportStaging WHERE CONVERT(date, TradeInDate) <> CONVERT(date, GETDATE())")
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            logging.error("Error in merge_to_target: %s", e)
            raise
    def get_sql_connection(self):
        conn_str = os.environ.get("CONNECTION_STRING")
        if not conn_str:
            raise ValueError("CONNECTION_STRING environment variable not set.")
        return pyodbc.connect(conn_str)
    def fetch_api_data(self, params):
        # Use params['StartDate'] and params['StopDate'] dynamically
        url = f"https://dataconnect.iqmetrix.net/Reports/TradeInActivityReport?ProviderID=18&StartDate={params['StartDate']}&StopDate={params['StopDate']}&LocationType=Company&LocationTypeIDs=-1&ComplanyID=13325"
        headers = {
            "Authorization": "Basic cG90YXRvLmV0bEB2aWN0cmEuY29tOjxuUVhmNSwjIXxoZj84T29hTUJe",
            "Cookie": "ApplicationGatewayAffinity=36f3760ed0711848f486da1adf16ec68; ApplicationGatewayAffinityCORS=36f3760ed0711848f486da1adf16ec68; DataConnectState=pnyjjmvt2zkwjkbtn42iteui"
        }
        logging.info("Fetching API data for %s to %s...", params['StartDate'], params['StopDate'])
        max_retries = 3
        backoff = 5
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=120)
                response.raise_for_status()
                data = response.json()
                logging.info("Fetched %d records from API.", len(data))
                logging.info("API Data Sample: %s", json.dumps(data[:5], indent=2))
                return data
            except requests.RequestException as e:
                logging.warning("API request failed (attempt %d/%d): %s", attempt, max_retries, e)
                if attempt == max_retries:
                    logging.error("API request failed after %d attempts: %s", max_retries, e)
                    raise
                import time
                time.sleep(backoff * attempt)

    def load_staging_data(self, conn, data):
        cursor = conn.cursor()
        from datetime import datetime
        import pytz
        columns = [
            'SaleInvoiceID', 'TradeInTransactionID', 'InvoiceIDByStore', 'InvoiceID', 'TradeInStatus', 'ItemID', 'ManufacturerModel', 'SerialNumber', 'StoreName', 'RegionName', 'TradeInDate',
            'TradeInDateEST', 'PhoneRebateAmount', 'PromotionValue', 'PreDeviceValueAmount', 'PrePromotionValueAmount', 'TrackingNumber', 'OriginalTradeInvoiceID', 'OrderNumber', 'CreditApplicationNum',
            'LocationCode', 'MasterOrderNumber', 'SequenceNumber', 'PromoValue', 'OrganicPrice', 'ComputedPrice', 'TradeInMobileNumber', 'SubmissionId', 'TradeInEquipMake', 'TradeInEquipCarrier',
            'DeviceSku', 'TradeInDeviceId', 'LobType', 'OrderType', 'PurchaseDeviceId', 'TradeInAmount', 'AmountUsed', 'AmountPending', 'PromoCompletion', 'PostTime', 'PostTimeEST', 'ResponseTime', 'ResponseTimeEST', 'MobileNumber'
        ]
        placeholders = ', '.join(['?'] * len(columns))
        sql = f"""
        INSERT INTO api.RQTradeinReportStaging (
            {', '.join(columns)}
        ) VALUES ({placeholders})
        """
        # Bulk insert data
        cursor.fast_executemany = True
        est = pytz.timezone('US/Eastern')
        import re
        def truncate_microseconds(dt_str):
            # Handles any number of microsecond digits, pads/truncates to 6, preserves trailing Z
            if not isinstance(dt_str, str):
                return dt_str
            match = re.match(r"(.*?\.)(\d+)(Z?)$", dt_str)
            if match:
                prefix, micro, z = match.groups()
                micro = (micro + '000000')[:6]  # Pad with zeros, then truncate to 6 digits
                return f"{prefix}{micro}{z}"
            return dt_str

        def to_datetime(dt_str, field_name=None):
            if not dt_str:
                return None
            dt_str = truncate_microseconds(dt_str)
            dt = None
            formats = [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(dt_str.replace('Z', ''), fmt)
                    return dt
                except ValueError:
                    continue
            try:
                dt = datetime.fromisoformat(dt_str.replace('Z', ''))
                return dt
            except ValueError:
                if field_name:
                    logging.warning("Could not parse datetime for field %s: %s", field_name, dt_str)
                return None

        def to_est(dt_str):
            if not dt_str:
                return None
            dt_str = truncate_microseconds(dt_str)
            dt = None
            formats = [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(dt_str.replace('Z', ''), fmt)
                    break
                except ValueError:
                    continue
            if dt is None:
                try:
                    dt = datetime.fromisoformat(dt_str.replace('Z', ''))
                except ValueError:
                    return None
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            return dt.astimezone(est).strftime("%Y-%m-%d %H:%M:%S")
        values_list = []
        for row in data:
            row = dict(row)
            # Convert all date fields to Python datetime objects, log if conversion fails
            for field in ['TradeInDate', 'PostTime', 'ResponseTime']:
                val = row.get(field)
                dt = to_datetime(val, field)
                if dt is None and val:
                    # If API value is present but conversion failed, log and use a fallback (current time)
                    logging.warning("API value for %s was not a valid datetime: %s. Using current time.", field, val)
                    dt = datetime.now()
                row[field] = dt
            # EST fields
            for field, src in [('TradeInDateEST', 'TradeInDate'), ('PostTimeEST', 'PostTime'), ('ResponseTimeEST', 'ResponseTime')]:
                src_val = row.get(src)
                # Always pass a string to to_est
                if isinstance(src_val, (str, type(None))):
                    est_val = to_est(src_val)
                else:
                    # Convert datetime to ISO string
                    est_val = to_est(src_val.isoformat())
                est_dt = to_datetime(est_val, field)
                if est_dt is None and est_val:
                    logging.warning("EST value for %s was not a valid datetime: %s. Using current time.", field, est_val)
                    est_dt = datetime.now()
                row[field] = est_dt
            values_list.append([row.get(col, None) for col in columns])
        cursor.executemany(sql, values_list)
