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
        # Create staging table (all columns as NVARCHAR(255)), add TradeInDateEST, PostTimeEST, ResponseTimeEST
        cursor.execute(
            """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReportStaging' AND TABLE_SCHEMA = 'api')
BEGIN
    CREATE TABLE api.RQTradeinReportStaging (
        SaleInvoiceID NVARCHAR(255),
        TradeInTransactionID NVARCHAR(255),
        InvoiceIDByStore NVARCHAR(255),
        InvoiceID NVARCHAR(255),
        TradeInStatus NVARCHAR(255),
        ItemID NVARCHAR(255),
        ManufacturerModel NVARCHAR(255),
        SerialNumber NVARCHAR(255),
        StoreName NVARCHAR(255),
        RegionName NVARCHAR(255),
        TradeInDate NVARCHAR(255),
        TradeInDateEST NVARCHAR(255),
        PhoneRebateAmount NVARCHAR(255),
        PromotionValue NVARCHAR(255),
        PreDeviceValueAmount NVARCHAR(255),
        PrePromotionValueAmount NVARCHAR(255),
        TrackingNumber NVARCHAR(255),
        OriginalTradeInvoiceID NVARCHAR(255),
        OrderNumber NVARCHAR(255),
        CreditApplicationNum NVARCHAR(255),
        LocationCode NVARCHAR(255),
        MasterOrderNumber NVARCHAR(255),
        SequenceNumber NVARCHAR(255),
        PromoValue NVARCHAR(255),
        OrganicPrice NVARCHAR(255),
        ComputedPrice NVARCHAR(255),
        TradeInMobileNumber NVARCHAR(255),
        SubmissionId NVARCHAR(255),
        TradeInEquipMake NVARCHAR(255),
        TradeInEquipCarrier NVARCHAR(255),
        DeviceSku NVARCHAR(255),
        TradeInDeviceId NVARCHAR(255),
        LobType NVARCHAR(255),
        OrderType NVARCHAR(255),
        PurchaseDeviceId NVARCHAR(255),
        TradeInAmount NVARCHAR(255),
        AmountUsed NVARCHAR(255),
        AmountPending NVARCHAR(255),
        PromoCompletion NVARCHAR(255),
        PostTime NVARCHAR(255),
        PostTimeEST NVARCHAR(255),
        ResponseTime NVARCHAR(255),
        ResponseTimeEST NVARCHAR(255),
        MobileNumber NVARCHAR(255),
        ETLRowInsertedEST DATETIME DEFAULT GETDATE()
    )
END
            """
        )
        # Create target table if not exists
        cursor.execute(
            """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReport' AND TABLE_SCHEMA = 'api')
BEGIN
    CREATE TABLE api.RQTradeinReport (
        SaleInvoiceID NVARCHAR(255),
        TradeInTransactionID NVARCHAR(255),
        InvoiceIDByStore NVARCHAR(255),
        InvoiceID NVARCHAR(255),
        TradeInStatus NVARCHAR(255),
        ItemID NVARCHAR(255),
        ManufacturerModel NVARCHAR(255),
        SerialNumber NVARCHAR(255),
        StoreName NVARCHAR(255),
        RegionName NVARCHAR(255),
        TradeInDate NVARCHAR(255),
        TradeInDateEST NVARCHAR(255),
        PhoneRebateAmount NVARCHAR(255),
        PromotionValue NVARCHAR(255),
        PreDeviceValueAmount NVARCHAR(255),
        PrePromotionValueAmount NVARCHAR(255),
        TrackingNumber NVARCHAR(255),
        OriginalTradeInvoiceID NVARCHAR(255),
        OrderNumber NVARCHAR(255),
        CreditApplicationNum NVARCHAR(255),
        LocationCode NVARCHAR(255),
        MasterOrderNumber NVARCHAR(255),
        SequenceNumber NVARCHAR(255),
        PromoValue NVARCHAR(255),
        OrganicPrice NVARCHAR(255),
        ComputedPrice NVARCHAR(255),
        TradeInMobileNumber NVARCHAR(255),
        SubmissionId NVARCHAR(255),
        TradeInEquipMake NVARCHAR(255),
        TradeInEquipCarrier NVARCHAR(255),
        DeviceSku NVARCHAR(255),
        TradeInDeviceId NVARCHAR(255),
        LobType NVARCHAR(255),
        OrderType NVARCHAR(255),
        PurchaseDeviceId NVARCHAR(255),
        TradeInAmount NVARCHAR(255),
        AmountUsed NVARCHAR(255),
        AmountPending NVARCHAR(255),
        PromoCompletion NVARCHAR(255),
        PostTime NVARCHAR(255),
        PostTimeEST NVARCHAR(255),
        ResponseTime NVARCHAR(255),
        ResponseTimeEST NVARCHAR(255),
        MobileNumber NVARCHAR(255),
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
            # Clean PostTime and ResponseTime columns to 'yyyy-MM-dd HH:mm:ss.ffff' format
            cursor.execute("UPDATE api.RQTradeinReport SET PostTime = CASE WHEN TRY_CONVERT(datetime2(4), REPLACE(REPLACE(PostTime, 'T', ' '), 'Z', '')) IS NOT NULL THEN FORMAT(TRY_CONVERT(datetime2(4), REPLACE(REPLACE(PostTime, 'T', ' '), 'Z', '')), 'yyyy-MM-dd HH:mm:ss.ffff') ELSE PostTime END, ResponseTime = CASE WHEN TRY_CONVERT(datetime2(4), REPLACE(REPLACE(ResponseTime, 'T', ' '), 'Z', '')) IS NOT NULL THEN FORMAT(TRY_CONVERT(datetime2(4), REPLACE(REPLACE(ResponseTime, 'T', ' '), 'Z', '')), 'yyyy-MM-dd HH:mm:ss.ffff') ELSE ResponseTime END")
            conn.commit()
            # Get counts
            inserted = cursor.execute("SELECT COUNT(*) FROM api.RQTradeinReport WHERE ETLRowInsertedEST = CONVERT(date, GETDATE())").fetchval()
            updated = cursor.execute("SELECT COUNT(*) FROM api.RQTradeinReport WHERE ETLRowUpdatedEST = CONVERT(date, GETDATE())").fetchval()
            logging.info(f"Merge complete. Inserted: {inserted}, Updated: {updated}")
            # Delete all records from staging table except those with today's date
            cursor.execute("DELETE FROM api.RQTradeinReportStaging WHERE CONVERT(date, TradeInDate) <> CONVERT(date, GETDATE())")
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            logging.error(f"Error in merge_to_target: {e}")
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
        logging.info(f"Fetching API data for {params['StartDate']} to {params['StopDate']}...")
        response = requests.get(url, headers=headers)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logging.error(f"API request failed: {e}. Response: {response.text}")
            raise
        data = response.json()
        logging.info(f"Fetched {len(data)} records from API.")
        try:
            logging.info("API Data Sample: %s", json.dumps(data[:5], indent=2))
        except Exception as e:
            logging.info("API Data Sample logging failed: %s", e)
        return data
    def load_staging_data(self, conn, data):
        cursor = conn.cursor()
        # Prepare SQL for bulk insert
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
        values_list = [[row.get(col, None) for col in columns] for row in data]
        cursor.executemany(sql, values_list)
