

import logging
import azure.functions as func
from rqtradein_etl import RQTradeInETL

app = func.FunctionApp()

@app.timer_trigger(schedule="*/5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def fnrqtradeintimer(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    from datetime import datetime, timedelta
    today = datetime.now()
    prev_day = today - timedelta(days=1)
    params = {
        "ProviderID": "18",
        "CompanyID": "13325",
        "LocationType": "Company",
        "LocationTypeIDs": "-1",
        "StartDate": prev_day.strftime("%Y-%m-%d"),
        "StopDate": today.strftime("%Y-%m-%d")
    }
    etl = RQTradeInETL()
    result = etl.run_etl(params)
    logging.info("Timer ETL result: %s", result)