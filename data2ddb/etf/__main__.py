import os
import time
import types
import requests
import pandas as pd
import dolphindb
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

from . import twse


def task(module: types.ModuleType, sess: dolphindb.session):
    response = module.crawler()
    df = module.processor(response)
    module.db_init(df, sess)
    module.db_update(df, sess)


def main():
    DDB_HOST = os.environ.get("DDB_HOST", "localhost")
    DDB_PORT = int(os.environ.get("DDB_PORT", "8848"))
    DDB_USER = os.environ.get("DDB_USER", "admin")
    DDB_PASSWD = os.environ.get("DDB_PASSWD", "123456")

    jobstores = {"default": MemoryJobStore()}
    executors = {
        "default": ThreadPoolExecutor(4),
    }

    scheduler = BackgroundScheduler(
        jobstores=jobstores, executors=executors, timezone=pytz.utc
    )

    sess = dolphindb.session()
    sess.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASSWD)
    scheduler.add_job(task, "interval", args=[twse, sess], seconds=5, id="twse_etf")

    scheduler.start()

    while True:
        time.sleep(300)


if __name__ == "__main__":
    main()
