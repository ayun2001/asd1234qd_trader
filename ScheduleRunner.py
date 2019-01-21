# coding=utf-8

import time

import schedule

import Common
from Log import Logger

CONST_PENDING_IDLE_TIME = 3
CONST_LOG_SCHEDULE_FILENAME = "schedule.log"

schedule_log_filename = "%s/%s" % (Common.CONST_DIR_LOG, CONST_LOG_SCHEDULE_FILENAME)

_log = Logger(schedule_log_filename, level='debug')


# 定义定时任务的工作函数
# ===============================================
def job():
    print "hello~!!!"
    _log.logger.info("hello~!!!")


# 定时任务加入定时任务组
# ===============================================
schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(CONST_PENDING_IDLE_TIME)
