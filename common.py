# coding=utf-8

import json
import math
import os
import pickle
import time
import types

# ============================================
# 全局常量
CONST_DIR_LOG = "./log"
CONST_DIR_CONF = "./conf"
CONST_DIR_DATABASE = "./db"

CONST_CONFIG_HOLIDAY_FILENAME = "holidays.json"
CONST_CONFIG_MAIL_FILENAME = "mail.json"
CONST_CONFIG_TRADER_FILENAME = "trader.json"

CONST_LOG_MAIL_FILENAME = "mail.log"
CONST_LOG_TA_FILENAME = "ta.log"
CONST_LOG_BOX_FILENAME = "box.log"
CONST_LOG_TRADER_FILENAME = "trader.log"

CONST_DB_BOX_FILENAME = "box.db"

CONST_SH_MARKET = 'SH'
CONST_SZ_MARKET = 'SZ'
CONST_ZX_MARKET = 'ZX'
CONST_CY_MARKET = 'CY'

CONST_K_5M = 0
CONST_K_15M = 1
CONST_K_60M = 3
CONST_K_DAY = 4
CONST_K_LENGTH = 30

CONST_RETRY_COUNT = 5
CONST_RETRY_INTERVAL = 300

CONST_TASK_WAITING_MS = 100
CONST_STOCK_LIST_IS_NULL = 0

CONST_STOCK_TYPE_1 = 'type1'
CONST_STOCK_TYPE_2 = 'type2'
CONST_STOCK_TYPE_3 = 'type3'
CONST_STOCK_TYPE_4 = 'type4'

TDX_HQ_SERVER_LIST = [
    "119.147.212.81:7709",
    # "221.231.141.60:7709",
    # "101.227.73.20:7709",
    "14.215.128.18:7709",
    "59.173.18.140:7709",
]

TDX_ORDER_SERVER_LIST = [

]

MARKET_NAME_MAPPING = {CONST_ZX_MARKET: "中小板", CONST_SZ_MARKET: "深圳主板", CONST_SH_MARKET: "上海主板",
                       CONST_CY_MARKET: "创业板"}
MARKET_CODE_MAPPING = {CONST_SH_MARKET: 1, CONST_SZ_MARKET: 0, CONST_ZX_MARKET: 0, CONST_CY_MARKET: 0}
STOCK_TYPE_NAME_MAPPING = {CONST_STOCK_TYPE_1: "一类", CONST_STOCK_TYPE_2: "二类", CONST_STOCK_TYPE_3: "三类",
                           CONST_STOCK_TYPE_4: "四类"}


# ============================================
# 全局函数
def trim(string):
    if not isinstance(string, types.StringTypes):
        return string
    else:
        return string.strip()


def file_exist(filename):
    if not isinstance(filename, types.StringTypes):
        return None
    return os.path.exists(trim(filename))


def create_directory(path):
    if not isinstance(path, types.StringTypes):
        return False
    else:
        os.makedirs(path, 0o755)
        return True


def delete_file(filename):
    if not isinstance(filename, types.StringTypes):
        return False
    else:
        result = file_exist(filename)
        if result is not None and result:
            os.remove(filename)
            return True
        else:
            return False


def get_current_timestamp():
    return int(time.time())


def dict_to_file(data, filename):
    with open(filename, 'wb') as _f:
        pickle.dump(data, _f)


def file_to_dict(filename):
    with open(filename) as _f:
        return pickle.load(_f)


def change_seconds_to_time(total_time):
    day_time_length = 24 * 60 * 60
    hour_time_length = 60 * 60
    min_time_length = 60
    if total_time < 60:
        return "%d seconds" % math.ceil(total_time)
    elif total_time > day_time_length:
        days = divmod(total_time, day_time_length)
        return "%d days, %s" % (int(days[0]), change_seconds_to_time(days[1]))
    elif total_time > hour_time_length:
        hours = divmod(total_time, hour_time_length)
        return '%d hours, %s' % (int(hours[0]), change_seconds_to_time(hours[1]))
    else:
        mins = divmod(total_time, min_time_length)
    return "%d minutes, %d seconds" % (int(mins[0]), math.ceil(mins[1]))


def check_today_is_holiday_time():
    _holiday_config_filename = "%s/%s" % (CONST_DIR_CONF, CONST_CONFIG_HOLIDAY_FILENAME)
    if not file_exist(_holiday_config_filename):
        return False

    try:
        with open(_holiday_config_filename, "r") as _file:
            holidays = json.load(_file).get("holidays", None)
    except Exception:
        return False

    if holidays is None:
        return False

    current_datetime = time.strftime('%Y%m%d', time.localtime(time.time()))
    if isinstance(current_datetime, types.ListType) and (current_datetime in holidays):
        return True
    else:
        return False
