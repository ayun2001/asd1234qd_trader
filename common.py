# coding=utf-8

import os
import types

# ============================================
# 全局常量
CONST_DIR_LOG = "./log"
CONST_DIR_CONF = "./conf"
CONST_DIR_DATABASE = "./db"

CONST_CONFIG_MAIL_FILENAME = "mail.conf"

CONST_LOG_MAIL_FILENAME = "mail.log"
CONST_LOG_TA_FILENAME = "ta.log"
CONST_LOG_BOX_FILENAME = "box.log"

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
