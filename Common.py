# coding=utf-8

import codecs
import datetime
import json
import math
import os
import pickle
import time
import types
from binascii import b2a_hex, a2b_hex

from Crypto.Cipher import AES

# ============================================
# 全局常量

CONST_APP_EXIT_CODE = 0

# 这里密钥key 长度必须为16（AES-128）、24（AES-192）、或32（AES-256）Bytes 长度.目前AES-128足够用
CONST_CRYPTO_AES_CBC_KEY = "9adunmMw6km6lriO"
CONST_CRYPTO_AES_CBC_KEY_LENGTH = len(CONST_CRYPTO_AES_CBC_KEY)
CONST_CRYPTO_AES_INIT_VECTOR = "0123456789ABCDEF"

CONST_DIR_LOG = "./log"
CONST_DIR_CONF = "./conf"
CONST_DIR_DATABASE = "./db"
CONST_DIR_BACKUP = "./bak"

CONST_CONFIG_HOLIDAY_FILENAME = "holidays.json"
CONST_CONFIG_MAIL_FILENAME = "mail.json"
CONST_CONFIG_ADAPTER_FILENAME = "adapter.json"

CONST_LOG_MAIL_FILENAME = "mail.log"
CONST_LOG_BOX_FILENAME = "box.log"
CONST_LOG_TRADER_FILENAME = "trader.log"

CONST_DB_BOX_FILENAME = "box.db"
CONST_DB_RECORDS_FILENAME = "records.db"
CONST_DB_POSITION_FILENAME = "position.db"

CONST_SH_MARKET = 'SH'
CONST_SZ_MARKET = 'SZ'
CONST_ZX_MARKET = 'ZX'
CONST_CY_MARKET = 'CY'

CONST_K_5M = 0
CONST_K_15M = 1
CONST_K_60M = 3
CONST_K_DAY = 4
CONST_K_LENGTH = 30

CONST_TASK_WAITING_TIME = 300
CONST_DATA_LIST_IS_NULL = 0

CONST_STOCK_TYPE_1 = "type1"
CONST_STOCK_TYPE_2 = "type2"
CONST_STOCK_TYPE_3 = "type3"
CONST_STOCK_TYPE_4 = "type4"

CONST_SH_STOCK_TURNOVER = 12
CONST_SZ_STOCK_TURNOVER = 12
CONST_ZX_STOCK_TURNOVER = 15
CONST_CY_STOCK_TURNOVER = 18

CONST_SELECT_SERVER_INTERVAL = 1
CONST_RETRY_CONNECT_INTERVAL = 15.0
CONST_CONNECT_TIMEOUT = 2500

CONST_STOCK_BUY = 0
CONST_STOCK_SELL = 1
CONST_STOCK_BUY_DESC = u"买入"
CONST_STOCK_SELL_DESC = u"卖出"
CONST_STOCK_ORDER_TYPE = 4  # 市价委托(上海五档即成剩撤/ 深圳五档即成剩撤)

MARKET_NAME_MAPPING = {CONST_ZX_MARKET: u"中小板", CONST_SZ_MARKET: u"深圳主板", CONST_SH_MARKET: u"上海主板",
                       CONST_CY_MARKET: u"创业板"}
MARKET_CODE_MAPPING = {CONST_SH_MARKET: 1, CONST_SZ_MARKET: 0, CONST_ZX_MARKET: 0, CONST_CY_MARKET: 0}
STOCK_TYPE_NAME_MAPPING = {CONST_STOCK_TYPE_1: u"一类", CONST_STOCK_TYPE_2: u"二类", CONST_STOCK_TYPE_3: u"三类",
                           CONST_STOCK_TYPE_4: u"四类"}


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


def list_dir_files(path, list_name):  # 传入存储的list
    for _file in os.listdir(path):
        file_path = os.path.join(path, _file)
        if os.path.isdir(file_path):
            list_dir_files(file_path, list_name)
        else:
            list_name.append(file_path)


def get_current_timestamp():
    return int(time.time())


def get_current_datetime():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def dict_to_file(data, filename):
    with codecs.open(filename, 'wb', 'utf-8') as _file:
        _file.write(get_encrypted_string(pickle.dumps(data)))


def file_to_dict(filename):
    with codecs.open(filename, 'r', 'utf-8') as _file:
        return get_decrypted_string(pickle.loads(_file.read()))


def change_seconds_to_time(total_time):
    day_time_length = 24 * 60 * 60
    hour_time_length = 60 * 60
    minute_time_length = 60
    if total_time < 60:
        return u"%d 秒" % math.ceil(total_time)
    elif total_time > day_time_length:
        days = divmod(total_time, day_time_length)
        return u"%d 日, %s" % (int(days[0]), change_seconds_to_time(days[1]))
    elif total_time > hour_time_length:
        hours = divmod(total_time, hour_time_length)
        return u"%d 时, %s" % (int(hours[0]), change_seconds_to_time(hours[1]))
    else:
        minutes = divmod(total_time, minute_time_length)
    return u"%d 分, %d 秒" % (int(minutes[0]), math.ceil(minutes[1]))


def check_today_is_holiday_time():
    _holiday_config_filename = "%s/%s" % (CONST_DIR_CONF, CONST_CONFIG_HOLIDAY_FILENAME)
    if not file_exist(_holiday_config_filename):
        return False

    try:
        with codecs.open(_holiday_config_filename, 'r', 'utf-8') as _file:
            holidays = json.load(_file).get("holidays", None)
    except Exception:
        return False

    if holidays is None:
        return False

    current_datetime = datetime.date.today()
    weekday_id = current_datetime.isoweekday()
    if isinstance(holidays, types.ListType) and (current_datetime.isoformat() in holidays) and \
            (weekday_id not in [6, 7]):  # 排除周末和国家放假的日期
        return True
    else:
        return False


def get_encrypted_string(plain_text):
    aes_crypto = AES.new(CONST_CRYPTO_AES_CBC_KEY, AES.MODE_CBC, CONST_CRYPTO_AES_INIT_VECTOR)
    count = len(plain_text)
    if count % CONST_CRYPTO_AES_CBC_KEY_LENGTH != 0:
        add = CONST_CRYPTO_AES_CBC_KEY_LENGTH - (count % CONST_CRYPTO_AES_CBC_KEY_LENGTH)
    else:
        add = 0
    plain_text += ('\0' * add)
    return b2a_hex(aes_crypto.encrypt(plain_text))


def get_decrypted_string(secret_text):
    aes_crypto = AES.new(CONST_CRYPTO_AES_CBC_KEY, AES.MODE_CBC, CONST_CRYPTO_AES_INIT_VECTOR)
    plain_text = aes_crypto.decrypt(a2b_hex(secret_text))
    return plain_text.rstrip('\0')


def load_adapter_config(filename):
    if not file_exist(filename):
        return None, u"交易模块配置文件: %s 不存在." % filename
    try:
        with codecs.open(filename, 'r', 'utf-8') as _file:
            return json.load(_file), None
    except Exception as err:
        return None, err.message
