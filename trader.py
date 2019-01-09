# coding=utf-8

import json

import box
import common
from log import Logger

trader_log_filename = "%s/%s" % (common.CONST_DIR_LOG, common.CONST_LOG_TRADER_FILENAME)
trader_config_filename = "%s/%s" % (common.CONST_DIR_CONF, common.CONST_CONFIG_TRADER_FILENAME)
trader_db_records_filename = "%s/%s" % (common.CONST_DIR_DATABASE, common.CONST_DB_RECORDS_FILENAME)
trader_db_position_filename = "%s/%s" % (common.CONST_DIR_DATABASE, common.CONST_DB_POSITION_FILENAME)
trader_db_box_filename = box.box_db_filename

MAX_VALID_BOX_INTERVAL_HOURS = 4  # 票箱会在每天的早上8：30，和中午12：00 左右开始选取，所以不会有操过


class Trader(object):
    def __init__(self):
        if not common.file_exist(common.CONST_DIR_LOG):
            common.create_directory(common.CONST_DIR_LOG)

        if not common.file_exist(common.CONST_DIR_CONF):
            common.create_directory(common.CONST_DIR_CONF)

        if not common.file_exist(common.CONST_DIR_DATABASE):
            common.create_directory(common.CONST_DIR_DATABASE)

        self.log = Logger(trader_log_filename, level='debug')

    @staticmethod
    def _load_box_db_file():
        if not common.file_exist(trader_db_box_filename):
            return None, "stock box file is not exist."

        try:
            box_data_set = common.file_to_dict(trader_db_box_filename)
        except Exception as err:
            return None, err.message

        box_timestamp = box_data_set.get("timestamp", None)
        box_value = box_data_set.get("value", None)
        if box_timestamp is None or box_value is None:
            return None, "stock box data error, data is null."

        current_timestamp = common.get_current_timestamp()
        if current_timestamp - box_timestamp > MAX_VALID_BOX_INTERVAL_HOURS:
            return None, "stock box data is too old."

        return box_value, None

    @staticmethod
    def _load_position_db_file():
        if not common.file_exist(trader_db_position_filename):
            return None, "position file is not exist."

        try:
            position_data_set = common.file_to_dict(trader_db_position_filename)
            return position_data_set, None
        except Exception as err:
            return None, err.message

    @staticmethod
    def _save_position_db_file(data):
        if common.file_exist(trader_db_position_filename):
            common.delete_file(trader_db_position_filename)

        try:
            common.dict_to_file(data, trader_db_position_filename)
        except Exception as err:
            return err.message

        return None

    @staticmethod
    def _load_trader_config():
        if not common.file_exist(trader_config_filename):
            return None, "config file: %s is not exist."
        try:
            with open(trader_config_filename, "r") as _file:
                return json.load(_file), None
        except Exception as err:
            return None, err.message

    def _send_trader_records_mail(self):
        pass

    # 记录交易记录
    def _save_trader_records(self):
        pass

    # 对加载得BOX进行初始筛选和排序，选择最合适得前几个（默认type1>type2>type3>type4）
    def _box_prepare_filter(self, top=5):
        pass

    # 计算当前持仓的状态，判定持仓的股票不能超过最大的值
    def _position_compute(self):
        pass

    # 扫描当前持仓的股票，如果满足调教就触发交易，并记录交易记录
    def _position_scanner(self):
        pass

    def run(self):
        pass


if __name__ == '__main__':
    pass
