# coding=utf-8

import box
import common
from log import Logger

trader_log_filename = "%s/%s" % (common.CONST_DIR_LOG, common.CONST_LOG_TRADER_FILENAME)
trader_config_filename = "%s/%s" % (common.CONST_DIR_CONF, common.CONST_CONFIG_TRADER_FILENAME)
trader_db_records_filename = "%s/%s" % (common.CONST_DIR_DATABASE, common.CONST_DB_TRADER_FILENAME)
trader_db_box_filename = box.box_db_filename


class Trader(object):
    def __init__(self):
        if not common.file_exist(common.CONST_DIR_LOG):
            common.create_directory(common.CONST_DIR_LOG)

        if not common.file_exist(common.CONST_DIR_CONF):
            common.create_directory(common.CONST_DIR_CONF)

        if not common.file_exist(common.CONST_DIR_DATABASE):
            common.create_directory(common.CONST_DIR_DATABASE)

        self.log = Logger(trader_log_filename, level='debug')

    def _send_trader_records_mail(self):
        pass

    def _load_box_db_file(self):
        pass

    def _load_position_db_file(self):
        pass

    def _save_position_db_file(self):
        pass

    def _load_trader_config(self):
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
