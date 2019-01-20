# coding=utf-8

import codecs
import datetime
import json

import box
import common
from log import Logger

current_year = datetime.datetime.now().year
trader_log_filename = "%s/%s" % (common.CONST_DIR_LOG, common.CONST_LOG_TRADER_FILENAME)
trader_config_filename = "%s/%s" % (common.CONST_DIR_CONF, common.CONST_CONFIG_TRADER_FILENAME)
trader_db_records_filename = "%s/%s_%s" % (common.CONST_DIR_DATABASE, current_year, common.CONST_DB_RECORDS_FILENAME)
trader_db_position_filename = "%s/%s_%s" % (common.CONST_DIR_DATABASE, current_year, common.CONST_DB_POSITION_FILENAME)
trader_db_box_filename = box.box_db_filename

MAX_VALID_BOX_INTERVAL_HOURS = 4  # 票箱会在每天的早上8：30，和中午12：00 左右开始选取，所以不会有操过4个小时


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
            return None, u"股票箱文件: %s 不存在." % trader_db_box_filename

        try:
            box_data_set = common.file_to_dict(trader_db_box_filename)
        except Exception as err:
            return None, err.message

        box_timestamp = box_data_set.get("timestamp", None)
        box_value = box_data_set.get("value", None)
        if box_timestamp is None or box_value is None:
            return None, u"股票箱数据为空."

        current_timestamp = common.get_current_timestamp()
        if current_timestamp - box_timestamp > MAX_VALID_BOX_INTERVAL_HOURS:
            return None, u"股票箱数据文件太旧, 时间超过：%d 小时" % MAX_VALID_BOX_INTERVAL_HOURS

        return box_value, None

    @staticmethod
    def _load_position_db_file():
        if not common.file_exist(trader_db_position_filename):
            return None, u"股票持仓数据文件: %s 不存在." % trader_db_position_filename

        try:
            position_data_set = common.file_to_dict(trader_db_position_filename)
            return position_data_set, None
        except Exception as err:
            return None, err.message

    @staticmethod
    def _save_position_db_file(db_dataset):
        if common.file_exist(trader_db_position_filename):
            common.delete_file(trader_db_position_filename)

        try:
            common.dict_to_file(db_dataset, trader_db_position_filename)
        except Exception as err:
            return err.message

        return None

    @staticmethod
    def _load_trader_config():
        if not common.file_exist(trader_config_filename):
            return None, u"交易模块配置文件: %s 不存在." % trader_config_filename
        try:
            with codecs.open(trader_config_filename, 'r', 'utf-8') as _file:
                return json.load(_file), None
        except Exception as err:
            return None, err.message

    # 记录交易记录
    def _save_trader_records(self, dataset):
        with codecs.open(trader_db_records_filename, 'a', 'utf-8') as _file:
            try:
                _file.writelines(map(lambda x: json.dumps(x) + '\n', dataset))  # 在写入参数str后加“\n”则会在每次完成写入后，自动换行到下一行
            except Exception as err:
                self.log.logger.error(u"保存交易记录数据出错: %s" % err.message)

    # 对加载得BOX进行初始筛选和排序，选择最合适得前几个（默认type1>type2>type3>type4）
    # 检查当前的股票箱内的股票是否倒了可以出发买点
    @staticmethod
    def _box_scanner(box_dataset):
        classify_dataset = {common.CONST_STOCK_TYPE_1: [], common.CONST_STOCK_TYPE_2: [],
                            common.CONST_STOCK_TYPE_3: [], common.CONST_STOCK_TYPE_4: []}

        for market_name, market_values in box_dataset.items():
            for stock_class_type, class_type_values in market_values.items():
                classify_dataset[stock_class_type].append(class_type_values.keys())

        return classify_dataset

    # 计算当前持仓的状态，判定是否有股票出发可以卖点
    def _position_scanner(self):
        pass

    def run(self):
        # 先卖出，再买入
        pass


if __name__ == '__main__':
    pass
