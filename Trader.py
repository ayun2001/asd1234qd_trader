# coding=utf-8

import codecs
import datetime
import json
import time

import Box
import Common
import HQAdapter
import OrderAdapter
from Log import Logger

_current_datetime = datetime.datetime.now()
trader_log_filename = "%s/%s_%s" % (Common.CONST_DIR_LOG, time.strftime('%Y%m%d', time.localtime(time.time())),
                                    Common.CONST_LOG_TRADER_FILENAME)
trader_config_filename = "%s/%s" % (Common.CONST_DIR_CONF, Common.CONST_CONFIG_TRADER_FILENAME)
trader_db_records_filename = "%s/%s_%s_%s" % (Common.CONST_DIR_DATABASE, _current_datetime.year,
                                              _current_datetime.month, Common.CONST_DB_RECORDS_FILENAME)
trader_db_position_filename = "%s/%s" % (Common.CONST_DIR_DATABASE, Common.CONST_DB_POSITION_FILENAME)
trader_db_box_filename = Box.box_db_filename

MIN_HOURS = 4
MIN_STOP_LOSS_RATIO = -3.0  # 负数，下跌3%
MIN_SELL_RAISE_RATIO = 3.0  # 涨幅3%
MAX_VALID_BOX_INTERVAL_HOURS = 4  # 票箱会在每天的早上8：30，和中午12：00 左右开始选取，所以不会有操过4个小时


# ============================================
# 函数定义
def _load_box_db_file():
    if not Common.file_exist(trader_db_box_filename):
        return None, u"股票箱文件: %s 不存在." % trader_db_box_filename

    try:
        box_data_set = Common.file_to_dict(trader_db_box_filename)
    except Exception as err:
        return None, err.message

    box_timestamp = box_data_set.get("timestamp", None)
    box_value = box_data_set.get("value", None)
    if box_timestamp is None or box_value is None:
        return None, u"股票箱数据为空."

    current_timestamp = Common.get_current_timestamp()
    if current_timestamp - box_timestamp > MAX_VALID_BOX_INTERVAL_HOURS:
        return None, u"股票箱数据文件太旧, 时间超过：%d 小时" % MAX_VALID_BOX_INTERVAL_HOURS

    return box_value, None


def _load_position_db_file():
    if not Common.file_exist(trader_db_position_filename):
        return None, u"股票持仓数据文件: %s 不存在." % trader_db_position_filename

    try:
        position_data_set = Common.file_to_dict(trader_db_position_filename)
        return position_data_set, None
    except Exception as err:
        return None, err.message


def _save_position_db_file(db_dataset):
    if Common.file_exist(trader_db_position_filename):
        Common.delete_file(trader_db_position_filename)

    try:
        Common.dict_to_file(db_dataset, trader_db_position_filename)
    except Exception as err:
        return err.message

    return None


def _load_trader_config():
    if not Common.file_exist(trader_config_filename):
        return None, u"交易模块配置文件: %s 不存在." % trader_config_filename
    try:
        with codecs.open(trader_config_filename, 'r', 'utf-8') as _file:
            return json.load(_file), None
    except Exception as err:
        return None, err.message


def _check_stock_sell_point(instance, market_code, market_desc, stock_code, stock_name):
    history_data_frame, err_info = HQAdapterHQAdapter.get_history_data_frame(instance, market=market_code, code=stock_code,
                                                                    market_desc=market_desc, name=stock_name,
                                                                    ktype=Common.CONST_K_60M,
                                                                    kcount=Common.CONST_K_LENGTH)
    if err_info is not None:
        return False, u"获得市场: %s, 股票: %s, 名称：%s, 历史K线数据错误: %s" % (market_desc, stock_code, stock_name, err_info)

    # 计算相关数据
    pct_change_list = sorted(list(history_data_frame['pct_change'].values[:MIN_HOURS]), reverse=True)
    kdj_values_list = list(history_data_frame['kdj_j'].values[:MIN_HOURS])
    kdj_cross_list = list(history_data_frame['kdj_cross'].values[:MIN_HOURS])
    kdj_cross_express_list = filter(lambda _item: _item != '', kdj_cross_list)  # 去掉之间没有值的空格

    # 求最大值
    max_j_value = max(kdj_values_list)
    max_pct_change_value = max(pct_change_list)

    # j值在最近4天内不能出现大于等于100
    bool_max_j_value = max_j_value >= 99.9

    # 不能出现小时内涨幅超过 5%的
    bool_more_than_spec_raise = max_pct_change_value > MIN_SELL_RAISE_RATIO

    # 判断 KDJ的J值 死叉
    if len(kdj_cross_express_list) > 0:
        try:
            down_cross_index_id = kdj_cross_list.index("down_cross")
            bool_down_cross_kdj = kdj_cross_express_list[0] == "down_cross" and \
                                  down_cross_index_id < MIN_SELL_RAISE_RATIO
        except ValueError:
            bool_down_cross_kdj = False
    else:
        bool_down_cross_kdj = False

    # 触发止损条件 (3%)
    if pct_change_list[0] < MIN_STOP_LOSS_RATIO:  # 已经倒序，第一个就是当前这个小时
        # 卖出股票
        OrderAdapter.send_stock_order(instance, stock_code, Common.CONST_STOCK_SELL, 0, 0)
        return True, u"触发止损条件 市场: %s, 股票: %s, 名称：%s, 发生卖出" % (market_desc, stock_code, stock_name)

    # 触发卖出条件 (上涨超过3%，KDJ_J> 100, KDJ死叉了)
    if bool_more_than_spec_raise and (bool_max_j_value or bool_down_cross_kdj):
        OrderAdapter.send_stock_order(instance, stock_code, Common.CONST_STOCK_SELL, 0, 0)
        return True, u"触发卖出条件 市场: %s, 股票: %s, 名称：%s, 发生卖出" % (market_desc, stock_code, stock_name)


class Trader(object):
    def __init__(self):
        if not Common.file_exist(Common.CONST_DIR_LOG):
            Common.create_directory(Common.CONST_DIR_LOG)

        if not Common.file_exist(Common.CONST_DIR_CONF):
            Common.create_directory(Common.CONST_DIR_CONF)

        if not Common.file_exist(Common.CONST_DIR_DATABASE):
            Common.create_directory(Common.CONST_DIR_DATABASE)

        self.log = Logger(trader_log_filename, level='debug')

    # 记录交易记录
    # 交易记录的格式
    '''
    {
        "timestamp": 0,  #时间戳
        "account_id": 0,   #账户id，以后用来聚合用
        "order_type": "buy",  # 这里交易方向，buy/sell
        "order_type_id": 0,   # 这里交易方向，buy:0 , sell:1
        "stock_id": "",    # 股票代码
        "stock_name": "",  # 股票名称
        "unit_price": 0,   # 股票单价
        "count": 0,       # 成交多少股
        "total": 0,        # 成交总金额 （没有算交易税）
        "change": 0.0,      # 交易后比之前投入增加多少百分比，正为赚，负为亏
        "revenue": 0.0     # 易后比之前投入增加多少营收，正为赚，负为亏
    }
    '''

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
        classify_dataset = {Common.CONST_STOCK_TYPE_1: [], Common.CONST_STOCK_TYPE_2: [],
                            Common.CONST_STOCK_TYPE_3: [], Common.CONST_STOCK_TYPE_4: []}

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


def run_trader_main():
    pass


if __name__ == '__main__':
    # 运行主程序
    run_trader_main()
