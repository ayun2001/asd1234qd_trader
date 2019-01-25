# coding=utf-8

import codecs
import datetime
import json
import time

from prettytable import PrettyTable

import Box
import Common
import HQAdapter
import Mail
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

MIN_DATA_CHECK_HOURS = 4
MIN_STOP_LOSS_RATIO = -3.0  # 负数，下跌3%
MIN_SELL_RAISE_RATIO = 3.0  # 涨幅3%
MAX_VALID_BOX_INTERVAL_HOURS = 4  # 票箱会在每天的早上8：30，和中午12：00 左右开始选取，所以不会有操过4个小时
MAX_SELL_TOTAL_RATIO = 0.6
MIN_TASK_WAITING_TIME = 20  # 单位：秒
MIN_TRADE_TIME_INTERVAL = 5 * 60 * 60  # 单位：秒


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
    """
    {"stock_code":{
      "timestamp": 0,  #时间戳
      "market_code": 0,
      "market_desc": "",
      "stock_name": "",
      "price": 0.0,
      "count": 0，
      "trade_account_id": 0,   #账户id，以后用来聚合用
    }}
    """
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


# 发送股票盒邮件
def _generate_trade_mail_message(data):
    mail_message = ""

    if len(data) <= 0:
        return mail_message

    for trade_type_id, trade_record_data in data.items():
        # 生成买入信息列表
        if trade_type_id == Common.CONST_STOCK_BUY and len(trade_record_data) > 0:
            table = PrettyTable([u"股票大盘", u"股票代码", u"股票名称", u"价格(元)", u"数量(股)", u"总价(元)"])
            table.format = True

            for record_item in trade_record_data:
                try:
                    table.add_row([
                        record_item["market_desc"],
                        record_item["stock_code"],
                        record_item["stock_name"],
                        record_item["price"],
                        record_item["count"],
                        record_item["total"],
                    ])
                except KeyError:
                    continue

            mail_message += u"<p>%s:</p>%s<br>" % (Common.CONST_STOCK_BUY_DESC, table.get_html_string())

        # 生成卖出信息列表
        if trade_type_id == Common.CONST_STOCK_SELL and len(trade_record_data) > 0:
            table = PrettyTable([u"股票大盘", u"股票代码", u"股票名称", u"价格(元)", u"数量(股)", u"总价(元)", u"营收(元)", u"营收率(百分率)"])
            table.format = True

            for record_item in trade_record_data:
                try:
                    table.add_row([
                        record_item["market_desc"],
                        record_item["stock_code"],
                        record_item["stock_name"],
                        record_item["price"],
                        record_item["count"],
                        record_item["total"],
                        record_item["revenue_value"],
                        record_item["revenue_change"]
                    ])
                except KeyError:
                    continue

            mail_message += u"<p>%s:</p>%s<br>" % (Common.CONST_STOCK_SELL_DESC, table.get_html_string())

    return mail_message


def _check_stock_sell_point(instance, market_code, market_desc, stock_code, stock_name):
    history_data_frame, err_info = HQAdapter.get_history_data_frame(instance, market=market_code, code=stock_code,
                                                                    market_desc=market_desc, name=stock_name,
                                                                    ktype=Common.CONST_K_60M,
                                                                    kcount=Common.CONST_K_LENGTH)
    if err_info is not None:
        return False, u"获得市场: %s, 股票: %s, 名称：%s, 历史K线数据错误: %s" % (market_desc, stock_code, stock_name, err_info)

    # 计算相关数据
    pct_change_list = sorted(list(history_data_frame['pct_change'].values[:MIN_DATA_CHECK_HOURS]), reverse=True)
    kdj_values_list = list(history_data_frame['kdj_j'].values[:MIN_DATA_CHECK_HOURS])
    kdj_cross_list = list(history_data_frame['kdj_cross'].values[:MIN_DATA_CHECK_HOURS])
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
        return True, u"触发止损条件 市场: %s, 股票: %s, 名称：%s, 发生卖出" % (market_desc, stock_code, stock_name)

    # 触发卖出条件 (上涨超过3%，KDJ_J> 100, KDJ死叉了)
    if bool_more_than_spec_raise and (bool_max_j_value or bool_down_cross_kdj):
        return True, u"触发卖出条件 市场: %s, 股票: %s, 名称：%s, 发生卖出" % (market_desc, stock_code, stock_name)

    return False, u"执行 市场: %s, 股票: %s, 名称：%s, 继续持仓..." % (market_desc, stock_code, stock_name)


class TradeExecutor(object):
    def __init__(self):
        if not Common.file_exist(Common.CONST_DIR_LOG):
            Common.create_directory(Common.CONST_DIR_LOG)

        if not Common.file_exist(Common.CONST_DIR_CONF):
            Common.create_directory(Common.CONST_DIR_CONF)

        if not Common.file_exist(Common.CONST_DIR_DATABASE):
            Common.create_directory(Common.CONST_DIR_DATABASE)

        self.log = Logger(trader_log_filename, level='debug')
        self.config = None
        self.connect_instance = None
        self.trade_records_data_set = {}

    # 记录交易记录
    # 交易记录的格式
    def _save_trader_records(self, dataset):
        """
        {
            "timestamp": 0,  #时间戳
            "datetime": 2018-01-01 xx:xx:xx,当前日期，用来可读的
            "trade_account_id": 0,   #账户id，以后用来聚合用
            "order_type": "buy",  # 这里交易方向，buy/sell
            "order_type_id": 0,   # 这里交易方向，buy:0 , sell:1
            "market_code"   # 市场代码
            "market_desc":  # 市场描述
            "stock_id": "",    # 股票代码
            "stock_name": "",  # 股票名称
            "price": 0.0,   # 股票单价
            "count": 0,       # 成交多少股
            "total": 0.0,        # 成交总金额 （没有算交易税）
            "revenue_change": 0.0,      # 交易后比之前投入增加多少百分比，正为赚，负为亏
            "revenue_value": 0.0     # 易后比之前投入增加多少营收，正为赚，负为亏
        }
        """
        with codecs.open(trader_db_records_filename, 'a', 'utf-8') as _file:
            try:
                _file.writelines(map(lambda x: json.dumps(x) + '\n', dataset))  # 在写入参数str后加“\n”则会在每次完成写入后，自动换行到下一行
            except Exception as err:
                self.log.logger.error(u"保存交易记录数据出错: %s" % err.message)

    # 对加载得BOX进行初始筛选和排序，选择最合适得前几个（默认type1>type2>type3>type4）
    # 检查当前的股票箱内的股票是否倒了可以出发买点
    def _box_scanner(self):
        # classify_dataset = {Common.CONST_STOCK_TYPE_1: [], Common.CONST_STOCK_TYPE_2: [],
        #                     Common.CONST_STOCK_TYPE_3: [], Common.CONST_STOCK_TYPE_4: []}
        #
        # for market_name, market_values in box_dataset.items():
        #     for stock_class_type, class_type_values in market_values.items():
        #         classify_dataset[stock_class_type].append(class_type_values.keys())
        #
        # return classify_dataset
        pass

    # 计算当前持仓的状态，判定是否有股票出发可以卖点
    def _position_scanner(self):
        # 读取持仓数据文件
        position_data, err_info = _load_position_db_file()

        # 执行持仓扫描逻辑
        if err_info is not None:
            self.log.logger.error(u"读取持仓股票数据文件错误: %s" % err_info)
            return
        else:
            records_set = []
            for stock_code, stock_values in position_data.items():
                try:
                    market_code = stock_values["market_code"]
                    market_desc = stock_values["market_desc"]
                    stock_name = stock_values["stock_name"]
                    position_own_value = position_data[stock_code]
                    last_trade_timestamp = position_own_value["timestamp"]
                    current_own_count = position_own_value["count"]
                    current_trade_account_id = position_own_value["trade_account_id"]

                    # 判断股票是否当天够买的，如果是跳过 (满足最小交易时间)
                    current_own_time_interval = Common.get_current_timestamp() - last_trade_timestamp
                    if current_own_time_interval < MIN_TRADE_TIME_INTERVAL:
                        self.log.logger.info(u"执行动作 市场: %s, 股票: %s, 名称：%s, 信号: %s, 持有时间: %s 不满足最小持仓时间" % (
                            market_desc, stock_code, stock_name, Common.CONST_STOCK_SELL_DESC,
                            Common.change_seconds_to_time(current_own_time_interval)))
                        continue

                    # 检查股票交易卖点
                    bool_sell, err_info = _check_stock_sell_point(self.connect_instance, stock_code, market_code,
                                                                  market_desc, stock_name)
                    self.log.logger.error(u"执行持仓 市场: %s, 股票: %s, 名称：%s, 卖点扫描, 结果: %s" % (
                        market_desc, stock_code, stock_name, err_info))

                    # 如果没有卖出信号，跳过后端的代码
                    if not bool_sell:
                        continue

                    # 执行卖出动做
                    while True:
                        if current_own_count <= 0:
                            # 删除持仓记录
                            del position_data[stock_code]
                            # 记录日志
                            self.log.logger.info(u"执行动作 市场: %s, 股票: %s, 名称：%s, 信号: %s 完成!" % (
                                market_desc, stock_code, stock_name, Common.CONST_STOCK_SELL_DESC))
                            break

                        # 获得5档价格数据
                        level5_quotes_dataset, err_info = HQAdapter.get_stock_quotes(self.connect_instance,
                                                                                     [(market_code, stock_code)])
                        if err_info is None:
                            self.log.logger.error(u"获得 市场: %s, 股票: %s, 名称：%s, 5档行情数据错误: %s" % (
                                market_desc, stock_code, stock_name, err_info))
                            continue

                        level5_quote_value = level5_quotes_dataset[stock_code]
                        avg_level5_price = level5_quote_value["buy5_avg_price"]
                        # 按照交易总数固定比例投放交易股票数量, 1手 = 100股
                        max_can_sell_count = int(
                            (level5_quote_value["buy5_step_count"] / 100) * MAX_SELL_TOTAL_RATIO) * 100

                        # 执行下订单动作, 4 市价委托(上海五档即成剩撤/ 深圳五档即成剩撤) -- 此时价格没有用处，用 0 传入即可
                        err_info = OrderAdapter.send_stock_order(self.connect_instance, stock_code,
                                                                 current_trade_account_id, Common.CONST_STOCK_SELL,
                                                                 0, max_can_sell_count)
                        # 记录交易数据
                        if err_info is None:
                            sell_total_value = avg_level5_price * max_can_sell_count
                            revenue_value = (avg_level5_price - position_own_value["price"]) * max_can_sell_count
                            revenue_change = revenue_value / (position_own_value["price"] * max_can_sell_count)
                            trader_record = {
                                "timestamp": Common.get_current_timestamp(),
                                "datetime": Common.get_current_datetime(),
                                "trade_account_id": current_trade_account_id,
                                "order_type": Common.CONST_STOCK_SELL_DESC,
                                "order_type_id": Common.CONST_STOCK_SELL,
                                "market_code": market_code,
                                "market_desc": market_desc,
                                "stock_code": stock_code,
                                "stock_name": stock_name,
                                "price": avg_level5_price,
                                "count": max_can_sell_count,
                                "total": sell_total_value,
                                "revenue_change": revenue_change,
                                "revenue_value": revenue_value
                            }
                            records_set.append(trader_record)
                            self.log.logger.info(
                                u"执行动作 市场: %s, 股票: %s, 名称：%s, 信号: %s, 价格: %.2f, 数量: %d, 总价: %.2f, 营收(元): %.2f, 营收率(%%): %.2f" % (
                                    market_desc, stock_code, stock_name, Common.CONST_STOCK_SELL_DESC, avg_level5_price,
                                    max_can_sell_count, sell_total_value, revenue_value, revenue_change))
                        else:
                            self.log.logger.warn(u"没有执行动作 市场: %s, 股票: %s, 名称：%s, 信号: %s, 价格: %.2f, 数量: %d" % (
                                market_desc, stock_code, stock_name, Common.CONST_STOCK_SELL_DESC, avg_level5_price,
                                max_can_sell_count))

                        # 减去当前的交易的投放量
                        current_own_count -= max_can_sell_count
                        # 等待订单消化时间
                        time.sleep(MIN_TASK_WAITING_TIME)

                except Exception as err:
                    self.log.logger.error(u"执行持仓股票扫描出现出错: %s" % err.message)
                    continue

            if len(records_set) > 0:
                # 保存交易数据倒本地z
                self._save_trader_records(records_set)
                # 保存数据到全局交易数据集，准备发送邮件
                self.trade_records_data_set[Common.CONST_STOCK_SELL] = records_set

        # 保存持仓文件
        _save_position_db_file(position_data)

    def execute(self):
        # 加载交易器的配置文件
        self.config, err_info = _load_trader_config()
        if err_info is not None:
            self.log.logger.error(u"加载交易器配置文件错误: %s", err_info)
            return None

        while True:
            self.connect_instance, err_info = OrderAdapter.create_connect_instance(self.config)
            if err_info is not None:
                self.log.logger.error(u"创建交易服务器连接实例失败: %s" % err_info)
                time.sleep(Common.CONST_RETRY_CONNECT_INTERVAL)  # 休息指定的事件，重新创建连接对象
                continue
            else:
                break

        # 清空交易记录
        self.trade_records_data_set = {}

        # 先卖出
        self._position_scanner()
        # 再买入
        self._box_scanner()

        # 返回正确的交易记录数据
        return self.trade_records_data_set


def trade_exec_main():
    trade_exec = TradeExecutor()

    if Common.check_today_is_holiday_time():
        trade_exec.log.logger.warning(u"节假日休假, 股票市场不交易, 跳过...")
        exit(0)

    trade_exec.log.logger.info(u"============== [开始自动交易] ==============")
    start_timestamp = time.time()
    valid_trade_records = trade_exec.execute()
    end_timestamp = time.time()
    trade_exec.log.logger.info(u"============== [结束自动交易] ==============")

    current_datetime = Common.get_current_datetime()

    if valid_trade_records is None:
        trade_exec.log.logger.error(u"执行交易记录为空")
        Mail.send_mail(title=u"[%s] 交易执行错误" % current_datetime, msg="[ERROR]")
        exit(0)

    current_datetime = Common.get_current_datetime()
    total_compute_time = Common.change_seconds_to_time(int(end_timestamp - start_timestamp))
    trade_exec.log.logger.info(u"计算总费时: %s" % total_compute_time)
    sendmail_message = _generate_trade_mail_message(valid_trade_records) + u"<p>计算总费时: %s</p>" % total_compute_time

    # 发送交易记录结果
    Mail.send_mail(title=u"日期:%s, 执行交易记录" % current_datetime, msg=sendmail_message)


if __name__ == '__main__':
    # 运行主程序
    trade_exec_main()
