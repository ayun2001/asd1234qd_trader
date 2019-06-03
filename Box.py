# coding=utf-8

import threading
import time

from prettytable import PrettyTable, ALL

import Common
import HQAdapter
import Mail
from Log import Logger

box_log_filename = "%s/%s" % (Common.CONST_DIR_LOG, Common.CONST_LOG_BOX_FILENAME)
box_db_filename = "%s/%s" % (Common.CONST_DIR_DATABASE, Common.CONST_DB_BOX_FILENAME)
box_config_filename = "%s/%s" % (Common.CONST_DIR_CONF, Common.CONST_CONFIG_ADAPTER_FILENAME)

# ============================================
# 预设值

Common.V_TRADE_X_MOD = Common.load_v_trade_x_mod()
MIN_DATA_CHECK_HOURS = 4
MIN_DATA_CHECK_DAYS = 4
MIN_60M_TIMEDELTA = MIN_DATA_CHECK_HOURS * 4
MIN_60M_PRICE_RISE = 5.0
MAX_BOX_THREAD_RUNNING_TIME = 45 * 60  # 45分钟内必须要完成所有分析，要不然自动停止
MIN_CHANGE_STOP_RAISE_RATIO = 9.9
MAX_KDJ_J_VALUE = 99.9

# ============================================
# 挂载交易模块
Common.V_TRADE_X_MOD = Common.load_v_trade_x_mod()


# ============================================
# 保存股票盒到硬盘
def _save_box_data(data):
    if not Common.file_exist(Common.CONST_DIR_DATABASE):
        Common.create_directory(Common.CONST_DIR_DATABASE)
    Common.dict_to_file(data, box_db_filename)


# 发送股票盒邮件
def _generate_box_mail_message(data):
    total_number = 0
    cy_number = 0
    zx_number = 0
    sh_number = 0
    sz_number = 0

    # 创建表格
    table = PrettyTable([u"股票大盘", u"股票分类", u"数量(个)", u"待选股票"])

    # 设置表格样式
    table.align = "l"  # 使用内容左对齐
    table.format = True  # 使用格式化
    table.vrules = ALL  # 垂直线
    table.hrules = ALL  # 水平线

    # 填充数据
    for market_name, market_values in data.items():
        for stock_class_type, class_type_values in market_values.items():
            selected_count = len(class_type_values.keys())
            total_number += selected_count

            if market_name == Common.CONST_SZ_MARKET:
                sz_number += selected_count
            if market_name == Common.CONST_SH_MARKET:
                sh_number += selected_count
            if market_name == Common.CONST_ZX_MARKET:
                zx_number += selected_count
            if market_name == Common.CONST_CY_MARKET:
                cy_number += selected_count

            table.add_row([
                # 股票大盘的名字
                Common.MARKET_NAME_MAPPING[market_name],
                # 股票所属分类
                Common.TYPE_NAME_MAPPING[stock_class_type],
                # 选中股票数量
                selected_count,
                # 选中股票列表
                u",".join(class_type_values.keys()) if selected_count > Common.CONST_DATA_LIST_LEN_ZERO else u"无"
            ])

    summary_message = u"总共选取股票数量: %d --> 上海: %d, 深圳: %d, 中小: %d, 创业: %d" % (
        total_number, sh_number, sz_number, zx_number, cy_number)
    mail_message = table.get_html_string() + u"<p>%s</p>" % summary_message
    return mail_message, summary_message


# ============================================
# 生成股票盒
class GenerateBox(object):
    def __init__(self):
        if not Common.file_exist(Common.CONST_DIR_LOG):
            Common.create_directory(Common.CONST_DIR_LOG)

        if not Common.file_exist(Common.CONST_DIR_CONF):
            Common.create_directory(Common.CONST_DIR_CONF)

        self.log = Logger(box_log_filename, level="debug", backup_count=Common.CONST_LOG_BACKUP_FILES)
        self.connect_instance = None
        self.config = None

    # 创建行情连接，
    def _create_safe_connect(self):
        multiplying_factor = 0
        while True:
            multiplying_factor += 1
            # 判断重新连接的次数是否超过最大次数
            if multiplying_factor > Common.CONST_MAX_CONNECT_RETRIES:
                multiplying_factor = 1
                Common.V_TRADE_X_MOD = Common.load_v_trade_x_mod()
                self.log.logger.info(u"重新加载底层驱动, 并重置等待时间")
            # 关闭之前的连接
            HQAdapter.destroy_connect_instance(self.connect_instance)
            # 重新创建行服务器连接
            self.connect_instance, err_info = HQAdapter.create_connect_instance(self.config)
            if self.connect_instance is None:
                retry_delay_time = Common.CONST_RETRY_CONNECT_INTERVAL * multiplying_factor
                self.log.logger.error(u"创建行情服务器连接实例失败: %s, 等待 %s 后重试" % (
                    err_info, Common.change_seconds_to_time(retry_delay_time)))
                time.sleep(retry_delay_time)  # 休息指定的事件，重新创建连接对象
                continue
            else:
                self.connect_instance.SetTimeout(Common.CONST_CONNECT_TIMEOUT, Common.CONST_CONNECT_TIMEOUT)
                self.log.logger.info(u"选中行情服务器 # %s" % err_info)
                break

    # 获得历史数据，拥有自动重试功能
    def _get_safe_history_data_frame(self, market_code, market_desc, stock_code, stock_name, ktype, kcount):
        multiplying_factor = 0
        while True:
            multiplying_factor += 1

            # 判断重新连接的次数是否超过最大次数
            if multiplying_factor > Common.CONST_MAX_CONNECT_RETRIES:
                multiplying_factor = 1
                Common.V_TRADE_X_MOD = Common.load_v_trade_x_mod()
                self.log.logger.info(u"重新加载底层驱动, 并重置等待时间")

            # 执行获得历史K线数据
            if self.connect_instance is not None:
                # 获得股票的K线信息
                history_data_frame, err_info = HQAdapter.get_history_data_frame(
                    self.connect_instance, market=market_code, code=stock_code, market_desc=market_desc,
                    name=stock_name, ktype=ktype, kcount=kcount)
            else:
                history_data_frame = None
                err_info = u"行情服务器连接实例为空, 等待重新创建, errCode=10038, 断开"

            # 对执行错误执行处理
            if err_info is not None:
                self.log.logger.error(
                    u"获得市场: %s, 股票: %s, 名称：%s, 历史数据错误: %s" % (market_desc, stock_code, stock_name, err_info))
                # 发现连接错误 10038 需要重连
                if err_info.find("errCode=10038") > -1 or err_info.find(u"断开") > -1:
                    retry_delay_time = Common.CONST_RETRY_CONNECT_INTERVAL * multiplying_factor
                    self.log.logger.info(u"等待: %s 后重试连接行情服务器" % Common.change_seconds_to_time(retry_delay_time))
                    # 休息指定的时间，重新创建连接对象
                    time.sleep(retry_delay_time)
                    # 关闭之前的连接
                    HQAdapter.destroy_connect_instance(self.connect_instance)
                    # 重新创建行服务器连接
                    self.connect_instance, err_info = HQAdapter.create_connect_instance(self.config)
                    if self.connect_instance is None:
                        self.log.logger.error(u"重新创建行情服务器连接实例失败: %s" % err_info)
                    else:
                        self.connect_instance.SetTimeout(Common.CONST_CONNECT_TIMEOUT, Common.CONST_CONNECT_TIMEOUT)
                        self.log.logger.info(u"重新创建行情服务器连接实例成功, 选中行情服务器 # %s" % err_info)
                else:
                    return None  # 错误，返回None
            else:  # 正确，返回数据
                return history_data_frame

    # 扩展股票数据
    def _get_stock_temp_list(self, stock_list=None):
        if stock_list is None:
            self.log.logger.error(u"股票清单为空, 检查源数据")
            return None
        stock_t_list = []
        for key, data in stock_list.items():
            try:
                stock_t_list.extend([(key, data["desc"], v["code"], v["name"]) for v in data["values"]])
            except KeyError:
                continue
        if len(stock_t_list) == Common.CONST_DATA_LIST_LEN_ZERO:
            self.log.logger.error(u"转换后的股票清单为空, 检查源数据")
            return None
        else:
            return stock_t_list

    # 挑出涨停盘的股票
    def _compute_task_handler(self, input_dataset=None, output_dataset=None):
        # 内联函数，修正处理数据 20190212
        def _delete_old_data(origin_data, days):
            return origin_data.drop(origin_data[:(len(origin_data.index) - days)].index)

        # 正式数据处理过程
        if input_dataset is None:
            self.log.logger.error(u"股票数据为空, 检查源数据")
            return

        market_name, market_desc, stock_code, stock_name = input_dataset
        self.log.logger.info(u"[第1阶段] 正在获得并处理 市场: %s, 股票: %s, 名称：%s 的数据" % (market_desc, stock_code, stock_name))
        try:
            market_code = Common.MARKET_CODE_MAPPING[market_name]
        except KeyError:
            self.log.logger.error(u"获得股票: %s, 名称：%s, 市场映射关系数据不存在" % (stock_code, stock_name))
            return

        # 获取日线历史数据
        history_data_frame = self._get_safe_history_data_frame(
            market_code=market_code, market_desc=market_desc, stock_code=stock_code, stock_name=stock_name,
            ktype=Common.CONST_K_DAY, kcount=Common.CONST_K_LENGTH)

        if history_data_frame is None:
            return

        history_data_frame_index_list = history_data_frame.index
        history_data_count = len(history_data_frame_index_list)
        # 这里需要高度关注下，因为默认可能只有14天
        if history_data_count < (Common.CONST_K_LENGTH / 2 if Common.CONST_K_LENGTH < 3 else 14):
            self.log.logger.error(u"参与计算得市场: %s, 股票: %s, 名称：%s, K数据: %d (不够>=14)" % (
                market_desc, stock_code, stock_name, history_data_count))
            return
        express_stock_hist_data_frame = history_data_frame[["close", "low", "open", "pct_change"]]
        for item_date_time in history_data_frame_index_list:
            try:
                close_value, low_value, open_value, pct_change_value = express_stock_hist_data_frame.loc[
                    item_date_time].values
                if pct_change_value > MIN_CHANGE_STOP_RAISE_RATIO and close_value > open_value:  # 只要涨停的,排除1字板, 涨幅必须大于99%的
                    interval_days = history_data_count - list(history_data_frame_index_list).index(item_date_time)
                    if MIN_DATA_CHECK_DAYS <= interval_days < history_data_count:  # 这里是老薛的要求，涨停后必须还有3天的数据观察期
                        # 生成股票数据
                        stock_content_info = {
                            "meta_data": {"days": interval_days, "datetime": item_date_time, "close": close_value,
                                          "low": low_value, "stock_code": stock_code, "stock_name": stock_name,
                                          "market_name": market_name, "market_desc": market_desc},
                            "data_frame": _delete_old_data(history_data_frame, interval_days)}  # 这里需要去掉涨停之前的历史数据
                        self.log.logger.info(
                            u"[第1阶段] 正在分析 市场: %s, 股票: %s, 名称: %s, 涨停价(元): %.3f, 涨停时间: %s, 距近时间(天): %d" % (
                                market_desc, stock_code, stock_name, close_value, item_date_time, interval_days))
                        # 保存数据
                        output_dataset[market_name][stock_code] = stock_content_info
            except Exception as err:
                self.log.logger.error(u"市场: %s, 股票: %s, 名称: %s, 数据时间: %s, 错误: %s" % (
                    market_desc, stock_code, stock_name, item_date_time, err.message))
            continue

    def _stock_60m_k_type_filter(self, market_name, market_desc, stock_name, stock_code, days):
        try:
            market_code = Common.MARKET_CODE_MAPPING[market_name]  # 市场代码
            market_turnover_ratio = Common.MARKET_TURNOVER_MAPPING[market_name]  # 市场换手率
        except KeyError:
            self.log.logger.error(u"获得市场: %s, 股票: %s, 名称：%s 市场映射关系数据不存在" % (market_desc, stock_code, stock_name))
            return False

        # 获取60分钟历史数据
        history_data_frame = self._get_safe_history_data_frame(
            market_code=market_code, market_desc=market_desc, stock_code=stock_code, stock_name=stock_name,
            ktype=Common.CONST_K_60M, kcount=Common.CONST_K_LENGTH)

        if history_data_frame is None:
            return False

        # 翻转这个data_frame
        history_data_frame.sort_index(ascending=False, inplace=True)
        # 获得必须要的数据 [:days * 4] 修正值，关注到涨停的那天, 老薛只关注涨停后的3天的数据作为判断
        pct_change_list = list(history_data_frame["pct_change"].values[:days * MIN_DATA_CHECK_HOURS])
        turnover_list = list(history_data_frame["turnover"].values[:days * MIN_DATA_CHECK_HOURS])
        kdj_values_list = list(history_data_frame["kdj_j"].values[:days * MIN_DATA_CHECK_HOURS])
        kdj_cross_list = list(history_data_frame["kdj_cross"].values[:days * MIN_DATA_CHECK_HOURS])
        kdj_cross_express_list = filter(lambda _item: _item != "", kdj_cross_list)  # 去掉之间没有值的空格
        # 20190212 J值修正，只看当前天数往前的4天时间内的J值最大值
        max_j_value = max(kdj_values_list[:MIN_DATA_CHECK_DAYS * MIN_DATA_CHECK_HOURS])
        max_pct_change_value = max(pct_change_list)
        # j值在最近4天内不能出现大于等于100
        bool_max_j_value = max_j_value >= MAX_KDJ_J_VALUE
        # 20190212 不能出现小时内涨幅超过 5%的, 同时换手率不能超过指定的市场类型的保留换手率
        max_pct_change_index = pct_change_list.index(max_pct_change_value)
        bool_more_than_spec_raise = max_pct_change_value > MIN_60M_PRICE_RISE and \
                                    turnover_list[max_pct_change_index] < market_turnover_ratio
        # 判断 KDJ的J值
        if len(kdj_cross_express_list) > Common.CONST_DATA_LIST_LEN_ZERO:
            try:
                up_index_id = kdj_cross_list.index("up_cross")
                bool_up_cross_kdj = kdj_cross_express_list[0] == "up_cross" and up_index_id >= MIN_DATA_CHECK_HOURS
            except ValueError:
                bool_up_cross_kdj = False
            try:
                down_index_id = kdj_cross_list.index("down_cross")
                bool_down_cross_kdj = kdj_cross_express_list[0] == "down_cross" and down_index_id < MIN_60M_TIMEDELTA
            except ValueError:
                bool_down_cross_kdj = False
        else:
            bool_up_cross_kdj = False
            bool_down_cross_kdj = False
        self.log.logger.info(
            u"[第2阶段] 正在判断 市场: %s, 股票: %s, 名称：%s, KDJ_J值>=100: %s, KDJ死叉: %s, KDJ金叉(错过买点1天): %s, 涨幅超过%.2f%%: %s" % (
                market_desc, stock_code, stock_name, str(bool_max_j_value), str(bool_down_cross_kdj),
                str(bool_up_cross_kdj), MIN_60M_PRICE_RISE, str(bool_more_than_spec_raise)))
        # KDJ的j值大于100，KDJ死叉，出现过金叉，但是某天涨幅超过5%
        if bool_max_j_value or bool_up_cross_kdj or bool_down_cross_kdj or bool_more_than_spec_raise:
            return True
        else:
            return False

    def stage1_compute_data(self):
        valid_stock_data_set = {Common.CONST_SH_MARKET: {}, Common.CONST_SZ_MARKET: {}, Common.CONST_ZX_MARKET: {},
                                Common.CONST_CY_MARKET: {}}

        # 获得全局股票列表
        stock_codes, err_info = HQAdapter.get_stock_codes(self.connect_instance)
        if stock_codes is None:
            self.log.logger.error(u"[第1阶段] 获得股票代码池错误: %s", err_info)
            return None

        # debug
        # stock_codes = {
        #     Common.CONST_ZX_MARKET: {'count': 1, 'desc': u"xx主板", 'values': [{'code': '002674', 'name': u"xxxx"}]},
        # }

        # 转换当前数据结果集，方便后续分析
        valid_stock_info_list = self._get_stock_temp_list(stock_codes)
        if valid_stock_info_list is None:
            return None

        # 获取当前股票池内股票的数据
        for stock_item in valid_stock_info_list:
            self._compute_task_handler(stock_item, valid_stock_data_set)
            # 延迟休息，防止被封
            time.sleep(Common.CONST_TASK_WAITING_TIME / 1000.0)

        return valid_stock_data_set

    def stage2_filter_data(self, stock_pool):
        if stock_pool is None:
            return None

        valid_stock_data_set = {
            Common.CONST_SH_MARKET: {Common.CONST_STOCK_TYPE_1: {}, Common.CONST_STOCK_TYPE_2: {},
                                     Common.CONST_STOCK_TYPE_3: {}, Common.CONST_STOCK_TYPE_4: {}},
            Common.CONST_SZ_MARKET: {Common.CONST_STOCK_TYPE_1: {}, Common.CONST_STOCK_TYPE_2: {},
                                     Common.CONST_STOCK_TYPE_3: {}, Common.CONST_STOCK_TYPE_4: {}},
            Common.CONST_ZX_MARKET: {Common.CONST_STOCK_TYPE_1: {}, Common.CONST_STOCK_TYPE_2: {},
                                     Common.CONST_STOCK_TYPE_3: {}, Common.CONST_STOCK_TYPE_4: {}},
            Common.CONST_CY_MARKET: {Common.CONST_STOCK_TYPE_1: {}, Common.CONST_STOCK_TYPE_2: {},
                                     Common.CONST_STOCK_TYPE_3: {}, Common.CONST_STOCK_TYPE_4: {}}}

        for market_code, market_values in stock_pool.items():
            for stock_code, stock_info_values in market_values.items():
                # 延迟休息，防止被封
                time.sleep(Common.CONST_TASK_WAITING_TIME / 1000.0)

                # 执行数据处理和分类过程
                try:
                    # 元数据纠正，只能看涨停后的数据 20190212
                    stock_data_frame = stock_info_values["data_frame"]
                    stock_meta_data = stock_info_values["meta_data"]
                    stock_close_prices_list = list(stock_data_frame["close"].values)
                    stock_turn_over_list = list(stock_data_frame["turnover"].values)
                    min_close_price = min(stock_close_prices_list)
                    max_turn_over = max(stock_turn_over_list)
                    meta_close_price = stock_meta_data["close"]
                    meta_low_price = stock_meta_data["low"]
                    stock_name = stock_meta_data["stock_name"]
                    interval_days = stock_meta_data["days"]
                    market_name = stock_meta_data["market_name"]
                    market_desc = stock_meta_data["market_desc"]

                    # 执行60分钟数据过滤
                    bool_filter_result = self._stock_60m_k_type_filter(
                        market_name=market_name, stock_code=stock_code, stock_name=stock_name,
                        market_desc=market_desc, days=interval_days
                    )

                    # print min_close_price, meta_close_price, meta_low_price

                    if min_close_price >= meta_close_price:
                        # Type1 一类
                        if market_name == Common.CONST_SH_MARKET and max_turn_over <= Common.CONST_SH_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_SZ_MARKET and max_turn_over <= Common.CONST_SZ_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_ZX_MARKET and max_turn_over <= Common.CONST_ZX_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_CY_MARKET and max_turn_over <= 18 and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        # Type2 二类
                        if market_name == Common.CONST_SH_MARKET and max_turn_over > Common.CONST_SH_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_SZ_MARKET and max_turn_over > Common.CONST_SZ_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_ZX_MARKET and max_turn_over > Common.CONST_ZX_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_CY_MARKET and max_turn_over > Common.CONST_CY_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue

                    if meta_close_price > min_close_price >= meta_low_price:
                        # Type3 三类
                        if market_name == Common.CONST_SH_MARKET and max_turn_over <= Common.CONST_SH_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_SZ_MARKET and max_turn_over <= Common.CONST_SZ_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_ZX_MARKET and max_turn_over <= Common.CONST_ZX_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_CY_MARKET and max_turn_over <= Common.CONST_CY_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        # Type4 四类
                        if market_name == Common.CONST_SH_MARKET and max_turn_over > Common.CONST_SH_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_SZ_MARKET and max_turn_over > Common.CONST_SZ_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_ZX_MARKET and max_turn_over > Common.CONST_ZX_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue
                        if market_name == Common.CONST_CY_MARKET and max_turn_over > Common.CONST_CY_STOCK_TURNOVER and not bool_filter_result:
                            valid_stock_data_set[market_name][Common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue

                except Exception as err:
                    self.log.logger.error(u"[第2阶段] 股票: %s, 分类错误: %s" % (stock_code, err.message))
                    continue

        return valid_stock_data_set

    def generate(self):
        # 加载股票箱的配置文件
        self.config, err_info = Common.load_adapter_config(box_config_filename)
        if err_info is not None:
            self.log.logger.error(u"加载股票箱配置文件错误: %s", err_info)
            return None

        # 创建行情服务器连接
        self._create_safe_connect()

        # 执行数据获取和分析
        valid_stock_pool = self.stage1_compute_data()
        if valid_stock_pool is None:
            return None

        # 获得所选股票箱
        selected_stock_box = self.stage2_filter_data(valid_stock_pool)

        # 关闭数据连接
        HQAdapter.destroy_connect_instance(self.connect_instance)
        self.connect_instance = None

        # 返回数据
        return selected_stock_box


def gen_box_main():
    gen_box = GenerateBox()

    if Common.check_today_is_holiday_time():
        gen_box.log.logger.warning(u"节假日休假, 股票市场不交易, 跳过")
        exit(0)

    # 计算股数据，并记录时间锚点
    gen_box.log.logger.info(u"============== [开始计算票箱] ==============")
    start_timestamp = time.time()
    valid_stock_box = gen_box.generate()
    end_timestamp = time.time()
    gen_box.log.logger.info(u"============== [结束计算票箱] ==============")

    current_datetime = Common.get_current_datetime()

    if valid_stock_box is None:
        gen_box.log.logger.error(u"生成的表股票箱为空")
        Mail.send_mail(title=u"[%s] 股票箱计算错误" % current_datetime, msg="[ERROR]")
        exit(0)

    # 生成邮件数据，并发送结果邮件
    total_compute_time = Common.change_seconds_to_time(int(end_timestamp - start_timestamp))
    mail_message, summary_message = _generate_box_mail_message(valid_stock_box)
    sendmail_message = mail_message + u"<p>计算总费时: %s</p>" % total_compute_time
    gen_box.log.logger.info(summary_message)
    gen_box.log.logger.info(u"计算总费时: %s" % total_compute_time)

    # 保存股票盒
    _save_box_data(data={"timestamp": Common.get_current_timestamp(), "value": valid_stock_box})
    # 发送已经选的股票结果
    Mail.send_mail(title=u"日期:%s, 选中的股票箱" % current_datetime, msg=sendmail_message)


if __name__ == '__main__':
    # 运行主程序, 这里需要使用线程函数的join的超时功能, 防止程序一直在后台运行
    current_thread = threading.Thread(target=gen_box_main)
    current_thread.daemon = True  # 所有daemon值为True的子线程将随主线程一起结束，而不论是否运行完成。
    current_thread.start()
    current_thread.join(timeout=MAX_BOX_THREAD_RUNNING_TIME)
