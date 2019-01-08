# coding=utf-8

import time

import adapter
import common
import mail
from log import Logger

_box_log_filename = "%s/%s" % (common.CONST_DIR_LOG, common.CONST_LOG_BOX_FILENAME)
_box_db_filename = "%s/%s" % (common.CONST_DIR_DATABASE, common.CONST_DB_BOX_FILENAME)

MIN_HOURS = 4
MIN_60M_TIMEDELTA = MIN_HOURS * 4
MIN_60M_PRICE_RISE = 5.0


# 保存股票盒到硬盘
def _storage_box_data(data):
    if not common.file_exist(common.CONST_DIR_DATABASE):
        common.create_directory(common.CONST_DIR_DATABASE)

    common.dict_to_file(data, _box_db_filename)


# 发送股票盒邮件
def _generate_box_mail_message(data):
    return "%s" % str(data)


# 生成股票盒
class GenerateBox(object):
    def __init__(self):
        if not common.file_exist(common.CONST_DIR_LOG):
            common.create_directory(common.CONST_DIR_LOG)

        self.log = Logger(_box_log_filename, level='debug')
        self.connect_instance = None

    # 扩展股票数据
    def _get_stock_temp_list(self, stock_list=None):
        if stock_list is None:
            self.log.logger.error("stock list is null, check data source ...")
            return None
        stock_t_list = []
        for key, data in stock_list.items():
            try:
                stock_t_list.extend([(key, data['desc'], v['code'], v['name']) for v in data['values']])
            except KeyError:
                continue
        if len(stock_t_list) == common.CONST_STOCK_LIST_IS_NULL:
            self.log.logger.error("turnover stock list is null, check data source ...")
            return None
        else:
            return stock_t_list

    # 跳出涨停盘的股票
    def _compute_task_handler(self, instance, input_data=None, output_dataset=None):
        if input_data is None:
            self.log.logger.error("input stock data is null, check input source ...")
            return
        market_name, desc_info, stock_code, stock_name = input_data
        self.log.logger.error("process stock: %s data ..." % stock_code)
        try:
            market_code = common.MARKET_CODE_MAPPING[market_name]
        except KeyError:
            self.log.logger.error("stock: %s market mapping error." % stock_code)
            return

        ok, history_data_frame = adapter.get_history_data_frame(instance, market=market_code, code=stock_code,
                                                                ktype=common.CONST_K_DAY, kcount=common.CONST_K_LENGTH)
        if not ok:
            return

        history_data_frame_index_list = history_data_frame.index
        history_data_count = len(history_data_frame_index_list)
        # 这里需要高度关注下，因为默认可能只有14天
        if history_data_count < (common.CONST_K_LENGTH / 2 if common.CONST_K_LENGTH < 3 else 14):
            self.log.logger.error("stock: %s data not enough." % stock_code)
            return
        express_stock_hist_data_frame = history_data_frame[['close', 'low', 'open', 'pct_change']]
        for item_data_time in history_data_frame_index_list:
            try:
                close_value, low_value, open_value, pct_change_value = express_stock_hist_data_frame.loc[
                    item_data_time].values
                if pct_change_value > 9.9 and close_value > open_value:  # 只要涨停的,排除1字板, 涨幅必须大于99%的
                    interval_days = history_data_count - list(history_data_frame_index_list).index(item_data_time)
                    if 4 <= interval_days < history_data_count:  # 这里是老薛的要求，涨停后必须还有3天的数据观察期
                        stock_content_info = {
                            'metaData': {'days': interval_days, 'date': item_data_time, 'close': close_value,
                                         'low': low_value, 'code': stock_code, 'name': stock_name,
                                         'marketName': market_name, 'marketDesc': desc_info},
                            'dataFrame': history_data_frame}  # 这里是否包去掉以前的历史数据，还要分析下
                        self.log.logger.error(
                            "stock: %s have been selected, rise close value: %.3f, datetime: %s, days: %d" % (
                                stock_code, close_value, item_data_time, interval_days))
                        output_dataset[market_name][stock_code] = stock_content_info
            except Exception as err:
                self.log.logger.error("stock: %s error: %s" % (stock_code, err.message))
            continue

    def _stock_60m_k_type_filter(self, market_name="", stock_code="300729", days=0):
        try:
            market_code = common.MARKET_CODE_MAPPING[market_name]
        except KeyError:
            return False, "stock: %s market code mapping error" % stock_code

        ok, history_data_frame, err_info = adapter.get_history_data_frame(self.connect_instance, market=market_code,
                                                                          code=stock_code, ktype=common.CONST_K_60M,
                                                                          kcount=common.CONST_K_LENGTH)
        if not ok:
            return False, err_info

        # 翻转这个dataframe
        history_data_frame.sort_index(ascending=False, inplace=True)
        # 获得必须要的数据 [:days * 4] 修正值，关注到涨停的那天, 老薛只关注涨停后的3天的数据作为判断
        pct_change_list = list(history_data_frame['pct_change'].values[:days * MIN_HOURS])
        kdj_values_list = list(history_data_frame['kdj_j'].values[:days * MIN_HOURS])
        kdj_cross_list = list(history_data_frame['kdj_cross'].values[:days * MIN_HOURS])
        kdj_cross_express_list = filter(lambda _item: _item != '', kdj_cross_list)  # 去掉之间没有值的空格
        # 求最大值
        max_j_value = max(kdj_values_list)
        max_pct_change_value = max(pct_change_list)
        # j值在最近4天内不能出现大于等于100
        bool_max_j_value = max_j_value >= 99.9
        # 不能出现小时内涨幅超过 5%的
        bool_more_than_spec_raise = max_pct_change_value > MIN_60M_PRICE_RISE
        # 判断 KDJ的J值
        if len(kdj_cross_express_list) > 0:
            try:
                up_cross_index_id = kdj_cross_list.index("up_cross")
                bool_up_cross_kdj = kdj_cross_express_list[0] == "up_cross" and up_cross_index_id >= MIN_HOURS
            except ValueError:
                bool_up_cross_kdj = False
            try:
                down_cross_index_id = kdj_cross_list.index("down_cross")
                bool_down_cross_kdj = kdj_cross_express_list[0] == "down_cross" and \
                                      down_cross_index_id < MIN_60M_TIMEDELTA
            except ValueError:
                bool_down_cross_kdj = False
        else:
            bool_up_cross_kdj = False
            bool_down_cross_kdj = False
        self.log.logger.info(
            "stage2 -> stock: %s, kdj_value>=100: %s, down_cross_kdj: %s, miss_buy_point(>1day): %s, rise>=%.2f%%: %s" % (
                stock_code, str(bool_max_j_value), str(bool_down_cross_kdj),
                str(bool_up_cross_kdj), MIN_60M_PRICE_RISE, str(bool_more_than_spec_raise)))
        # KDJ的j值大于100，KDJ死叉，出现过金叉，但是某天涨幅超过5%
        if bool_max_j_value or bool_up_cross_kdj or bool_down_cross_kdj or bool_more_than_spec_raise:
            return True, None
        else:
            return False, None

    def stage1_compute_data(self):
        valid_stock_data_set = {common.CONST_SH_MARKET: {}, common.CONST_SZ_MARKET: {}, common.CONST_ZX_MARKET: {},
                                common.CONST_CY_MARKET: {}}

        ok, stock_codes, err_info = adapter.get_stock_codes(self.connect_instance)
        if not ok:
            self.log.logger.error("stage1 get stock codes error: %s", err_info)
            return None

        # debug
        # stock_codes = {CONST_SZ_MARKET: {'count': 1, 'desc': "xx主板", 'values': [{'code': '300745', 'name': "xxxx"}]}}

        valid_stock_info_list = self._get_stock_temp_list(stock_codes)
        if valid_stock_info_list is None:
            return None

        for stock_item in valid_stock_info_list:
            self._compute_task_handler(self.connect_instance, stock_item, valid_stock_data_set)
            # 延迟休息，防止被封
            time.sleep(common.CONST_TASK_WAITING_MS / 1000.0)

        return valid_stock_data_set

    def stage2_filter_data(self, stock_pool):
        valid_stock_data_set = {
            common.CONST_SH_MARKET: {common.CONST_STOCK_TYPE_1: {}, common.CONST_STOCK_TYPE_2: {},
                                     common.CONST_STOCK_TYPE_3: {}, common.CONST_STOCK_TYPE_4: {}},
            common.CONST_SZ_MARKET: {common.CONST_STOCK_TYPE_1: {}, common.CONST_STOCK_TYPE_2: {},
                                     common.CONST_STOCK_TYPE_3: {}, common.CONST_STOCK_TYPE_4: {}},
            common.CONST_ZX_MARKET: {common.CONST_STOCK_TYPE_1: {}, common.CONST_STOCK_TYPE_2: {},
                                     common.CONST_STOCK_TYPE_3: {}, common.CONST_STOCK_TYPE_4: {}},
            common.CONST_CY_MARKET: {common.CONST_STOCK_TYPE_1: {}, common.CONST_STOCK_TYPE_2: {},
                                     common.CONST_STOCK_TYPE_3: {}, common.CONST_STOCK_TYPE_4: {}},
        }

        if stock_pool is None:
            return valid_stock_data_set

        for market_code, market_values in stock_pool.items():
            for stock_code, stock_info_values in market_values.items():
                try:
                    stock_data_frame = stock_info_values['dataFrame']
                    stock_meta_data = stock_info_values['metaData']
                    stock_close_prices_list = list(stock_data_frame['close'].values)
                    stock_turn_over_list = list(stock_data_frame['turnover'].values)
                    min_close_price = min(stock_close_prices_list)
                    max_turn_over = max(stock_turn_over_list)
                    meta_close_price = stock_meta_data['close']
                    meta_low_price = stock_meta_data['low']
                    # stock_name = stock_meta_data['name']
                    interval_days = stock_meta_data['days']
                    market_name = stock_meta_data['marketName']
                    # market_desc = stock_meta_data['marketDesc']

                    bool_filter_result, err_info = self._stock_60m_k_type_filter(market_name=market_name,
                                                                                 stock_code=stock_code,
                                                                                 # stock_name=stock_name,
                                                                                 # market_desc=market_desc,
                                                                                 days=interval_days)
                    if err_info is not None:
                        self.log.logger.error("%s", err_info)
                        continue

                    # 延迟休息，防止被封
                    time.sleep(common.CONST_TASK_WAITING_MS / 1000.0)

                    if min_close_price >= meta_close_price:
                        # Type1
                        if market_name == common.CONST_SH_MARKET and max_turn_over <= 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_SZ_MARKET and max_turn_over <= 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_ZX_MARKET and max_turn_over <= 15 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_CY_MARKET and max_turn_over <= 18 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_1][stock_code] = stock_meta_data
                            continue
                        # Type2
                        if market_name == common.CONST_SH_MARKET and max_turn_over > 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_SZ_MARKET and max_turn_over > 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_ZX_MARKET and max_turn_over > 15 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_CY_MARKET and max_turn_over > 18 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_2][stock_code] = stock_meta_data
                            continue

                    if meta_close_price > min_close_price >= meta_low_price:
                        # Type3
                        if market_name == common.CONST_SH_MARKET and max_turn_over <= 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_SZ_MARKET and max_turn_over <= 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_ZX_MARKET and max_turn_over <= 15 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_CY_MARKET and max_turn_over <= 18 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_3][stock_code] = stock_meta_data
                            continue
                        # Type4
                        if market_name == common.CONST_SH_MARKET and max_turn_over > 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_SZ_MARKET and max_turn_over > 12 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_ZX_MARKET and max_turn_over > 15 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue
                        if market_name == common.CONST_CY_MARKET and max_turn_over > 18 and not bool_filter_result:
                            valid_stock_data_set[market_name][common.CONST_STOCK_TYPE_4][stock_code] = stock_meta_data
                            continue

                except Exception as err:
                    self.log.logger.warn("stage2 stock: %s classify error: %s", (stock_code, err.message))
                    continue

        return valid_stock_data_set

    def generate(self):
        self.connect_instance, err_info = adapter.create_connect_instance()
        if self.connect_instance is None:
            self.log.logger.error("create hq connect instance error: %s" % err_info)
            return

        valid_stock_pool = self.stage1_compute_data()
        if valid_stock_pool is None:
            return

        return self.stage2_filter_data(valid_stock_pool)


if __name__ == '__main__':
    gen_box = GenerateBox()
    valid_stock_box = gen_box.generate()
    # 保存股票盒
    _storage_box_data(data={"timestamp": common.get_current_timestamp(), "value": valid_stock_box})
    # 发送已经选的股票
    mail.send_mail(title=u"[%s] 选中的股票箱" % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))),
                   msg=_generate_box_mail_message(valid_stock_box))
