# coding=utf-8

import common
from log import Logger

_box_log_filename = "%s/%s" % (common.CONST_DIR_LOG, common.CONST_LOG_BOX_FILENAME)


class GenerateBox(object):
    def __init__(self):
        if not common.file_exist(common.CONST_DIR_LOG):
            common.create_directory(common.CONST_DIR_LOG)

        if not common.file_exist(common.CONST_DIR_DATABASE):
            common.create_directory(common.CONST_DIR_DATABASE)

        self.log = Logger(_box_log_filename, level='debug')

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
    def _compute_task_handler(self, source, input_data=None, output_dataset=None):
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

        ok, history_data_frame = source.get_history_data_frame(market=market_code, code=stock_code,
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

    def stage1_compute_data(self):
        pass

    def stage2_filter_data(self):
        pass

    def generate(self):
        pass


if __name__ == '__main__':
    gen_box = GenerateBox()
    gen_box.generate()
