# coding=utf-8

import random
import re

import TradeX2
import pandas as pd

import common
from ta import TA


def _make_hq_query_index_list(count, step):
    numbers = int(count / step)
    if count - int(count / step) * step > 0:
        numbers += 1
    return [i * step for i in range(numbers)]


def check_stop_trade_stock(dataset):
    try:
        stock_trade_volume_list = dataset['volume'].values
        if len(stock_trade_volume_list) == 0 or stock_trade_volume_list[-1] == 0.0:
            return True, None
        else:
            return False, None
    except Exception as err:
        return False, "check stock was halted error: %s" % err.message


def create_connect_instance():
    try:
        host, port = random.choice(common.TDX_HQ_SERVER_LIST).split(':')
        port = int(port)
    except Exception:
        host = "101.227.73.20"
        port = 7709

    try:
        instance = TradeX2.TdxHq_Connect(host, port)
        return instance, None
    except TradeX2.TdxHq_error as err:
        return None, err.message


def get_finance_info(instance, market, code):
    err_info, content = instance.GetFinanceInfo(market, code)
    if err_info != "":
        return None, "get market: %d stock: %s finance information error: %s" % (market, code, err_info)
    else:
        return content, None


def get_stock_bars(instance, category, market, code, start, count):
    err_info, number, content = instance.GetSecurityBars(category, market, code, start, count)
    if err_info != "":
        return None, 0, "get stock: %s finance information error: %s" % (code, err_info)
    else:
        return content, number, None


def get_stock_codes(instance):
    sz_market = 0
    sh_market = 1

    stock_codes = {
        common.CONST_SZ_MARKET: {'count': 0, 'desc': common.MARKET_NAME_MAPPING[common.CONST_SZ_MARKET],
                                 'values': []},
        common.CONST_SH_MARKET: {'count': 0, 'desc': common.MARKET_NAME_MAPPING[common.CONST_SH_MARKET],
                                 'values': []},
        common.CONST_ZX_MARKET: {'count': 0, 'desc': common.MARKET_NAME_MAPPING[common.CONST_ZX_MARKET],
                                 'values': []},
        common.CONST_CY_MARKET: {'count': 0, 'desc': common.MARKET_NAME_MAPPING[common.CONST_CY_MARKET],
                                 'values': []},
    }

    min_field_count = 3
    sh_market_stock_expr = re.compile(r'^60[0-3][0-9]+$')  # 匹配上海市场股票
    sz_market_stock_expr = re.compile(r'^[03]0[0-9]+$')  # 匹配深圳市场股票
    zx_market_stock_expr = re.compile(r'^002[0-9]+$')  # 匹配中小板市场股票
    cy_market_stock_expr = re.compile(r'^30[0-9]+$')  # 匹配创业板市场股票
    st_stock_filter_expr = re.compile(r'^\**ST.+$')  # 匹配ST股票（ST, *ST, **ST）

    err_info, sh_stock_count = instance.GetSecurityCount(sh_market)
    if err_info != "":
        return False, None, "get SH market stock number error: %s" % err_info

    err_info, sz_stock_count = instance.GetSecurityCount(sz_market)
    if err_info != "":
        return False, None, "get SZ market stock number error: %s" % err_info

    # 拉取整个上海股票列表
    index = 0
    err_info, sh_step, stock_code_content = instance.GetSecurityList(sh_market, 0)
    if err_info != "":
        return False, None, "get SH market stocks list error: [%d] %s" % (index, err_info)
    else:
        for line in stock_code_content.split('\n')[1:]:  # 循环中去掉第一行标题
            fields = line.split('\t')
            if len(fields) >= min_field_count and sh_market_stock_expr.match(
                    fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                stock_codes[common.CONST_SH_MARKET]['values'].append(
                    {'code': fields[0], 'name': fields[2]})

    # 不用再拉第一份数据了, 已经有了
    for start in _make_hq_query_index_list(sh_stock_count, sh_step)[1:]:
        index += 1
        err_info, sh_step, stock_code_content = instance.GetSecurityList(sh_market, start)
        if err_info != "":
            return False, None, "get SH market stocks list error: [%d] %s" % (index, err_info)
        else:
            for line in stock_code_content.split('\n')[1:]:  # 循环中去掉第一行标题
                fields = line.split('\t')
                if len(fields) >= min_field_count and sh_market_stock_expr.match(
                        fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                    stock_codes[common.CONST_SH_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2]})

    # 拉取整个深圳股票列表
    index = 0
    err_info, sz_step, stock_code_content = instance.GetSecurityList(sz_market, 0)
    if err_info != "":
        return False, None, "get SZ market stocks list error: [%d] %s" % (index, err_info)
    else:
        for line in stock_code_content.split('\n')[1:]:  # 循环中去掉第一行标题
            fields = line.split('\t')
            if len(fields) >= min_field_count and sz_market_stock_expr.match(
                    fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                if zx_market_stock_expr.match(fields[0]) is not None:
                    stock_codes[common.CONST_ZX_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2]})
                elif cy_market_stock_expr.match(fields[0]) is not None:
                    stock_codes[common.CONST_CY_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2]})
                else:
                    stock_codes[common.CONST_SZ_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2]})

    # 不用再拉第一份数据了, 已经有了
    for start in _make_hq_query_index_list(sz_stock_count, sz_step)[1:]:  # 循环中去掉第一行标题
        index += 1
        err_info, sz_step, stock_code_content = instance.GetSecurityList(sz_market, start)
        if err_info != "":
            return False, None, "get SZ market stocks list error: [%d] %s" % (index, err_info)
        else:
            for line in stock_code_content.split('\n')[1:]:
                fields = line.split('\t')
                if len(fields) >= min_field_count and sz_market_stock_expr.match(
                        fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                    if zx_market_stock_expr.match(fields[0]) is not None:
                        stock_codes[common.CONST_ZX_MARKET]['values'].append(
                            {'code': fields[0], 'name': fields[2]})
                    elif cy_market_stock_expr.match(fields[0]) is not None:
                        stock_codes[common.CONST_CY_MARKET]['values'].append(
                            {'code': fields[0], 'name': fields[2]})
                    else:
                        stock_codes[common.CONST_SZ_MARKET]['values'].append(
                            {'code': fields[0], 'name': fields[2]})

    # 各板块股票数量统计
    stock_codes[common.CONST_SH_MARKET]['count'] = len(stock_codes[common.CONST_SH_MARKET]['values'])
    stock_codes[common.CONST_SZ_MARKET]['count'] = len(stock_codes[common.CONST_SZ_MARKET]['values'])
    stock_codes[common.CONST_ZX_MARKET]['count'] = len(stock_codes[common.CONST_ZX_MARKET]['values'])
    stock_codes[common.CONST_CY_MARKET]['count'] = len(stock_codes[common.CONST_CY_MARKET]['values'])

    # 返回数据
    return True, stock_codes, None


def get_history_data_frame(instance, market, code, ktype=common.CONST_K_DAY, kcount=common.CONST_K_LENGTH):
    min_k_type_field_count = 7
    # 获得公司流通总股本, 用来算换手率（注意是单位： 万股）
    finance_content, err_info = get_finance_info(instance, market, code)
    if err_info != "":
        return False, None, err_info
    else:
        contents = finance_content.split('\n')
        if len(contents) < 2:
            return False, None, "get stock: %s data incomplete." % code
        try:
            circulating_equity_number = float(contents[1].split('\t')[2]) * 10000  # 变成标准股数
        except Exception as err:
            return False, None, "get stock: %s total number error: %s" % (code, err.message)

    # 获得K线详细信息
    err_info, data_count, history_data_content = get_stock_bars(instance, ktype, market, code, 0, kcount * 3)
    if data_count <= 0:
        return False, None, "stock is not being listed. skipping..." % code
    if err_info != "":
        return False, None, err_info
    else:
        contents = history_data_content.split('\n')
        if len(contents) < 2:
            return False, None, "get stock: %s data incomplete." % code

        data_frame_spec_data_set = []
        for line in contents[1:]:  # 去掉标题头
            fields = line.split('\t')
            if len(fields) < min_k_type_field_count:
                continue
            try:
                data_frame_spec_data_set.append(
                    {'time': fields[0], 'open': float(fields[1]), 'close': float(fields[2]),
                     'high': float(fields[3]), 'low': float(fields[4]), 'volume': float(fields[5]),
                     'pvolume': float(fields[6]), 'turnover': float(fields[5]) * 100 / circulating_equity_number})
            except Exception as err:
                return False, None, "get stock: %s data is not enough, error: %s" % (code, err.message)

        # 生成数据集
        history_data_frame = pd.DataFrame(data_frame_spec_data_set)
        history_data_frame.set_index(['time'], inplace=True)

        # 检查股票是否停牌
        if check_stop_trade_stock(history_data_frame):
            return False, None, "the stock: %s was halted, skipping..." % code

        ta_instance = TA()
        # 添加ma5, ma10均线数据
        ta_instance.make_ma_data(history_data_frame)
        # 添加价格变动
        ta_instance.make_change_data(history_data_frame)
        # 添加 KDJ 数据属性
        ta_instance.make_kdj_data(history_data_frame)
        # 添加 MACD 数据属性
        ta_instance.make_macd_data(history_data_frame)
        # 添加 RSI 数据属性
        ta_instance.make_rsi_data(history_data_frame)
        # 添加 CCI 数据属性
        ta_instance.make_cci_data(history_data_frame)
        # 添加 cross 数据
        ta_instance.make_macd_cross(history_data_frame)
        ta_instance.make_kdj_cross(history_data_frame)
        ta_instance.make_rsi_cross(history_data_frame)

        # 数据处理, 删除之前多预留出来的行数, 这个实用index获得连续索引, 注意取值长度
        drop_start = 0 - len(history_data_frame.index)
        drop_end = 0 - kcount
        history_data_frame.drop(history_data_frame[drop_start:drop_end].index, inplace=True)

        # 返回结果
        return True, history_data_frame, None