# coding=utf-8

import random
import re

import TradeX2
import pandas as pd

import Common
from Algorithm import TA


# ============================================
# 行情接口函数

def _make_hq_query_index_list(count, step):
    numbers = int(count / step)
    if count - int(count / step) * step > 0:
        numbers += 1
    return [i * step for i in range(numbers)]


def check_stop_trade_stock(dataset):
    stock_trade_volume_list = dataset['volume'].values
    if len(stock_trade_volume_list) == 0 or stock_trade_volume_list[-1] == 0.0:
        return True
    else:
        return False


def create_connect_instance(config):
    try:
        host, port = random.choice(config["servers"]).split(':')
        port = int(port)
    except Exception as err:
        return None, u"行情配置信息关联错误: %s" % err.message.decode('gbk')

    try:
        instance = TradeX2.TdxHq_Connect(host, port)
        return instance, None
    except TradeX2.TdxHq_error as err:
        return None, err.message.decode('gbk')


def get_finance_info(instance, market, code):
    err_info, content = instance.GetFinanceInfo(market, code)
    if err_info != "":
        return None, u"获得股票: %s, 财务信息错误: %s" % (code, err_info.decode('gbk'))  # 这里一定要decode(gbk), 要不然后面报错
    else:
        return content, None


def get_stock_bars(instance, category, market, code, start, count):
    err_info, number, content = instance.GetSecurityBars(category, market, code, start, count)
    if err_info != "":
        return None, 0, u"股票: %s, K线数据错误: %s" % (code, err_info.decode('gbk'))  # 这里一定要decode(gbk), 要不然后面报错
    else:
        return content, number, None


def get_stock_codes(instance):
    sz_market = 0
    sh_market = 1

    stock_codes = {
        Common.CONST_SZ_MARKET: {'count': 0, 'desc': Common.MARKET_NAME_MAPPING[Common.CONST_SZ_MARKET],
                                 'values': []},
        Common.CONST_SH_MARKET: {'count': 0, 'desc': Common.MARKET_NAME_MAPPING[Common.CONST_SH_MARKET],
                                 'values': []},
        Common.CONST_ZX_MARKET: {'count': 0, 'desc': Common.MARKET_NAME_MAPPING[Common.CONST_ZX_MARKET],
                                 'values': []},
        Common.CONST_CY_MARKET: {'count': 0, 'desc': Common.MARKET_NAME_MAPPING[Common.CONST_CY_MARKET],
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
        return None, u"获得上海市场股票总数错误: %s" % err_info.decode('gbk')

    err_info, sz_stock_count = instance.GetSecurityCount(sz_market)
    if err_info != "":
        return None, u"获得深圳市场股票总数错误: %s" % err_info.decode('gbk')

    # 拉取整个上海股票列表
    index = 0
    err_info, sh_step, stock_code_content = instance.GetSecurityList(sh_market, 0)
    if err_info != "":
        return None, u"获得上海市场股票清单错误: [%d] %s" % (index, err_info.decode('gbk'))
    else:
        for line in stock_code_content.split('\n')[1:]:  # 循环中去掉第一行标题
            fields = line.split('\t')
            if len(fields) >= min_field_count and sh_market_stock_expr.match(
                    fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                stock_codes[Common.CONST_SH_MARKET]['values'].append(
                    {'code': fields[0], 'name': fields[2].decode('gbk')})  # 这里一定要decode(gbk), 要不然后面报错

    # 不用再拉第一份数据了, 已经有了
    for start in _make_hq_query_index_list(sh_stock_count, sh_step)[1:]:
        index += 1
        err_info, sh_step, stock_code_content = instance.GetSecurityList(sh_market, start)
        if err_info != "":
            return None, u"获得上海市场股票清单错误: [%d] %s" % (index, err_info.decode('gbk'))
        else:
            for line in stock_code_content.split('\n')[1:]:  # 循环中去掉第一行标题
                fields = line.split('\t')
                if len(fields) >= min_field_count and sh_market_stock_expr.match(
                        fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                    stock_codes[Common.CONST_SH_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2].decode('gbk')})

    # 拉取整个深圳股票列表
    index = 0
    err_info, sz_step, stock_code_content = instance.GetSecurityList(sz_market, 0)
    if err_info != "":
        return None, u"获得深圳市场股票清单错误: [%d] %s" % (index, err_info.decode('gbk'))
    else:
        for line in stock_code_content.split('\n')[1:]:  # 循环中去掉第一行标题
            fields = line.split('\t')
            if len(fields) >= min_field_count and sz_market_stock_expr.match(
                    fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                if zx_market_stock_expr.match(fields[0]) is not None:
                    stock_codes[Common.CONST_ZX_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2].decode('gbk')})
                elif cy_market_stock_expr.match(fields[0]) is not None:
                    stock_codes[Common.CONST_CY_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2].decode('gbk')})
                else:
                    stock_codes[Common.CONST_SZ_MARKET]['values'].append(
                        {'code': fields[0], 'name': fields[2].decode('gbk')})

    # 不用再拉第一份数据了, 已经有了
    for start in _make_hq_query_index_list(sz_stock_count, sz_step)[1:]:  # 循环中去掉第一行标题
        index += 1
        err_info, sz_step, stock_code_content = instance.GetSecurityList(sz_market, start)
        if err_info != "":
            return None, u"获得深圳市场股票清单错误: [%d] %s" % (index, err_info.decode('gbk'))
        else:
            for line in stock_code_content.split('\n')[1:]:
                fields = line.split('\t')
                if len(fields) >= min_field_count and sz_market_stock_expr.match(
                        fields[0]) is not None and st_stock_filter_expr.match(fields[2]) is None:
                    if zx_market_stock_expr.match(fields[0]) is not None:
                        stock_codes[Common.CONST_ZX_MARKET]['values'].append(
                            {'code': fields[0], 'name': fields[2].decode('gbk')})
                    elif cy_market_stock_expr.match(fields[0]) is not None:
                        stock_codes[Common.CONST_CY_MARKET]['values'].append(
                            {'code': fields[0], 'name': fields[2].decode('gbk')})
                    else:
                        stock_codes[Common.CONST_SZ_MARKET]['values'].append(
                            {'code': fields[0], 'name': fields[2].decode('gbk')})

    # 各板块股票数量统计
    stock_codes[Common.CONST_SH_MARKET]['count'] = len(stock_codes[Common.CONST_SH_MARKET]['values'])
    stock_codes[Common.CONST_SZ_MARKET]['count'] = len(stock_codes[Common.CONST_SZ_MARKET]['values'])
    stock_codes[Common.CONST_ZX_MARKET]['count'] = len(stock_codes[Common.CONST_ZX_MARKET]['values'])
    stock_codes[Common.CONST_CY_MARKET]['count'] = len(stock_codes[Common.CONST_CY_MARKET]['values'])

    # 返回数据
    return stock_codes, None


def get_history_data_frame(instance, market, market_desc, code, name, ktype=Common.CONST_K_DAY,
                           kcount=Common.CONST_K_LENGTH):
    min_k_type_field_count = 7
    # 获得公司流通总股本, 用来算换手率（注意是单位： 万股）
    finance_content, err_info = get_finance_info(instance, market, code)
    if err_info is not None:
        return None, err_info
    else:
        contents = finance_content.split('\n')
        if len(contents) < 2:
            return None, u"获得市场: %s, 股票:, %s 名称: %s, 数据结构不完整..." % (market_desc, code, name)
        try:
            circulating_equity_number = float(contents[1].split('\t')[2]) * 10000  # 变成标准股数
        except Exception as err:
            return None, u"获得市场: %s, 股票:, %s 名称: %s, 流通股总数错误: %s" % (market_desc, code, name, err.message)

    # 获得K线详细信息
    history_data_content, data_count, err_info = get_stock_bars(instance, ktype, market, code, 0, kcount * 3)
    if data_count <= 0:
        return None, u"获得市场: %s, 股票:, %s, 名称: %s, K数据总数不合法(<=0), 跳过..." % (market_desc, code, name)
    if err_info is not None:
        return None, err_info
    else:
        contents = history_data_content.split('\n')
        if len(contents) < 2:
            return None, u"获得市场: %s, 股票: %s, 名称: %s, K线数据结构不完整" % (market_desc, code, name)

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
                return None, u"获得市场: %s, 股票: %s, 名称: %s, K线数据元素不完整, 错误: %s" % (market_desc, code, name, err.message)

        # 生成数据集
        history_data_frame = pd.DataFrame(data_frame_spec_data_set)
        history_data_frame.set_index(['time'], inplace=True)

        # 检查股票是否停牌
        if check_stop_trade_stock(history_data_frame):
            return None, u"发现市场: %s, 股票: %s, 名称: %s, 已经停牌，跳过..." % (market_desc, code, name)

        try:
            # 添加ma5, ma10均线数据
            TA.make_ma_data(history_data_frame)
            # 添加价格变动
            TA.make_change_data(history_data_frame)
            # 添加 KDJ 数据属性
            TA.make_kdj_data(history_data_frame)
            # 添加 MACD 数据属性
            TA.make_macd_data(history_data_frame)
            # 添加 RSI 数据属性
            TA.make_rsi_data(history_data_frame)
            # 添加 CCI 数据属性
            TA.make_cci_data(history_data_frame)
            # 添加 cross 数据
            TA.make_macd_cross(history_data_frame)
            TA.make_kdj_cross(history_data_frame)
            TA.make_rsi_cross(history_data_frame)
        except Exception, err_info:
            return None, u"添加其他关键数据错误: %s" % err_info

        # 数据处理, 删除之前多预留出来的行数, 这个实用index获得连续索引, 注意取值长度
        drop_start = 0 - len(history_data_frame.index)
        drop_end = 0 - kcount
        history_data_frame.drop(history_data_frame[drop_start:drop_end].index, inplace=True)

        # 返回结果
        return history_data_frame, None


def get_stock_quotes(instance, dataset):
    err_info, quotes_count, quotes_content = instance.GetSecurityQuotes(dataset)
    if err_info != "":
        return None, err_info.decode('gbk')
    else:
        level5_quotes_data_set = {}
        for line in quotes_content.split('\n')[1:]:  # 循环中去掉第一行标题
            fields = line.split('\t')
            try:
                # 计算每一个戳和交易的总数，方便下单判断
                buy2_count = fields[19] + fields[23]
                buy3_count = buy2_count + fields[27]
                buy4_count = buy3_count + fields[31]
                buy5_count = buy4_count + fields[35]
                sell2_count = fields[20] + fields[24]
                sell3_count = sell2_count + fields[28]
                sell4_count = sell3_count + fields[32]
                sell5_count = sell4_count + fields[36]

                # 生成5档数据
                level5_quotes_data_set[fields[1]] = {
                    "buy1_price": fields[17], "buy1_value": fields[19], "buy1_count": fields[19],
                    "sell1_price": fields[18], "sell1_value": fields[20], "sell1_count": fields[20],
                    "buy2_price": fields[21], "buy2_value": fields[23], "buy2_count": buy2_count,
                    "sell2_price": fields[22], "sell2_value": fields[24], "sell2_count": sell2_count,
                    "buy3_price": fields[25], "buy3_value": fields[27], "buy3_count": buy3_count,
                    "sell3_price": fields[26], "sell3_value": fields[28], "sell3_count": sell3_count,
                    "buy4_price": fields[29], "buy4_value": fields[31], "buy4_count": buy4_count,
                    "sell4_price": fields[30], "sell4_value": fields[32], "sell4_count": sell4_count,
                    "buy5_price": fields[33], "buy5_value": fields[35], "buy5_count": buy5_count,
                    "sell5_price": fields[34], "sell5_value": fields[36], "sell5_count": sell5_count,
                }
            except Exception:
                continue
        return level5_quotes_data_set, None
