# coding=utf-8

import talib

import common


class TA(object):
    def __init__(self):
        if not common.file_exist(common.CONST_DIR_LOG):
            common.create_directory(common.CONST_DIR_LOG)

    @staticmethod
    def make_macd_data(dataset, s=12, l=26, m=9):  # MACD数据一定要长时间的数据 30个数据点不够, 至少需要90, 这样数据才准
        try:
            dataset['macd_dif'] = dataset['close'].ewm(span=s).mean() - dataset['close'].ewm(span=l).mean()
            dataset['macd_dea'] = dataset['macd_dif'].ewm(span=m).mean()
            dataset['macd'] = 2 * (dataset['macd_dif'] - dataset['macd_dea'])
        except Exception as err:
            raise Exception, "add macd data error: %s" % err.message

    @staticmethod
    def make_macd_cross(dataset):
        try:
            dataset['macd_cross'] = ""
            macd_pos = dataset['macd_dif'] > dataset['macd_dea']  # 以K值小于D值为参考, 注意这里数据是最后一天在最后面
            dataset.loc[
                macd_pos[(macd_pos == True) & (macd_pos.shift(1) == False)].index, 'macd_cross'] = "up_cross"  # 金叉
            dataset.loc[
                macd_pos[(macd_pos == False) & (macd_pos.shift(1) == True)].index, 'macd_cross'] = "down_cross"  # 死叉
        except Exception as err:
            raise Exception, "add macd cross data error: %s" % err.message

    @staticmethod
    def make_kdj_data(dataset, n1=9, n2=3):
        try:
            lvds = dataset['low'].rolling(window=n1).min()
            lvds.fillna(value=dataset['low'].expanding().min(), inplace=True)
            hvds = dataset['high'].rolling(window=n1).max()
            hvds.fillna(value=dataset['close'].expanding().max(), inplace=True)
            rsv = (dataset['close'] - lvds) / (hvds - lvds) * 100
            dataset['kdj_k'] = rsv.ewm(com=2).mean()
            dataset['kdj_d'] = dataset['kdj_k'].ewm(com=2).mean()
            dataset['kdj_j'] = n2 * dataset['kdj_k'] - 2 * dataset['kdj_d']
        except Exception as err:
            raise Exception, "add kdj data error: %s" % err.message

    @staticmethod
    def make_kdj_cross(dataset):
        try:
            dataset['kdj_cross'] = ""
            kdj_pos = dataset['kdj_k'] > dataset['kdj_d']  # 以K值小于D值为参考, 注意这里数据是最后一天在最后面
            dataset.loc[kdj_pos[(kdj_pos == True) & (kdj_pos.shift(1) == False)].index, 'kdj_cross'] = "up_cross"  # 金叉
            dataset.loc[
                kdj_pos[(kdj_pos == False) & (kdj_pos.shift(1) == True)].index, 'kdj_cross'] = "down_cross"  # 死叉
        except Exception as err:
            raise Exception, "add kdj cross data error: %s" % err.message

    @staticmethod
    def make_change_data(dataset):
        try:
            dataset['change'] = dataset['close'].diff()  # 计算价格偏差
            dataset['pct_change'] = dataset['close'].pct_change() * 100  # 计算百分比
        except Exception as err:
            raise Exception, "add change data error: %s" % err.message

    @staticmethod
    def make_ma_data(dataset):
        try:
            dataset['ma5'] = dataset['close'].rolling(window=5).mean()
            dataset['ma10'] = dataset['close'].rolling(window=10).mean()
            dataset['ma20'] = dataset['close'].rolling(window=20).mean()
            dataset['ma30'] = dataset['close'].rolling(window=30).mean()
            dataset['ma60'] = dataset['close'].rolling(window=60).mean()
        except Exception as err:
            raise Exception, "add ma data error: %s" % err.message

    @staticmethod
    def make_rsi_data(dataset, n1=6, n2=12, n3=24):
        try:
            dataset['rsi6'] = talib.RSI(dataset['close'].values, n1)
            dataset['rsi12'] = talib.RSI(dataset['close'].values, n2)
            dataset['rsi24'] = talib.RSI(dataset['close'].values, n3)
        except Exception as err:
            raise Exception, "add rsi data error: %s" % err.message

    @staticmethod
    def make_rsi_cross(dataset):
        try:
            dataset['rsi_cross'] = ''
            rsi_pos = dataset['rsi12'] > dataset['rsi24']  # 以K值小于D值为参考, 注意这里数据是最后一天在最后面
            dataset.loc[rsi_pos[(rsi_pos == True) & (rsi_pos.shift(1) == False)].index, 'rsi_cross'] = "up_cross"  # 金叉
            dataset.loc[
                rsi_pos[(rsi_pos == False) & (rsi_pos.shift(1) == True)].index, 'rsi_cross'] = "down_cross"  # 死叉
        except Exception as err:
            raise Exception, "add rsi cross data error: %s" % err.message

    @staticmethod
    def make_cci_data(dataset):
        try:
            dataset['cci'] = talib.CCI(dataset['high'].values, dataset['low'].values, dataset['close'].values)
        except Exception as err:
            raise Exception, "add cci data error: %s" % err.message
