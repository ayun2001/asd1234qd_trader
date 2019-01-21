# coding=utf-8

import random

import TradeX2

import Common


# ============================================
# 下单接口函数


def create_connect_instance(config):
    try:
        host, port = random.choice(config["servers"]).split(':')
        port = int(port)
        client_version = config["version"]
        client_branch_id = config["branch_id"]
        login_account_id = config["account_id"]
        trade_account_id = config["trade_id"]
        password = Common.get_decrypted_string(config["password"])
        tx_password = ""  # 这个默认留空就可以了
    except Exception as err:
        return None, u"交易配置信息关联错误: %s" % err.message

    try:
        instance = TradeX2.Logon(host, port, client_version, client_branch_id, login_account_id, trade_account_id,
                                 password, tx_password)
        return instance, None
    except TradeX2.error as err:
        return None, u"连接交易服务器错误: %s" % err.message


def send_stock_order(instance, code, account_id, action_id, uprice, count):
    pass


def cancel_stock_order():
    pass