# coding=utf-8

import random
import time

import TradeX2

import Common


# ============================================
# 下单接口函数


def create_connect_instance(config):
    temp_last_selected_server_key = "order_75fayla1cEoD"
    try:
        while True:  # 需要利用config配置项目保存一个临时数据，这个数据只在运行过程中有效
            current_selected_server = random.choice(config["order_servers"])
            if current_selected_server == config.get(temp_last_selected_server_key) and \
                    len(config["order_servers"]) > 1:
                time.sleep(Common.CONST_SELECT_SERVER_INTERVAL)
                continue
            else:
                config[temp_last_selected_server_key] = current_selected_server
                break

        host, port = current_selected_server.split(':')
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
        return instance, u"地址: %s, 端口: %d" % (host, port)
    except TradeX2.error as err:
        return None, u"连接交易服务器错误: %s" % err.message


def send_stock_order(instance, code, account_id, action_id, uprice, count):
    # 使用  4 市价委托(上海五档即成剩撤/ 深圳五档即成剩撤) 这样的交易模式
    pass


def cancel_stock_order():
    pass


def send_new_ipo_stocks():
    pass


def get_history_trade_data():
    pass


def get_current_orders():
    pass
