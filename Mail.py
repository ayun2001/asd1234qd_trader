# coding=utf-8

import codecs
import json
import smtplib
import time
from email.header import Header
from email.mime.text import MIMEText

import Common
from Log import Logger

mail_log_filename = "%s/%s_%s" % (Common.CONST_DIR_LOG, time.strftime('%Y%m%d', time.localtime(time.time())),
                                  Common.CONST_LOG_MAIL_FILENAME)
mail_config_filename = "%s/%s" % (Common.CONST_DIR_CONF, Common.CONST_CONFIG_MAIL_FILENAME)


def _load_config():
    if not Common.file_exist(mail_config_filename):
        return None, u"配置文件: %s 不存在."
    try:
        with codecs.open(mail_config_filename, 'r', 'utf-8') as _file:
            return json.load(_file), None
    except Exception as err:
        return None, err.message


def send_mail(title, msg):
    if not Common.file_exist(Common.CONST_DIR_LOG):
        Common.create_directory(Common.CONST_DIR_LOG)

    if not Common.file_exist(Common.CONST_DIR_CONF):
        Common.create_directory(Common.CONST_DIR_CONF)

    log = Logger(mail_log_filename, level='debug')

    config, err = _load_config()
    if config is None:
        log.logger.error(u"邮件发送客户端配置文件加载错误: %s", err)
        return

    host = config.get("host", "localhost")
    port = config.get("port", 25)
    user = config.get("user", "root")
    pwd = Common.get_decrypted_string(config.get("pwd", ""))
    # pwd = config.get("pwd", "")
    sender = config.get("sender", "localhost")
    receivers = config.get("receivers", [])
    message = MIMEText(msg, 'html', 'utf-8')  # 'plain' 普通文本邮件， 'html' HTML邮件
    message['Subject'] = Header(title, 'utf-8')
    message['From'] = Header(sender, 'utf-8')  # 发送者
    message['To'] = Header(';'.join(receivers), 'utf-8')  # 接收者

    try:
        smtp_instance = smtplib.SMTP()
        smtp_instance.connect(host, port)  # 25 为 SMTP 端口号
        smtp_instance.login(user, pwd)
        smtp_instance.sendmail(sender, receivers, message.as_string())
        log.logger.info(u"主题: [%s] 的邮件已经被发送." % title)
    except Exception as err:
        log.logger.error(u"主题: [%s] 的邮件发送失败, 错误: %s" % (title, str(err)))


if __name__ == '__main__':
    send_mail("test", "<p>test</p>")
