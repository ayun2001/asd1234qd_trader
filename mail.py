# coding=utf-8

import json
import smtplib
from email.mime.text import MIMEText
from email.header import Header

import common
from log import Logger

mail_log_filename = "%s/%s" % (common.CONST_DIR_LOG, common.CONST_LOG_MAIL_FILENAME)
mail_config_filename = "%s/%s" % (common.CONST_DIR_CONF, common.CONST_CONFIG_MAIL_FILENAME)


def _load_config():
    if not common.file_exist(mail_config_filename):
        return None, "config file: %s is not exist."
    try:
        with open(mail_config_filename, "r") as _file:
            return json.load(_file), None
    except Exception as err:
        return None, err.message


def send_mail(title, msg):
    if not common.file_exist(common.CONST_DIR_LOG):
        common.create_directory(common.CONST_DIR_LOG)

    if not common.file_exist(common.CONST_DIR_CONF):
        common.create_directory(common.CONST_DIR_CONF)

    log = Logger(mail_log_filename, level='debug')

    config, err = _load_config()
    if config is None:
        log.logger.error("mail sender config load error: %s", err)
        return

    host = config.get("host", "localhost")
    port = config.get("port", 25)
    user = config.get("user", "root")
    pwd = config.get("pwd", "")
    sender = config.get("sender", "localhost")
    receivers = config.get("receivers", [])
    message = MIMEText(msg, 'plain', 'utf-8')
    message['Subject'] = Header(title, 'utf-8')

    try:
        smtp_instance = smtplib.SMTP()
        smtp_instance.connect(host, port)  # 25 为 SMTP 端口号
        smtp_instance.login(user, pwd)
        smtp_instance.sendmail(sender, receivers, message.as_string())
        log.logger.info("subject: %s mail has been sent." % title)
    except smtplib.SMTPException as err:
        log.logger.error("subject: %s mail send failed, error: %s" % (title, err.message))
