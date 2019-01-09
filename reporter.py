# coding=utf-8

import common
from log import Logger

reporter_log_filename = "%s/%s" % (common.CONST_DIR_LOG, common.CONST_LOG_TRADER_FILENAME)


class Reporter(object):
    def __init__(self):
        if not common.file_exist(common.CONST_DIR_LOG):
            common.create_directory(common.CONST_DIR_LOG)

        self.log = Logger(reporter_log_filename, level='debug')

    def generate(self):
        pass


if __name__ == '__main__':
    pass
