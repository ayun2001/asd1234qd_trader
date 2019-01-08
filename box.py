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

    def stage1_compute_data(self):
        pass

    def stage2_filter_data(self):
        pass

    def generate(self):
        pass


if __name__ == '__main__':
    gen_box = GenerateBox()
    gen_box.generate()
