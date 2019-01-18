# coding=utf-8

from optparse import OptionParser

import common

parser = OptionParser()
parser.add_option("-k", "--key", dest="key",
                  help="encrypt the key string to secret text")

options, args = parser.parse_args()

if options.key is None:
    print "please try do: python %s -h" % __file__
else:
    print common.get_encrypted_string(options.key)
