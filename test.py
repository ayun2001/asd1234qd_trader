# coding=utf-8


import HQAdapter

instance, err = HQAdapter.create_connect_instance()

err_info, quotes_count, quotes_content = instance.GetSecurityQuotes([(0, "000001")])
a = []
for line in quotes_content.split("\n"):
    a.append(line.split('\t'))

for item in zip(a[0], a[1]):
    print item[0], item[1]
