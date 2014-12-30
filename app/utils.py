# coding: utf-8
import xml.etree.ElementTree as ET
from subprocess import Popen, PIPE
from collections import OrderedDict
from os.path import dirname, abspath, join

ada_merge_dir = abspath(join(dirname(dirname(__file__)), 'ada-merge'))


def use_real_path(func):

    def new_func(path):
        real_path = join(ada_merge_dir, path)
        result = func(real_path)
        return result

    return new_func


@use_real_path
def get_config(filepath):
    kvs = OrderedDict()
    tree = ET.parse(filepath)
    root = tree.getroot()

    for child in root:
        if child[0].text == "core-site":
            core_site = child[1].text
        if child[0].text == "mapred-site":
            mapred_site = child[1].text
        if child[0].text == "hbase-site":
            hbase_site = child[1].text
        kvs[child[0].text] = child[1].text

    return kvs

    '''
    core_site_tree = ET.parse(core_site)
    mapred_site_tree = ET.parse(mapred_site)
    hbase_site_tree = ET.parse(hbase_site)

    for tree in (core_site_tree, mapred_site_tree, hbase_site_tree):
        for child in tree.getroot():
            root.append(child)

    ET.ElementTree(root).write("tmp.xml")
    '''
