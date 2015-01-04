# coding: utf-8
import xml.etree.ElementTree as ET
from subprocess import Popen, PIPE
from collections import OrderedDict
from os.path import dirname, abspath, join

ada_merge_dir = abspath(join(dirname(dirname(__file__)), 'ada-merge'))


def use_real_path(func):

    def new_func(path, *args):
        real_path = join(ada_merge_dir, path)
        result = func(real_path, *args)
        return result

    return new_func


@use_real_path
def get_config(filepath):
    kvs = OrderedDict()
    tree = ET.parse(filepath)
    root = tree.getroot()

    for child in root:
        kvs[child[0].text] = child[1].text

    return kvs


@use_real_path
def update_config(filepath, configs):
    print(filepath)
    config_xml_tree = ET.parse(filepath)
    root = config_xml_tree.getroot()

    for child in root:
        try:
            child[1].text = configs[child[0].text]
        except KeyError:
            print(child[0].text + "not in configs")

    ET.ElementTree(root).write(filepath)
