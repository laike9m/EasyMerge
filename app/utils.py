# coding: utf-8
import xml.etree.ElementTree as ET
from subprocess import Popen, PIPE
from collections import OrderedDict
from os.path import dirname, abspath, join
from itertools import ifilterfalse

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
        # for test, 测试的时候并不传所有配置项
        if child[0].text in configs:
            child[1].text = configs[child[0].text]

    ET.ElementTree(root).write(filepath)


def update_and_fetch_mrtask_script(configs):
    """
    task1, 除了最后一个输入参数merge-json 外都一样
    task2, 没有需要更新的部分
    task3, 最后两部分需要更新, 通道, 输出路径
    """
    with open(join(ada_merge_dir, 'celery-mr-task1.sh'), 'r+') as t1:
        with open(join(ada_merge_dir, 'celery-mr-task2.sh'), 'r+') as t2:
            with open(join(ada_merge_dir, 'celery-mr-task3.sh'), 'r+') as t3:
                # 记录行号, 方便之后写入
                t1_content = [l for l in t1.readlines() if not l.isspace()]
                t2_content = [l for l in t2.readlines() if not l.isspace()]
                t3_content = [l for l in t3.readlines() if not l.isspace()]
                t1.seek(0, 0)
                t2.seek(0, 0)
                t3.seek(0, 0)
                lineno1, task1_command = filter(
                    lambda x: x[1].startswith("hadoop"),
                    enumerate(t1_content)
                )[0]
                lineno2, task2_command = filter(
                    lambda x: x[1].startswith("hadoop"),
                    enumerate(t2_content)
                )[0]
                lineno3, task3_command = filter(
                    lambda x: x[1].startswith("hadoop"),
                    enumerate(t3_content)
                )[0]

                # merge-json could be '', if so, don't update task1 script
                if configs["merge-json"]:
                    merge_json = configs["merge-json"]
                    task1_command = task1_command.split()[:-1]
                    task1_command.append(merge_json)
                    task1_command = ' '.join(task1_command)

                # also works if len(channel)=1 or 0
                channel = ','.join(configs["channel"]) if configs["channel"] \
                    else task3_command.split()[-2]

                gdb_json = configs["gdb-json"] if configs["gdb-json"] \
                    else task3_command.split()[-1]

                task3_command = task3_command.split()[:-2]
                task3_command.append(channel)
                task3_command.append(gdb_json)
                task3_command = ' '.join(task3_command)

                modified_commands = {
                    'task1': task1_command,
                    'task2': task2_command,
                    'task3': task3_command
                }
                t1_content[lineno1] = task1_command
                t2_content[lineno2] = task2_command
                t3_content[lineno3] = task3_command
                t1.writelines(t1_content)
                t2.writelines(t2_content)
                t3.writelines(t3_content)
                return modified_commands
