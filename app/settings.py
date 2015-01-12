# coding: utf-8

MERGE_JSON_DIR = "/home/ada/cyx/entitymerge"
MERGE_JSON_HDFS_DIR = '/tmp/sname-merge/'   # where to put merge-json on HDFS
MERGEJSON_NOT_EXIST_ERR = 0
OUTPUT = 'output'
QUIT = 'quit'
debug=True
MR_TASK = {
    '1': "celery-mr-task1.sh",  # 都是相对ada-merge 文件夹的路径
    '2': "celery-mr-task2.sh",
    '3': "celery-mr-task3.sh",
}
FROZEN_TIME = "140812"  # for test