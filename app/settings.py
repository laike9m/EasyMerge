MERGE_JSON_DIR = "/home/ada/cyx/entitymerge"
MERGE_JSON_HDFS_DIR = '/tmp/sname-merge/'   # where to put merge-json on HDFS
MERGEJSON_NOT_EXIST_ERR = 0
OUTPUT = 'output'
QUIT = 'quit'
debug=True
MR_TASK = {
    '1': 'ada-merge/celery-mr-task1.sh',
    '2': 'ada-merge/celery-mr-task2.sh',
    '3': 'ada-merge/celery-mr-task3.sh',
}
FROZEN_TIME = "140812"  # for test