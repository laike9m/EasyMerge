# coding: utf-8
"""
launch flask serve before running tests
"""

import json
from unittest import TestCase
from os.path import dirname, abspath, join
import shutil
from os import remove
import requests
import arrow
from app.settings import *
from app.utils import set_ada_merge_dir


class MRCommandTest(TestCase):

    ada_merge_dir = abspath(join(dirname(dirname(__file__)), 'ada-merge'))
    date = FROZEN_TIME
    get_hdfs_path = lambda _, path: MERGE_JSON_HDFS_DIR+FROZEN_TIME+'/'+path
    root_url = 'http://127.0.0.1:5000'
    task_script1 = join(ada_merge_dir, 'celery-mr-task1.sh')
    task_script2 = join(ada_merge_dir, 'celery-mr-task2.sh')
    task_script3 = join(ada_merge_dir, 'celery-mr-task3.sh')

    # 如果有多个test method, setUp 和 tearDown 真的每次都会被调用, 而且不同 testmethod
    # 是独立的 instance, 这一点有待研究

    def setUp(self):
        shutil.copy(self.task_script1, self.task_script1+'.bak')
        shutil.copy(self.task_script2, self.task_script2+'.bak')
        shutil.copy(self.task_script3, self.task_script3+'.bak')

    def tearDown(self):
        shutil.copy(self.task_script1+'.bak', self.task_script1)
        shutil.copy(self.task_script2+'.bak', self.task_script2)
        shutil.copy(self.task_script3+'.bak', self.task_script3)
        remove(self.task_script1+'.bak')
        remove(self.task_script2+'.bak')
        remove(self.task_script3+'.bak')

    def test_update_and_fetch_command(self):
        resp = requests.post(
            url=self.root_url + '/config/',
            data={
                "core-site": "conf/core-site-35.xml",
                "mapred-site": "conf/mapred-site-35.xml",
                "hbase-site": "conf/hbase-site-35.xml",
                "merge-json": "merge-json",
                "gdb-json": "gdb-json",
                "channel": ["bbs", "web"],
            }
        )
        resp_data = resp.json()
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar"
                         " ict.ada.merge.loader.MergeJsonLoader -libjars"
                         " $libs -conf merge.xml 2>&1 " +
                         self.get_hdfs_path("merge-json"),
                         resp_data['task1'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.newid.MinIDJob -libjars $libs -conf "
                         "merge.xml 2>&1 merge-config.json",
                         resp_data['task2'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.dump.DumpJob -libjars $libs -files data"
                         " -conf merge.xml 2>&1 bbs,web gdb-json",
                         resp_data['task3'].strip())

        resp = requests.post(
            url=self.root_url + '/config/',
            data={
                "core-site": "conf/core-site-35.xml",
                "mapred-site": "conf/mapred-site-35.xml",
                "hbase-site": "conf/hbase-site-35.xml",
                "merge-json": "",
                "gdb-json": "",
                "channel": []
            }
        )
        resp_data = resp.json()
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar"
                         " ict.ada.merge.loader.MergeJsonLoader -libjars"
                         " $libs -conf merge.xml 2>&1 " +
                         self.get_hdfs_path("merge-json"),
                         resp_data['task1'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.newid.MinIDJob -libjars $libs -conf "
                         "merge.xml 2>&1 merge-config.json",
                         resp_data['task2'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.dump.DumpJob -libjars $libs -files data"
                         " -conf merge.xml 2>&1 bbs,web gdb-json",
                         resp_data['task3'].strip())


class ConfigUpdateTest(TestCase):

    root_url = 'http://127.0.0.1:5000'
    CONFIG_JSON = join(dirname(dirname(abspath(__file__))), "config.json")

    def setUp(self):
        shutil.copy(self.CONFIG_JSON, self.CONFIG_JSON+'.bak')
        self.original_ada_merge_dir = json.load(
            open(self.CONFIG_JSON))['ada_merge_dir']
        pass

    def tearDown(self):
        shutil.copy(self.CONFIG_JSON+'.bak', self.CONFIG_JSON)
        remove(self.CONFIG_JSON+'.bak')
        requests.get(self.root_url + "?ada_merge_dir=" +
                     self.original_ada_merge_dir)


    def test_set_ada_merge_dir(self):
        resp = requests.get(self.root_url+"?ada_merge_dir=xxx")
        self.assertIn("no merge.xml in directory: xxx", resp.text)
        new_ada_merge_dir = "/Users/laike9m/Desktop/ada-merge"
        requests.get(self.root_url + "?ada_merge_dir=" + new_ada_merge_dir)
        self.assertEqual(json.load(open(self.CONFIG_JSON))['ada_merge_dir'],
                         new_ada_merge_dir)