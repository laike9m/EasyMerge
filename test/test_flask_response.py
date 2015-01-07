# coding: utf-8
"""
launch flask serve before running tests
"""

import requests
from unittest import TestCase
from os.path import dirname, abspath, join
import shutil
from os import remove


class FlaskTest(TestCase):

    ada_merge_dir = abspath(join(dirname(dirname(__file__)), 'ada-merge'))
    root_url = 'http://127.0.0.1:5000'
    task_script1 = join(ada_merge_dir, 'celery-mr-task1.sh')
    task_script2 = join(ada_merge_dir, 'celery-mr-task2.sh')
    task_script3 = join(ada_merge_dir, 'celery-mr-task3.sh')

    def setUp(self):
        with open(self.task_script1) as t1:
            self.t1_content = t1.read()
        with open(self.task_script2) as t2:
            self.t2_content = t2.read()
        with open(self.task_script3) as t3:
            self.t3_content = t3.read()
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
                "merge-json": "",
                "gdb-json": "",
                "channel": [],
            }
        )
        resp_data = resp.json()
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar"
                         " ict.ada.merge.loader.MergeJsonLoader -libjars"
                         " $libs -conf merge.xml /tmp/sname-merge",
                         resp_data['task1'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.newid.MinIDJob -libjars $libs -conf "
                         "merge.xml merge-config.json",
                         resp_data['task2'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.dump.DumpJob -libjars $libs -files data"
                         " -conf merge.xml baidubaike,wikibaike,web,news,bbs,"
                         "weibo,hudongbaike /tmp/gdb-json",
                         resp_data['task3'].strip())

        resp = requests.post(
            url=self.root_url + '/config/',
            data={
                "core-site": "conf/core-site-35.xml",
                "mapred-site": "conf/mapred-site-35.xml",
                "hbase-site": "conf/hbase-site-35.xml",
                "merge-json": "test_merge.json",
                "gdb-json": "test_gdb.json",
                "channel": ["bbs", "web"],
            }
        )
        resp_data = resp.json()
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar"
                         " ict.ada.merge.loader.MergeJsonLoader -libjars"
                         " $libs -conf merge.xml test_merge.json",
                         resp_data['task1'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.newid.MinIDJob -libjars $libs -conf "
                         "merge.xml merge-config.json",
                         resp_data['task2'].strip())
        self.assertEqual(u"hadoop jar target/ada-merge-0.1-SNAPSHOT.jar "
                         "ict.ada.merge.dump.DumpJob -libjars $libs -files data"
                         " -conf merge.xml bbs,web test_gdb.json",
                         resp_data['task3'].strip())

    def test_write_mrtask_script(self):
        requests.post(
            url=self.root_url + '/config/',
            data={
                "core-site": "conf/core-site-35.xml",
                "mapred-site": "conf/mapred-site-35.xml",
                "hbase-site": "conf/hbase-site-35.xml",
                "merge-json": "/tmp/sname-merge",
                "gdb-json": "/tmp/gdb-json",
                "channel": ["baidubaike","wikibaike","web","news","bbs",
                            "weibo","hudongbaike"],
            }
        )
        with open(join(self.ada_merge_dir, 'celery-mr-task1.sh')) as t1:
            self.assertEqual(t1.read(), self.t1_content)
        with open(join(self.ada_merge_dir, 'celery-mr-task2.sh')) as t2:
            self.assertEqual(t2.read(), self.t2_content)
        with open(join(self.ada_merge_dir, 'celery-mr-task3.sh')) as t3:
            self.assertEqual(t3.read(), self.t3_content)
