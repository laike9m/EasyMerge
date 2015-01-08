# do NOT run this script directly, use tests-run.sh
# coding: utf-8
from unittest import TestCase
from os.path import join
from formencode.doctest_xml_compare import xml_compare
import xml.etree.ElementTree as ET
from app import utils


class TestUtil(TestCase):

    def setUp(self):
        self.root = "127.0.0.1:5000"
        self.test_file = 'conf/core-site.xml'
        self.get_xml_tree = lambda: ET.parse(join("ada-merge", self.test_file)).getroot()
        self.old_xml_tree = self.get_xml_tree()

    def test_xml_read_write(self):
        xml_content = utils.get_config(self.test_file)
        utils.update_config(self.test_file, xml_content)
        new_xml_tree = self.get_xml_tree()
        # pass element to xml_compare, not ElementTree
        self.assertEqual(True, xml_compare(self.old_xml_tree, new_xml_tree))

    def test_get_mergejson_path_on_hdfs(self):
        hdfspath = utils.get_mergejson_path_on_hdfs("aaa", "150315")
        self.assertEqual("/tmp/sname-merge/150315/aaa", hdfspath)
        hdfspath = utils.get_mergejson_path_on_hdfs("/ppp/aaa", "150315")
        self.assertEqual("/tmp/sname-merge/150315/ppp/aaa", hdfspath)
        hdfspath = utils.get_mergejson_path_on_hdfs("aaa/", "150315")
        self.assertEqual("/tmp/sname-merge/150315/aaa/", hdfspath)
        hdfspath = utils.get_mergejson_path_on_hdfs("/dsd/aaa/", "150315")
        self.assertEqual("/tmp/sname-merge/150315/dsd/aaa/", hdfspath)

    def test_get_mergejson_relative_path(self):
        hdfspath = "/tmp/sname-merge/150315/aaa"
        relative_path = utils.get_mergejson_relative_path(hdfspath)
        self.assertEqual("aaa", relative_path)
        hdfspath = "/otherpath/808012/aaa/2323"
        relative_path = utils.get_mergejson_relative_path(hdfspath)
        self.assertEqual("aaa/2323", relative_path)
        hdfspath = "/tmp/sname-merge/15031/aaa"
        relative_path = utils.get_mergejson_relative_path(hdfspath)
        self.assertEqual(None, relative_path)
