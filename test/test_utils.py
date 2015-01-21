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
