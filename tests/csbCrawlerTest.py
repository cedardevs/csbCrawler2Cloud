import unittest
import tarfile
import os
from app.CsbCrawler import CsbCrawler

class csbCrawlerTest(unittest.TestCase):
    csbCrawler = None
    metadata_file_name = ""
    xyz_file_name = ""
    tar_file_name = ""

    def setUp(self):
        root_dir = os.getenv("CSBCRAWLER")
        self.csbCrawler = CsbCrawler(root_dir)
        self.metadata_file_name = self.csbCrawler.data_dir + "20190626_8bfee6d7ec345d3b503a4ed3adc0288b_metadata.json"
        self.xyz_file_name = self.csbCrawler.data_dir + "20190626_8bfee6d7ec345d3b503a4ed3adc0288b_pointData.xyz"
        self.tar_file_name = self.csbCrawler.data_dir + "20190626_8bfee6d7ec345d3b503a4ed3adc0288b.tar.gz"

    def tearDown(self):
        self.csbCrawler = None

    def test_parse_metadata(self):
        metadata_file = open(self.metadata_file_name, "r")
        metadata = self.csbCrawler.parse_metadata(metadata_file)
        metadata_file.close()
        self.assertTrue(metadata['platform']['name'], "Joe Pyne")
        self.assertTrue(metadata['platform']['uniqueID'], "ROSEP-ffa635f8-7aa4-49d8-bc04-581457fb9e46")
        self.assertTrue(metadata['providerContactPoint']['orgName'], "Rose Point")

    def test_process_file(self):
        metadata_file = open(self.metadata_file_name, "r")
        tar = tarfile.open(self.tar_file_name, "r:gz")
        metadata = self.csbCrawler.parse_metadata(metadata_file)
        self.csbCrawler.process_xyz_files(tar, metadata)
        # Close files
        metadata_file.close()
        tar.close()


    def test_checkDateISO(self):
        obsTime = self.csbCrawler.timeFormatter("20180410T140006Z")
        print(obsTime)
        self.assertEqual(str(obsTime), "2018-04-10T14:00:06")

