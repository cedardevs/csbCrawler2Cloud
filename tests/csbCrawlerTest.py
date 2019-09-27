import unittest
import tarfile
import app.csbCrawler

class csbCrawlerTest(unittest.TestCase):
    metadata_file_name = r"../data/20190626_8bfee6d7ec345d3b503a4ed3adc0288b_metadata.json"
    xyz_file_name = r"../data/20190626_8bfee6d7ec345d3b503a4ed3adc0288b_pointData.xyz"
    tar_file_name = r"../data/20190626_8bfee6d7ec345d3b503a4ed3adc0288b.tar.gz"

    def test_parse_metadata(self):
        metadata_file = open(self.metadata_file_name, "r")
        metadata = app.csbCrawler.parse_metadata(metadata_file)
        metadata_file.close()
        self.assertTrue(metadata['platform']['name'], "Joe Pyne")
        self.assertTrue(metadata['platform']['uniqueID'], "ROSEP-ffa635f8-7aa4-49d8-bc04-581457fb9e46")
        self.assertTrue(metadata['providerContactPoint']['orgName'], "Rose Point")

    def test_process_file(self):
        metadata_file = open(self.metadata_file_name, "r")
        tar = tarfile.open(self.tar_file_name, "r:gz")
        metadata = app.csbCrawler.parse_metadata(metadata_file)
        app.csbCrawler.process_tar(tar, metadata)
        # Close files
        metadata_file.close()
        tar.close()

if __name__ == '__main__':
    unittest.main()