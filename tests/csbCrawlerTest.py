import unittest
import app.csbCrawler

class csbCrawlerTest(unittest.TestCase):
    def test_parse_metadata(self):
        file_name = r"../data/20190626_8bfee6d7ec345d3b503a4ed3adc0288b_metadata.json"
        data = app.csbCrawler.parse_metadata(file_name)
        self.assertTrue(data['platform']['name'], "Joe Pyne")
        self.assertTrue(data['platform']['uniqueID'], "ROSEP-ffa635f8-7aa4-49d8-bc04-581457fb9e46")
        self.assertTrue(data['providerContactPoint']['orgName'], "Rose Point")

