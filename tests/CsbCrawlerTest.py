import unittest
import tarfile
import os
from app.CsbCrawler import CsbCrawler
from app import spatialutil


class csbCrawlerTest(unittest.TestCase):
    csbCrawler = None
    metadata_file_name = ""
    csv_file_name = ""
    tar_file_name = ""

    def setUp(self):
        root_dir = os.getenv("CSBCRAWLER")
        self.csbCrawler = CsbCrawler(root_dir)
        self.metadata_file_name = self.csbCrawler.test_data_dir + "input/20190626_8bfee6d7ec345d3b503a4ed3adc0288b_metadata.json"
        self.csv_file_name = self.csbCrawler.test_data_dir + "input/20190626_8bfee6d7ec345d3b503a4ed3adc0288b_pointData.csv"
        self.tar_file_name = self.csbCrawler.test_data_dir + "input/20190626_8bfee6d7ec345d3b503a4ed3adc0288b.tar.gz"

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
        assert tarfile.is_tarfile(self.tar_file_name)
        metadata = self.csbCrawler.parse_metadata(metadata_file)
        self.csbCrawler.metadata = metadata # Unique ID will be needed by process_csb_files()
        self.csbCrawler.process_csv_files(tar)
        # Close files
        metadata_file.close()
        tar.close()

    def test_check_date_iso(self):
        test_data = [ # (input_string, expected_result, description of input)
            ('20180410T140006Z',        '2018-04-10T14:00:06.000Z', 'No punctuation'),
            ('20180410T140006.123Z',    '2018-04-10T14:00:06.123Z', 'No punctuation, fractional seconds'),
            ('2018-04-10T14:00:06Z',    '2018-04-10T14:00:06.000Z', 'With punctuation'),
            ('2018-04-10T14:00:06.123Z','2018-04-10T14:00:06.123Z', 'With punctuation, fractional seconds'),
            ('2018-04-10T14:00:06.12Z', '2018-04-10T14:00:06.120Z', 'With punctuation, fractional seconds 2 decimals'),
            ('2018-04-10T14:00:06.1234Z','2018-04-10T14:00:06.123Z', 'With punctuation, fractional seconds 4 decimals'),
            ('20180410T140006+0000',    '2018-04-10T14:00:06.000Z', 'No punctuation, 0000 timezone offset'),
            ('2018-04-10T14:00:06+00:00','2018-04-10T14:00:06.000Z', 'With punctuation, 00:00 timezone offset'),
            ('2018-04-10T140006+00:00', '2018-04-10T14:00:06.000Z', 'With some punctuation, 00:00 timezone offset'),
            ('2018-04-10T14:00:06-07:00','2018-04-10T21:00:06.000Z', 'With punctuation, -07:00 timezone offset'),
            ('2018-04-10T14:00:06+01:00','2018-04-10T13:00:06.000Z', 'With punctuation, +01:00 timezone offset'),
        ]

        # Loop over test data
        for test_line in test_data:
            input_string, expected_result, description = test_line
            print(f'"{input_string}" -> "{expected_result}", {description}')
            obs_time = CsbCrawler.time_formatter(input_string)
            print('         Actual result', obs_time)
            self.assertEqual(str(obs_time), expected_result)

    def test_spatial_join(self):
        file_name = "subset.csv"
        points_file_path = self.csbCrawler.test_data_dir + "reprocessed/csv/" + file_name

        spatial_join = spatialutil.spatial_join(self.csbCrawler, points_file_path)

        # Restrict based on EXCLUDE column values
        pts_to_share = spatial_join[spatial_join['EXCLUDE'] != "Y"]
        new_file_name = file_name[:-4] + "_exc.csv"
        if os.path.exists(self.csbCrawler.test_data_dir + "reprocessed/csv/" + new_file_name):
            os.remove(self.csbCrawler.test_data_dir + "reprocessed/csv/" + new_file_name)

        # Remove unnecessary columns
        pts_to_share = pts_to_share[['UUID', 'LAT', 'LON', 'DEPTH', 'TIME']]

        pts_to_share.to_csv(self.csbCrawler.test_data_dir + "reprocessed/csv/" + new_file_name, index=False)
