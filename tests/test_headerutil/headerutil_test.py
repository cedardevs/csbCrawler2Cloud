import unittest
import app.headerutil as header_util
from app.CsbCrawler import CsbCrawler

class TestHeaderUtilities(unittest.TestCase):
    data_filenames = [ # (input_file, expected_header_map, expected_data_line, expected_error)
        ('data-rosep-v1.xyz', {'lon':1, 'lat':0, 'depth':2, 'time':3}, 1, None),
        ('data-rosep-v2.xyz', {'lon':1, 'lat':0, 'depth':2, 'time':3}, 1, None),
        ('data-jcuau-v1.xyz', {'lon':0, 'lat':1, 'depth':2, 'time':3}, 1, None),
        ('data-farsnd-v1.xyz', {'lon':0, 'lat':1, 'depth':2, 'time':3}, 1, None),
        ('data-withLinesToIgnore.xyz', {'lon':0, 'lat':1, 'depth':2, 'time':3}, 5, None),
        ('data-withLongitudeLatitudeHeader.xyz', {'lon':0, 'lat':1, 'depth':2, 'time':3}, 2, None),
        ('data-withExtraColumns.xyz', {'lon':0, 'lat':1, 'depth':2, 'time':4}, 1, None),
        ('data-withTooMuchHeader.xyz', {'lon':0, 'lat':1, 'depth':2, 'time':3}, 0, header_util.MissingDataError),
        ('data-withNoHeader.xyz', {'lon':0, 'lat':1, 'depth':2, 'time':3}, 0, header_util.MissingHeaderError),
    ]

    def test_header_map(self):
        for test_line in self.data_filenames:
            input_file, expected_header_map, expected_data_line, expected_error = test_line
            print("filename='%s'" % input_file)
            if (expected_error != None):
                print("    Expected error thrown:", expected_error)
                with self.assertRaises(expected_error):
                    header_map, first_data_line = header_util.get_xyz_header_map_and_data_line_number(input_file)
            else:
                try:
                    header_map, first_data_line = header_util.get_xyz_header_map_and_data_line_number(input_file)
                    print("    header_map=" + str(header_map))
                    print("    first_data_line=%d" % first_data_line)
                    self.assertEqual(header_map, expected_header_map)
                    self.assertEqual(first_data_line, expected_data_line)
                except header_util.Error as err:
                    print("Error>> " + err.message)
                # except header_util.IncorrectHeaderError as err:
                #     print("IncorrectHeaderError>> " + err.message)
                # except header_util.MissingHeaderError as err:
                #     print("MissingHeaderError>> " + err.message)
                # except header_util.MissingDataError as err:
                #     print("MissingDataError>> " + err.message)

    def test_datetime_valid(self):
        test_data = [ # (input_string, expected_result, description of input)
            ('20180410T140006Z',            True, 'No punctuation'),
            ('20180410T140006.123Z',        True, 'No punctuation, fractional seconds'),
            ('2018-04-10T14:00:06Z',        True, 'With punctuation'),
            ('2018-04-10T14:00:06.123Z',    True, 'With punctuation, fractional seconds'),
            ('2018-04-10T14:00:06.12Z',     True, 'With punctuation, fractional seconds 2 decimals'),
            ('2018-04-10T14:00:06.1235Z',   True, 'With punctuation, fractional seconds 4 decimals'),
            ('20180410T140006+0000',        True, 'No punctuation, 0000 timezone offset'),
            ('2018-04-10T14:00:06+00:00',   True, 'With punctuation, 00:00 timezone offset'),
            ('2018-04-10T140006+00:00',     True, 'With some punctuation, 00:00 timezone offset'),
            ('2018-04-10T14:00:06-07:00',   True, 'With punctuation, -07:00 timezone offset'),
            ('2018-04-10T14:00:06+01:00',   True, 'With punctuation, +01:00 timezone offset'),
            ('20180410 140006Z',            False, 'No punctuation, no T separator'),
            ('20180410T',                   True, 'No punctuation, no time portion'),
            ('20180410TZ',                  False, 'No punctuation, no time portion except for timezone'),
            ('T140006Z',                    False, 'No punctuation, no date portion'),
            ('20180410',                    False, 'Just numbers'),
            ('abc',                         False, 'Just letters'),
            ('2018-04-10T14:00Z',           True, 'Time has no seconds'),
            ('2018-04-10T14Z',              True, 'Time is hour only'),
            ('2018-04T14:00:06Z',           True, 'Date has no day of month'), # if not specified, today's day of month is used
            ('2018T14:00:06Z',              True, 'Date is year only'), # if not specified, today's month and day are used
        ]

        # Loop over test data
        for test_line in test_data:
            input_string, expected_result, description = test_line
            print(f'"{input_string}" -> {expected_result}, {description}')
            result = header_util.datetime_valid(input_string)
            print('         Actual result', result)
            if result:
                print('         Interpreted as', CsbCrawler.time_formatter(input_string))
            self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()