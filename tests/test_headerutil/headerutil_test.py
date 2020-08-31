import unittest
import app.headerutil as header_util

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
            print("### filename='%s'" % input_file)
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

if __name__ == '__main__':
    unittest.main()