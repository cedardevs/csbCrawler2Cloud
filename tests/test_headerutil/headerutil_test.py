import app.headerutil as header_util

data_filenames = [
    'data-rosep-v1.xyz',
    'data-rosep-v2.xyz',
    'data-jcuau-v1.xyz',
    'data-withLinesToIgnore.xyz',
    'data-withTooMuchHeader.xyz',
    'data-withNoHeader.xyz',
]

for fn in data_filenames:
    print("### filename='%s'" % fn)
    try:
        header_map, first_data_line = header_util.get_xyz_header_map_and_data_line_number(fn)
        print("    header_map=" + str(header_map))
        print("    first_data_line=%d" % first_data_line)
    except header_util.IncorrectHeaderError as err:
        print("Error " + err.message)
    except header_util.MissingHeaderError as err:
        print("Error " + err.message)
    except header_util.MissingDataError as err:
        print("Error " + err.message)