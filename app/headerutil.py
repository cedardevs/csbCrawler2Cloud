import re

def parse_header_line(header_line):
    header_map = {}
    column_headers = re.split('[, ]+', header_line) # comma and/or space
    #print("column_headers=" + str(column_headers))
    for i, header in enumerate(column_headers):
        lc_header = header.lower()
        if lc_header in ['lon', 'long', 'longitude']:
            header_map['lon'] = i
        elif lc_header in ['lat', 'latitude']:
            header_map['lat'] = i
        elif lc_header == 'depth':
            header_map['depth'] = i
        elif lc_header == 'time':
            header_map['time'] = i
    return header_map

# header_map, first_data_line = header_util.get_xyz_header_map_and_data_line_number(csv_filename)
def get_xyz_header_map_and_data_line_number(csv_filename):
    #print("In get_xyz_header_map_and_data_line_number")
    default_xyz_header = 'lat,lon,depth,time'
    header_map = {}
    first_data_line = -1
    header_line = ''
    max_xyz_lines_to_scan = 10
    line_index = 0
    with open(csv_filename, "r") as csv_file:
        for line in csv_file:
            if line_index >= max_xyz_lines_to_scan or first_data_line >= 0:
                break
            line = line.strip()
            if not line == '': # skip blank lines
                # A header must contain at least 'lat' and 'lon'
                if "lat" in line.lower() and "lon" in line.lower():
                    header_line = line
                    header_map = parse_header_line(header_line)
                    #print(f"get_xyz_header_map_and_data_line_number(): parsed header line='{line}' into indexMap:{str(header_map)}")
                else:
                    # Data lines have at least 2 numeric tokens
                    tokens = re.split('[, ]+', line)
                    numeric_tokens = []
                    for t in tokens:
                        try:
                            numeric_tokens.append(float(t))
                        except ValueError:
                            pass
                    numeric_count = len(numeric_tokens)
                    #print(f"not a header, looking for data with numeric_count >= 2, numeric_count={numeric_count} for line '{line}'")
                    if numeric_count >= 2:
                        first_data_line = line_index
                        if header_line == '':
                            raise MissingHeaderError(f"Data encountered before a header was found in '{csv_filename}'")
                        # if 'time' is in the header, confirm the time column contains the letter 'T' (ISO 8601)
                        if 'time' in header_map:
                            if 'T' in tokens[header_map['time']]:
                                pass
                                #print(f"get_xyz_header_map_and_data_line_number(): data line='{line}' will be matched to header_map:{header_map}'")
                            else:
                                #print(f"get_xyz_header_map_and_data_line_number(): WARN header line='{header_line}' did not match data, using default header '{default_xyz_header}'")
                                header_map = parse_header_line(default_xyz_header)
                                if not 'T' in tokens[header_map['time']]:
                                    raise IncorrectHeaderError(f"Header and default header did not match the time column in file '{csv_filename}'")
            line_index += 1
    #print("...result=")
    #print([header_map, first_data_line])
    if not 'lat' in header_map:
        raise MissingHeaderError(f"No header found at the beginning of '{csv_filename}'")
    if first_data_line < 0:
        raise MissingDataError(f"No data found (file empty?) within the first {max_xyz_lines_to_scan} lines of csv file '{csv_filename}'")
    return header_map, first_data_line


class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class IncorrectHeaderError(Error):
    """Exception raised if csv file header does not match time field

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        self.message = message

class MissingHeaderError(Error):
    """Exception raised if csv file lacks a header

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        self.message = message

class MissingDataError(Error):
    """Exception raised if csv file does not have data near the start of the file

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        self.message = message