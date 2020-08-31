# Extract CSB files

from datetime import datetime, timezone
import itertools
import json
import os
import sys
import tarfile
from tarfile import TarFile
from typing import Any, Union
import dateutil.parser

import yaml
import app.spatialutil as spatial_util
import app.headerutil as header_util
import hashlib


class CsbCrawler:
    output_dir = ""
    data_dir = ""
    bucket = ""
    test_data_dir = ""
    manifest_dir = ""
    manifest_file = "manifest.txt"

    enable_upload = False
    overwrite_s3 = False
    access_key = ""
    secret_key = ""

    metadata = []

    @staticmethod
    def time_formatter(obs_time_str):
        try:
            obs_time = dateutil.parser.parse(obs_time_str)
            #obs_time = obs_time.replace(tzinfo=timezone.utc)
            obs_time = obs_time.astimezone(timezone.utc)
            obs_time_result = obs_time.isoformat("T", timespec="milliseconds")
            return str(obs_time_result).replace('+00:00', 'Z') # Use shorter UTC format
        except ValueError as ve:
            print("Invalid date format, skipping...")
            return None

    # Scan through json_meta for values with the key of search_key
    def find_values(self, search_key, json_meta):
        results = []
        if type(json_meta) == str:
            json_meta = json.loads(json_meta)
        if type(json_meta) is dict:
            for jsonkey in json_meta:
                if type(json_meta[jsonkey]) in (list, dict):
                    results.append(self.find_values(search_key, json_meta[jsonkey]))
                elif jsonkey == search_key:
                    #print( "Found " + id + " with value '" + json_meta[jsonkey] + "'")
                    results.append(json_meta[jsonkey])
        elif type(json_meta) is list:
            for item in json_meta:
                if type(item) in (list, dict):
                    results.append(self.find_values(search_key, item))
        return results

    def add_uuid_to_csv(self, tar, tar_info):
        csv_file = tar.extractfile(tar_info)
        print(f"csv_file='{csv_file}', type(csv_file)={type(csv_file)}")

        tf = tarfile.open(tar.name)
        print("tarfile contents:", tf.getnames())

        work_dir = self.output_dir + "working/"
        tar.extract(tar_info, work_dir)
        working_file = work_dir + os.path.basename(tar_info.name)
        print(f"working_file='{working_file}', type={type(working_file)}")

        #tar_item_name = self.data_dir + "input/" + os.path.basename(tar_info.name)
        #print(f"tar_item_name='{tar_item_name}', type={type(tar_item_name)}")
        #print(f"type(tar_info)={type(tar_info)}")

        # Gather header_map from csv_file to handle multiple formats
        try:
            header_map, first_data_line = header_util.get_xyz_header_map_and_data_line_number(working_file)
        except header_util.Error as err:
            print(f"Error parsing header: {err.message}")
            return None
        #finally:
        #    if (os.path.exists(working_file)):
        #        os.remove(working_file)

        file_name = os.path.basename(tar_info.name)
        # get unique_id, flattening the list if there are multiple results
        unique_ids = list(itertools.chain(*self.find_values('uniqueID', self.metadata)))
        unique_id = unique_ids[0]
        #print("uniqueID from metadata='" + unique_id + "'")

        print("Adding " + unique_id + " to csv")
        new_file_name = self.output_dir + "working/" + file_name[:-4] + ".csv"
        new_csv_file = open(new_file_name, "w+")
        new_csv_file.write("UUID,LON,LAT,DEPTH,TIME,PLATFORM_NAME,PROVIDER\n")
        # Skip header
        for cnt in range(first_data_line):
            csv_file.readline()
        print("cnt=", cnt)
        cnt = 1

        # Loop through csv_file and write info back out with unique_id included.
        for _ in csv_file:
            # TODO add more validation checks?
            line = (csv_file.readline()).decode("UTF-8").strip()
            tokens = line.split(",")

            if len(tokens) == 4:
                obs_time_str = tokens[3]
                obs_time = self.time_formatter(obs_time_str)
                if (obs_time != None):
                    new_line = unique_id + "," + tokens[1] + "," + tokens[0] + "," + tokens[2] + "," + obs_time + "," + self.metadata["platform"]["name"] + "," + self.metadata["providerContactPoint"]["orgName"]
                    #print("Line {}: {}".format(cnt, new_line))
                    new_csv_file.write(new_line + "\n")

            cnt += 1

        csv_file.close()
        new_csv_file.close()
        # Perform spatial join on new csv file
        join = spatial_util.spatial_join(self, new_csv_file.name)

        if join is not None:

            # Remove unnecessary columns
            pts_to_share = join[join['EXCLUDE'] != "Y"]
            pts_to_share = pts_to_share[['UUID', 'LON', 'LAT', 'DEPTH', 'TIME', 'PLATFORM_NAME', 'PROVIDER','EXCLUDE']]

            # Write back out as a csv
            print(pts_to_share['EXCLUDE'].count())
            if pts_to_share['EXCLUDE'].count() > 0:
                pts_to_share.to_csv(self.output_dir + "csv/" + file_name[0:-4] + ".csv", index=False)

    def parse_metadata(self, metadata_file):
        # Date is represented in filename YYYYMMDD
        file_name = os.path.basename(metadata_file.name)
        metadata = json.load(metadata_file)
        print(metadata)
        return metadata

    # Extract file
    def extract_metadata(self, tar):
        metadata = None
        for tar_info in tar:

            if tar_info.isreg() and tar_info.name[-13:] == "metadata.json":
                file = tar.extractfile(tar_info)
                metadata = self.parse_metadata(file)
                break

        return metadata

    def process_csv_files(self, tar):
        for tar_info in tar:

            if tar_info.isreg() and (tar_info.name[-4:] == ".xyz"):
                if tar_info.name[-4:] == ".xyz":
                    print("extracting... " + tar_info.name)
                    self.add_uuid_to_csv(tar, tar_info)

    def compute_md5sum(self, full_path):
        hash_md5 = hashlib.md5()
        with open(full_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def full_manifest_path(self, full_manifest_dir):
        return os.path.join(full_manifest_dir, self.manifest_file)

    def find_in_manifest(self, full_manifest_dir, file_match_string):
        pre_existing_item = False
        # make full_manifest_dir, then make manifest file if not present
        os.makedirs(full_manifest_dir, exist_ok=True)
        manifest_path = self.full_manifest_path(full_manifest_dir)
        # print("full_manifest_path='%s'" % full_manifest_path)
        if os.path.exists(manifest_path):
            # print("...manifest exists - search")
            with open(manifest_path, "r") as mfile:
                for line in mfile:
                    line = line.rstrip()
                    if line.startswith(file_match_string):
                        # print("Line already exists: " + line)
                        pre_existing_item = True
                        break
        return pre_existing_item

    # Print every file with its size recursing through dirs
    def recurse_dir(self, dir_path):
        for item in os.listdir(dir_path):
            item_full_path = os.path.join(dir_path, item)

            if os.path.isdir(item_full_path):
                self.recurse_dir(item_full_path)
            else:
                if item[-7:] == ".tar.gz":
                    # Get md5 checksum of file
                    md5sum = self.compute_md5sum(item_full_path)
                    # Get file stats
                    stat = os.stat(item_full_path)
                    isodate = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()

                    # fileinfo will be recorded in the manifest file
                    fileinfo = "%s, %s, %s, %s" % (item_full_path, md5sum, stat.st_size, isodate)
                    # Use file_match_string since we don't use the full fileinfo for identifying a match
                    file_match_string = "%s, %s," % (item_full_path, md5sum)

                    # pathName, md5sum, size, lastModDate
                    print(fileinfo)
                    # Check for pre-existing entry
                    pre_existing_item = False
                    # dir_path replaces previous args if absolute, so strip leading '/'
                    full_manifest_dir = os.path.join(self.manifest_dir, dir_path.lstrip("/"))
                    #print("full_manifest_dir ='%s'" % full_manifest_dir)

                    pre_existing_item = self.find_in_manifest(full_manifest_dir, file_match_string)
                    if not(pre_existing_item):
                        # Write manifest entry
                        manifest_path = self.full_manifest_path(full_manifest_dir)
                        with open(manifest_path, "a") as mfile:
                            mfile.write(fileinfo + "\n")
                        tar: Union[TarFile, Any] = tarfile.open(item_full_path, "r:gz")
                        self.metadata = self.extract_metadata(tar)
                        self.process_csv_files(tar)
                        tar.close()

    def __init__(self, root_dir):
        print("root_dir=" + root_dir)
        with open(root_dir + "/config/config.yaml") as f:
            docs = yaml.load(f, Loader=yaml.FullLoader)
            # Use config _dir values as-is if they are absolute (start with '/'), otherwise, they are relative to root_dir.
            self.output_dir    = docs["output_dir"]    if docs["output_dir"].startswith('/')    else os.path.join(root_dir, docs["output_dir"])
            self.data_dir      = docs["data_dir"]      if docs["data_dir"].startswith('/')      else os.path.join(root_dir, docs["data_dir"])
            self.test_data_dir = docs["test_data_dir"] if docs["test_data_dir"].startswith('/') else os.path.join(root_dir, docs["test_data_dir"])
            self.manifest_dir  = docs["manifest_dir"]  if docs["manifest_dir"].startswith('/')  else os.path.join(root_dir, docs["manifest_dir"])
            try:
                os.makedirs(self.manifest_dir, exist_ok = True)
                print("Top of manifest directory hierarchy is '%s'" % self.manifest_dir)
            except OSError as error:
                sys.exit("Manifest directory error: " + self.manifest_dir)
            self.enable_upload = docs["enable_upload"]
            print("Uploads enabled: %s" % (self.enable_upload))
            self.overwrite_s3 = docs["overwrite_s3"]
            print("Overwrite enabled: %s" % (self.overwrite_s3))
            self.bucket = docs["bucket"]

        # Load credentials
        with open(root_dir + "/config/credentials.yaml") as f:
            secrets = yaml.load(f, Loader=yaml.FullLoader)
            self.access_key = secrets["ACCESS_KEY"]
            self.secret_key = secrets["SECRET_KEY"]
