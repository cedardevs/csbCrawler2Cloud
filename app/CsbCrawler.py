# Extract CSB files

from datetime import datetime
import json
import os
import tarfile
from tarfile import TarFile
from typing import Any, Union

import yaml


class CsbCrawler:
    output_dir = ""
    data_dir = ""
    bucket = ""
    test_data_dir = ""

    access_key = ""
    secret_key = ""

    @staticmethod
    def time_formatter(obs_time_str):
        try:
            obs_time = datetime.strptime(obs_time_str, '%Y%m%dT%H%M%SZ')
            return obs_time.isoformat("T", timespec="seconds")
        except ValueError as ve:
            print("Invalid date format, skipping...")
            return None

    def write_metadata_to_csv(self, metadata, csv_file_name):
        csv_file = open(self.output_dir + "metadata/" + csv_file_name, "w")
        csv_file.write("UUID,NAME,DATE,PROVIDER\n")
        csv_file.write(metadata["uuid"] + "," + metadata["platform"]["name"] + "," + metadata["date"] + "," +
                       metadata["providerContactPoint"]["orgName"])
        csv_file.close()

    def add_uuid_to_xyz(self, tar, tar_info):
        xyz_file = tar.extractfile(tar_info)
        file_name = os.path.basename(tar_info.name)
        uuid = file_name[9:41]

        print("Adding " + uuid + " to xyz")
        new_file_name = self.output_dir + "xyz/uuid_" + file_name
        new_xyz_file = open(new_file_name, "w+")
        new_xyz_file.write("UUID,LAT,LON,DEPTH,TIME\n")

        # Skip header
        xyz_file.readline()
        cnt = 1

        # Loop through xyz_file and write info back out with uuid included.
        for _ in xyz_file:
            # TODO add more validation checks?
            line = (xyz_file.readline()).decode("UTF-8").strip()
            tokens = line.split(",")

            if len(tokens) == 4:
                obs_time_str = tokens[3]
                obs_time = self.time_formatter(obs_time_str)
                if (obs_time != None):
                    new_line = uuid + "," + tokens[0] + "," + tokens[1] + "," + tokens[2] + "," + obs_time
                    print("Line {}: {}".format(cnt, new_line))
                    new_xyz_file.write(new_line + "\n")

            cnt += 1

        xyz_file.close()
        new_xyz_file.close()

    def parse_metadata(self, metadata_file):
        # Date is represented in filename YYYYMMDD
        file_name = os.path.basename(metadata_file.name)
        metadata = json.load(metadata_file)
        metadata["uuid"] = file_name[9:41]
        metadata["date"] = file_name[:8]
        csv_file_name = file_name[:-7] + "_metadata.csv"
        print(metadata)
        print("csv_file_name=" + csv_file_name)
        # Write out combined metadata to CSV
        self.write_metadata_to_csv(metadata, csv_file_name)
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

    def process_xyz_files(self, tar):
        for tar_info in tar:

            if tar_info.isreg() and (tar_info.name[-4:] == ".xyz"):
                if tar_info.name[-4:] == ".xyz":
                    print("extracting... " + tar_info.name)
                    self.add_uuid_to_xyz(tar, tar_info)

    # Print every file with its size recursing through dirs
    def recurse_dir(self, dir_path):
        for item in os.listdir(dir_path):
            item_full_path = os.path.join(dir_path, item)

            if os.path.isdir(item_full_path):
                self.recurse_dir(item_full_path)
            else:
                if item[-7:] == ".tar.gz":
                    print("%s - %s bytes" % (item_full_path, os.stat(item_full_path).st_size))
                    tar: Union[TarFile, Any] = tarfile.open(item_full_path, "r:gz")
                    metadata = self.extract_metadata(tar)
                    self.process_xyz_files(tar)
                    tar.close()

    def __init__(self, root_dir):
        print("root_dir=" + root_dir)
        with open(root_dir + "config/config.yaml") as f:
            docs = yaml.load(f, Loader=yaml.FullLoader)
            self.output_dir = docs["output_dir"]
            self.data_dir = docs["data_dir"]
            self.test_data_dir = docs["test_data_dir"]
            self.bucket = docs["bucket"]

        # Load credentials
        with open(root_dir + "config/credentials.yaml") as f:
            secrets = yaml.load(f, Loader=yaml.FullLoader)
            self.access_key = secrets["ACCESS_KEY"]
            self.secret_key = secrets["SECRET_KEY"]
