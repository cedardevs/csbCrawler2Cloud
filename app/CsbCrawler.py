# Extract CSB files

import boto3
from datetime import datetime
import json
import os
import tarfile
import yaml

class CsbCrawler:

    output_dir = ""
    data_dir = ""
    bucket = ""
    test_data_dir = ""

    access_key = ""
    secret_key = ""

    def upload_to_aws(self, local_file, bucket, s3_file):

        s3 = boto3.client('s3', aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key)

        try:
            s3.upload_file(local_file, bucket, s3_file)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False

    def upload_files_to_aws(self, sub_dir):
        #upload metadata
        print("output_dir=" + self.output_dir)
        upload_dir = self.output_dir + sub_dir
        for item in os.listdir(upload_dir):
            item_full_path = os.path.join(upload_dir, item)
            item_relative_path = sub_dir + item
            print(item_full_path + "; " + item_relative_path)
            self.upload_to_aws(item_full_path, self.bucket, item_relative_path)


    def write_metadata_to_csv(self, metadata, csv_file_name):
        csv_file = open(self.output_dir + "metadata/" + csv_file_name, "w")
        csv_file.write("UUID,NAME,DATE,PROVIDER\n")
        csv_file.write(metadata["uuid"] + "," + metadata["platform"]["name"] + "," + metadata["date"] + "," + metadata["providerContactPoint"]["orgName"])
        csv_file.close()

    def timeFormatter(self, obsTimeStr):
        try:
            obsTime = datetime.strptime(obsTimeStr, '%Y%m%dT%H%M%SZ')
            return obsTime.isoformat("T", timespec="seconds")
        except ValueError as ve:
            print("Invalid date format, skipping...")
            return None

    def add_uuid_to_xyz(self, tar, tar_info):
        xyz_file = tar.extractfile(tar_info)
        file_name = os.path.basename(tar_info.name)
        uuid = file_name[9:41]

        print("Adding " + uuid + " to xyz")
        new_file_name = self.output_dir + "xyz/uuid_" + file_name
        new_xyz_file = open(new_file_name,"w+")
        new_xyz_file.write("UUID,LAT,LON,DEPTH,TIME\n")

        #Skip header
        xyz_file.readline()
        cnt = 1

        #Loop through xyz_file and write info back out with uuid included.
        for line in xyz_file:
            #TODO add more validation checks?
            line = (xyz_file.readline()).decode("UTF-8").strip()
            tokens = line.split(",")

            if len(tokens) == 4:
                obsTimeStr = tokens[3]
                obsTime = self.timeFormatter(obsTimeStr)
                if (obsTime!=None):
                    newLine = uuid + "," + tokens[0] + "," + tokens[1] + "," + tokens[2] + "," + obsTime
                    print("Line {}: {}".format(cnt, newLine))
                    new_xyz_file.write(newLine + "\n")



            cnt += 1

        xyz_file.close()
        new_xyz_file.close()

    def parse_metadata(self, metadata_file):
        #Date is represented in filename YYYYMMDD
        file_name = os.path.basename(metadata_file.name)
        metadata = json.load(metadata_file)
        metadata["uuid"] = file_name[9:41]
        metadata["date"] = file_name[:8]
        csv_file_name = file_name[:-7] + "_metadata.csv"
        print(metadata)
        print("csv_file_name=" + csv_file_name)
        #Write out combined metadata to CSV
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

    def process_xyz_files(self, tar, metadata):
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
                    tar = tarfile.open(item_full_path, "r:gz")
                    metadata = self.extract_metadata(tar)
                    self.process_xyz_files(tar, metadata)
                    tar.close

    def __init__(self, root_dir):

        print("root_dir=" + root_dir)
        with open(root_dir + "config/config.yml") as f:
            docs = yaml.load(f, Loader=yaml.FullLoader)
            self.output_dir = docs["output_dir"]
            self.data_dir = docs["data_dir"]
            self.test_data_dir = docs["test_data_dir"]
            self.bucket = docs["bucket"]

        ## Load credentials
        with open(root_dir + "config/credentials.yml") as f:
            secrets = yaml.load(f, Loader=yaml.FullLoader)
            self.access_key = secrets["ACCESS_KEY"]
            self.secret_key = secrets["SECRET_KEY"]




