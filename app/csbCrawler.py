# Extract CSB files

import boto3
import json
import os
import tarfile
import yaml

project_dir = ""
output_dir = ""
data_dir = ""
bucket = ""

access_key = ""
secret_key = ""

def upload_to_aws(local_file, bucket, s3_file):

    s3 = boto3.client('s3', aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False

def upload_files_to_aws():
    #upload metadata
    for item in os.listdir(output_dir+"metadata/"):
        item_full_path = os.path.join(output_dir, item)
        item_relative_path = "metadata/" + item
        upload_to_aws(item_full_path, bucket, item_relative_path)


def write_metadata_to_csv(metadata, csv_file_name):
    csv_file = open(output_dir + "metadata/" + csv_file_name, "w")
    csv_file.write("UUID,NAME,DATE,PROVIDER\n")
    csv_file.write(metadata["uuid"] + "," + metadata["platform"]["name"] + "," + metadata["date"] + "," + metadata["providerContactPoint"]["orgName"])

def add_uuid_to_xyz(tar, tar_info):
    xyz_file = tar.extractfile(tar_info)
    file_name = os.path.basename(tar_info.name)
    uuid = file_name[9:41]

    print("Adding " + uuid + " to xyz")
    new_file_name = output_dir + "xyz/uuid_" + file_name
    new_xyz_file = open(new_file_name,"w+")
    new_xyz_file.write("UUID,LAT,LON,DEPTH,TIME\n")

    #Skip header
    xyz_file.readline()
    cnt = 1

    #Loop through xyz_file and write info back out with uuid included.
    for line in xyz_file:
        #TODO add more validation checks?
        line = uuid + "," + (xyz_file.readline()).decode("UTF-8").strip()
        tokens = line.split(",")
        timestamp = tokens[4]
        print (timestamp)

        if len(tokens) == 5:
            print("Line {}: {}".format(cnt, line))
            new_xyz_file.write(line + "\n")
        cnt += 1

    xyz_file.close()
    new_xyz_file.close()

def parse_metadata(metadata_file):
    #Date is represented in filename YYYYMMDD
    file_name = os.path.basename(metadata_file.name)
    metadata = json.load(metadata_file)
    metadata["uuid"] = file_name[9:41]
    metadata["date"] = file_name[:8]
    csv_file_name = file_name[:-7] + "_metadata.csv"
    print(metadata)
    print("csv_file_name=" + csv_file_name)
    #Write out combined metadata to CSV
    write_metadata_to_csv(metadata, csv_file_name)
    return metadata


# Extract file
def extract_metadata(tar):
    for tar_info in tar:

        if tar_info.isreg() and tar_info.name[-13:] == "metadata.json":
            file = tar.extractfile(tar_info)
            metadata = parse_metadata(file)
            break

    return metadata

def process_xyz_files(tar, metadata):
    for tar_info in tar:

        if tar_info.isreg() and (tar_info.name[-4:] == ".xyz"):
            if tar_info.name[-4:] == ".xyz":
                print("extracting... " + tar_info.name)
                add_uuid_to_xyz(tar, tar_info)

# Print every file with its size recursing through dirs
def recurse_dir(root_dir):

    root_dir = os.path.abspath(root_dir)
    for item in os.listdir(root_dir):
        item_full_path = os.path.join(root_dir, item)

        if os.path.isdir(item_full_path):
            recurse_dir(item_full_path)
        else:
            if item[-7:] == ".tar.gz":
                print("%s - %s bytes" % (item_full_path, os.stat(item_full_path).st_size))
                tar = tarfile.open(item_full_path, "r:gz")
                metadata = extract_metadata(tar)
                process_xyz_files(tar, metadata)
                tar.close

if __name__ == '__main__':
    # Get full size of home directory
    ## Switch to config file for data directory
    with open("/Users/dneufeld/Repos/csbCrawler2Cloud/config/config.yml") as f:
        docs = yaml.load(f, Loader=yaml.FullLoader)
        project_dir = docs["project_dir"]
        output_dir = docs["output_dir"]
        data_dir = docs["data_dir"]
        bucket = docs["bucket"]

    #recurse_dir(data_dir)

    ## Load credentials
    with open("/Users/dneufeld/Repos/csbCrawler2Cloud/config/credentials.yml") as f:
        secrets = yaml.load(f, Loader=yaml.FullLoader)
        access_key = secrets["ACCESS_KEY"]
        secret_key = secrets["SECRET_KEY"]


    #upload_files_to_aws()


