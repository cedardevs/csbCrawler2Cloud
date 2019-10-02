# Extract CSB files

import io
import json
import os
import tarfile

def write_metadata_to_csv(metadata, csv_file_name):
    csv_file = open("output/" + csv_file_name, "w")
    csv_file.write("UUID,NAME,DATE,PROVIDER\n")
    csv_file.write(metadata["uuid"] + "," + metadata["platform"]["name"] + "," + metadata["date"] + "," + metadata["providerContactPoint"]["orgName"])

def add_uuid_to_xyz(xyz_file):
    file_name = os.path.basename(xyz_file.name)
    uuid = file_name[9:41]
    print("Adding " + uuid + " to xyz")
    #Loop through xyz_file and write info back out with uuid included.
    line = uuid + "," + (xyz_file.readline()).decode("UTF-8")
    cnt = 1
    while line:
        print("Line {}: {}".format(cnt, line.strip()))
        line = uuid + "," + (xyz_file.readline()).decode("UTF-8")
        cnt += 1

def parse_metadata(metadata_file):
    #Date is represented in filename YYYYMMDD
    file_name = os.path.basename(metadata_file.name)
    metadata = json.load(metadata_file)
    metadata["uuid"] = file_name[9:41]
    metadata["date"] = file_name[:8]
    csv_file_name = file_name[:-6] + "csv"
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

def process_tar(tar, metadata):
    for tar_info in tar:

        if tar_info.isreg() and (tar_info.name[-4:] == ".xyz"):
            if tar_info.name[-4:] == ".xyz":
                print("extracting... " + tar_info.name)
                xyz_file = tar.extractfile(tar_info)
                add_uuid_to_xyz(xyz_file)

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
                process_tar(tar, metadata)
                tar.close

if __name__ == '__main__':
    # Get full size of home directory
    ## Switch to config file for data directory
    recurse_dir("data")

