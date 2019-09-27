# Extract CSB files

import io
import json
import os
import tarfile

def parse_metadata(file):

    data = json.load(file)
    return data

# Add metadata
def add_metadata(file, metadata):

    print(file.readline())

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
                add_metadata(xyz_file, metadata)

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

