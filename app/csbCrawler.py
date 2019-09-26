# Extract CSB files

import io
import json
import os
import tarfile

def parse_metadata(file_name):
    with open(file_name, "r") as read_file:
        data = json.load(read_file)
    return data

# Extract item
def extract_item(item_full_path):
    tar = tarfile.open(item_full_path, "r:gz")
    for tar_info in tar:
        #print(tarinfo.name, "is", tarinfo.size, "bytes in size and is")

        if tar_info.isreg() and (tar_info.name[-4:] == ".xyz" or tar_info.name[-13:] == "metadata.json"):
            if tar_info.name[-4:] == ".xyz" or tar_info.name[-13:] == "metadata.json":
                print("extracting... " + tar_info.name)
                file = tar.extractfile(tar_info)
                print(file.readline())
    tar.close()
    return file


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
                csv_input = extract_item(item_full_path)
                print (csv_input.readline())

if __name__ == '__main__':
    # Get full size of home directory
    ## Switch to config file for data directory
    recurse_dir("data")

