# Extract CSB files

import json
import os
import tarfile

def parse_metadata(file_item):
    return []

# Extract item
def extract_item(file_item):
    tar = tarfile.open(file_item, "r:gz")
    for tarinfo in tar:
        #print(tarinfo.name, "is", tarinfo.size, "bytes in size and is")

        if tarinfo.isreg():
            if tarinfo.name[-4:]== ".xyz" or tarinfo.name[-13:]== "metadata.json":
                print("extracting... " + tarinfo.name)
                tar.extract(tarinfo, "output", True)
    tar.close()


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
                extract_item(item_full_path)

if __name__ == '__main__':
    # Get full size of home directory
    ## Switch to config file for data directory
    recurse_dir("data")

