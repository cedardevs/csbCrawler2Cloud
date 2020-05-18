import os
import boto3

def upload_to_aws(crawler, local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=crawler.access_key,
                      aws_secret_access_key=crawler.secret_key)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False


def upload_files_to_aws(crawler, sub_dir):
    # upload metadata
    print("output_dir=" + crawler.output_dir)
    upload_dir = crawler.output_dir + sub_dir
    for item in os.listdir(upload_dir):
        item_full_path = os.path.join(upload_dir, item)
        item_relative_path = sub_dir + item
        print(item_full_path + "; " + item_relative_path)
        upload_to_aws(crawler, item_full_path, crawler.bucket, item_relative_path)
