import os
import uuid
import boto3
import botocore


def objectkey_exists(s3, bucket, s3_file):
    exists = True
    try:
        s3 = boto3.resource('s3')
        object = s3.Object(bucket, s3_file)
        print("load object")
        object.load()
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = e.response['Error']['Code']
        if error_code == '404':
            exists = False
    return exists


def upload_to_aws(crawler, local_file, bucket, s3_file, overwrite):
    s3 = boto3.client('s3', aws_access_key_id=crawler.access_key,
                      aws_secret_access_key=crawler.secret_key)
    key_exists = False
    osim_uuid = str(uuid.uuid4())
    #print("osim_uuid ='%s'" % osim_uuid)

    if not overwrite:
        key_exists = objectkey_exists(s3, bucket, s3_file)

    if (not key_exists) or (key_exists and overwrite):
        try:
            s3.upload_file(local_file, bucket, s3_file,
                ExtraArgs={'Metadata': {'osim-uuid': osim_uuid}})
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False


def upload_files_to_aws(crawler, sub_dir, overwrite):
    # upload metadata
    print("output_dir=" + crawler.output_dir)
    upload_dir = crawler.output_dir + sub_dir
    for item in os.listdir(upload_dir):
        key = item[0:4] + "/" + item[4:6] + "/" + item[6:8] + "/" + item
        item_full_path = os.path.join(upload_dir, item)
        item_relative_path = sub_dir + key
        print("key=" + item_relative_path)
        print(item_full_path + "; " + item_relative_path)
        upload_to_aws(crawler, item_full_path, crawler.bucket, item_relative_path, overwrite)
