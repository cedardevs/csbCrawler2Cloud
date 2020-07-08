import os
from app.CsbCrawler import CsbCrawler
import app.awsutil as awsutil

root_dir = os.getenv("CSBCRAWLER")

csbCrawler = CsbCrawler(root_dir)

# Crawl and process files
csbCrawler.recurse_dir(csbCrawler.data_dir)

if csbCrawler.enable_upload:

  # Upload csv
  print("Uploading csv")
  awsutil.upload_files_to_aws(csbCrawler, "csv/", csbCrawler.overwrite_s3)



