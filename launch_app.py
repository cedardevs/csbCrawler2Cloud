import os
from app.CsbCrawler import CsbCrawler
import app.awsutil as awsutil

root_dir = os.getenv("CSBCRAWLER")

csbCrawler = CsbCrawler(root_dir)

# Crawl and process files
csbCrawler.recurse_dir(csbCrawler.data_dir)

if csbCrawler.enable_upload:
  # Upload metadata
  print("Uploading metadata")
  awsutil.upload_files_to_aws(csbCrawler, "metadata/")

  # Upload xyz
  print("Uploading xyz")
  awsutil.upload_files_to_aws(csbCrawler, "xyz/")



