import os
from app.CsbCrawler import CsbCrawler

class LaunchApp:

    if __name__ == "__main__":
        root_dir = os.getenv("CSBCRAWLER")

        csbCrawler = CsbCrawler(root_dir)

        #Reprocess available local data
        csbCrawler.recurse_dir(csbCrawler.data_dir)

        #Upload metadata
        csbCrawler.upload_files_to_aws("metadata/")

        #Upload xyz
        csbCrawler.upload_files_to_aws("xyz/")