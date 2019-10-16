import os
from app.CsbCrawler import CsbCrawler

class LaunchApp:

    if __name__ == "__main__":
        root_dir = os.getenv("CSBCRAWLER")

        csbCrawler = CsbCrawler(root_dir)
        csbCrawler.recurse_dir(root_dir + "data/")

        #Upload metadata
        csbCrawler.upload_files_to_aws("metadata/")

        #Upload xyz
        csbCrawler.upload_files_to_aws("xyz/")