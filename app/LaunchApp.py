from app.CsbCrawler import CsbCrawler

class LaunchApp:

    if __name__ == "__main__":
        csbCrawler = CsbCrawler()
        #Upload metadata
        csbCrawler.upload_files_to_aws("metadata/")

        #Upload xyz
        csbCrawler.upload_files_to_aws("xyz/")