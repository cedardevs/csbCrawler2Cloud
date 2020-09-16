# csbCrawler2Cloud
Python script to load csb data to s3 buckets

### Environment variable
Assumes
 -  `CSBCRAWLER` is set to the root of this project
    - If you are at the project root, run 
    ```bash
       $ export CSBCRAWLER=`pwd`
    ```
    - To confirm the current value
    ```bash
       $ printenv CSBCRAWLER
       /Users/username/src/github/cedardevs/csbCrawler2Cloud
    ```

### Libraries
[Pipenv](https://pipenv-fork.readthedocs.io/en/latest/) is used to manage external libraries. `Pipfile` contains the list of dependencies under the [packages] section.

Verify python version 3.7+
```bash
pipenv run python --version
```

Install spatialindex (on Mac) to support RTree dependency (use homebrew, pipenv didn't find it)
```bash
brew install spatialindex
```

Install all dependencies "[Packages]" from the Pipfile
```bash
pipenv install
```
If the above install does not work, try installing the the dependencies individually
```bash
pipenv install boto3
pipenv install rtree
pipenv install shapely
pipenv install pyyaml
pipenv install geopandas
```

Run app
```bash
pipenv run python launch_app.py
```
#### Troubleshooting
These environment variables should be set by your ~/.profile (or ~/.bash_profile)
```bash
export LC_ALL='en_US.UTF-8'
export LANG='en_US.UTF-8'
```

### Upload to AWS
 - Requires credentials.yaml
   ACCESS_KEY: xxx
   SECRET_KEY: xxx
   
### File convention assumed  
The data lands on NCEI disk as a tarball with 3 files:
 - YYYYMMDD_uuid_geojson.json
 - YYYYMMDD_uuid_metadata.json
 - YYYYMMDD_uuid_pointData.xyz
 
### Athena Notes
```
-- Generate timestamp column
 SELECT *, from_iso8601_timestamp("xyz"."time") ts FROM csbathenadb.xyz 

-- Create parquet table 
CREATE TABLE csbathenadb.csb_mv
WITH (
  format='PARQUET',
  external_location='s3://csbxyzfiles/optimized/'
) AS SELECT
  xyz.*
, "metadata"."name"
, "metadata"."provider"
FROM
  xyz
, metadata
WHERE ("xyz"."uuid" = "metadata"."uuid")

-- Query using dates
SELECT
  *
FROM
  xyz
WHERE
  from_iso8601_timestamp("xyz"."time")
BETWEEN 
  from_iso8601_timestamp('2015-01-01T00:00:00') 
AND 
  from_iso8601_timestamp('2019-01-01T23:59:00')   
```
  
### Example Request 
```
{
  "uuid": "",
  "email": "david.neufeld@colorado.edu",
  "platform.name": "Tenacity",
  "bbox": "-140.0,24.0,-111.0,32.0",
  "sdate": "2015-01-01T00:00:00",
  "edate": "2019-01-01T23:59:00"
}
```

## Testing
To use the testing facility build into PyCharm (JetBrains IDE), you will need to **set the CSBCRAWLER environment variable** for the 
testSuite to run. For example, to execute CsbCrawlerTest.py, 
1. Open the file in the editor, 
1. Right click on the "CsbCrawlerTest.py" tab at the top of the editor window, 
1. Select `Edit 'Unittests in CsbCraw...'...`
1. In the dialog that opens, add `CSBCRAWLER=/Users/ktanaka/src/github/cedardevs/csbCrawler2Cloud` in the "Environment 
variables:" box (adjust the path to your project location)
1. Click "OK" to close the dialog

Now if you right-click on the "CsbCrawlerTest.py" tab, you can select `Run 'Unittests in CsbCraw...'...`

## Dev Notes
### tar.gz Manifest
Refering to this article for generating md5sum on a file:
https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file

## Bucket policy
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicRead",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::odp-noaa-nesdis-ncei-csb/*"
        }
    ]
}
```
