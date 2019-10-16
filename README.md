# csbCrawler2Cloud
Python script to load csb data to s3 buckets

Going forward the data lands on NCEI disk as a tarball with 3 files:
 - YYYYMMDD_uuid_geojson.json
 - YYYYMMDD_uuid_metadata.json
 - YYYYMMDD_uuid_pointData.xyz
 
 
 
 ### Athena Notes
-- Generate timestamp column
 SELECT *, from_iso8601_timestamp("xyz"."time") ts FROM csbathenadb.xyz 
 
-- Create View
 CREATE OR REPLACE VIEW csb_view AS 
SELECT
  xyz.*
, "metadata"."name"
, "metadata"."provider"
FROM
  xyz
, metadata
WHERE ("xyzt"."uuid" = "metadata"."uuid")

-- Query using dates
SELECT
  *
FROM
  xyz
WHERE
  from_iso8601_timestamp("xyz"."time")
BETWEEN 
  parse_datetime('2015-01-01-00:00:00','yyyy-MM-dd-HH:mm:ss') 
AND 
  parse_datetime('2019-01-01-23:59:00','yyyy-MM-dd-HH:mm:ss')   