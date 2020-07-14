## S3
Bucket is in AWS ODP Account in the Oregon West Region
S3 buckets have events (via bucket properties) configured to publish to the SNS topics 
    
## SNS 
SNS are set-up on the AWS ODP account in the Oregon West Region
Allow s3 to publish to SNS
    {
      "Sid": "csb-s3-ID",
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "SNS:Publish",
      "Resource": "arn:aws:sns:us-west-2:541768555562:odp-noaa-nesdis-ncei-csb",
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "541768555562"
        },
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:*:*:odp-noaa-nesdis-ncei-csb"
        }
    }

## SQS are set-up
SQS are set-up on the nesdis-sandbox account
Allow SNS to send message to sqs
    {
      "Sid": "CSB-SQSPolicy001",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:us-west-2:282856304593:noaa-nesdis-ncei-csb-test-sqs",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:sns:us-west-2:541768555562:odp-noaa-nesdis-ncei-csb-test"
        }
      }