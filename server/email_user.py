import boto3
from botocore.exceptions import ClientError


class EmailUser:
    def send(self, recipient, access_url):
        # This address must be verified with Amazon SES.
        SENDER = "info@cedardevs.org"

        # AWS Region you're using for Amazon SES.
        AWS_REGION = "us-east-1"

        # The subject line for the email.
        SUBJECT = "CSB Extract Service"

        # The email body for recipients with non-HTML email clients.
        BODY_TEXT = ("Order complete. Please access the file at " + access_url)

        # The HTML body of the email.
        BODY_HTML = """<html>
            <head></head>
            <body>
                <h1>Order complete</h1>
                <p>Please click the following link to access your file.
                    <a href='""" + access_url + """'>Download</a> 
                </p>
            </body>
        </html>"""

        # The character encoding for the email.
        CHARSET = "UTF-8"

        # Create a new SES resource and specify a region.
        emailClient = boto3.client('ses', region_name=AWS_REGION)

        # Provide the contents of the email.
        response = emailClient.send_email(
            Destination={
                'ToAddresses': [recipient, ],
            },
            Message={
                'Body': {
                    'Html': {'Charset': CHARSET, 'Data': BODY_HTML,
                             },
                    'Text': {'Charset': CHARSET, 'Data': BODY_TEXT,
                             },
                },
                'Subject': {
                    'Charset': CHARSET, 'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )

        print("Email sent Message ID: " + response['MessageId'])