import boto3

session = boto3.Session(
    aws_access_key_id='fakeMyKeyId',
    aws_secret_access_key='fakeSecretAccessKey',
    region_name='us-west-2'
)
dynamodb = session.resource(
    'dynamodb',
    endpoint_url='http://localhost:8123'
)


table = dynamodb.Table("unpolishedData")

response = table.scan()
for item in response['Items']:
    print(item)
