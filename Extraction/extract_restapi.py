import requests
import json
import configparser
import csv
import boto3

lat = 42.36
lon = 71.05

lat_log_params = {"lat": lat, "lon": lon}

api_response = requests.get(
    "http://api.open-notify.org/iss-now.json",
    params=lat_log_params
)

response_json = json.loads(api_response.content)
all_passes = []
current_pass = []

# store the lat/log from the request

current_pass.append(lat)
current_pass.append(lon)

# store the ISS current latitude/longitude and timestamp

current_pass.append(response_json['iss_position']['latitude'])
current_pass.append(response_json['iss_position']['longitude'])
current_pass.append(response_json['timestamp'])
all_passes.append(current_pass)

export_file = "export_file.csv"
with open(export_file, 'w') as fp:
    csvw = csv.writer(fp, delimiter='|')
    csvw.writerows(all_passes)

# establish s3 connection from vscode to send data

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
s3.upload_file(export_file, bucket_name, export_file)

