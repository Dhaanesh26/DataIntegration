import pymysql
import csv
import boto3
import configparser

# initialize a connection to mysql database

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("127.0.0.1", "hostname")
port = parser.get("127.0.0.1", "port")
username = parser.get("127.0.0.1", "username")
dbname = parser.get("127.0.0.1", "database")
password = parser.get("127.0.0.1", "password")
conn = pymysql.connect(host=hostname,
 user=username,
 password=password,
 db=dbname,
 port=int(port))
if conn is None:
 print("Error connecting to the MySQL database")
else:
 print("MySQL connection established!")

# perform full extraction to extract entire contents from the table and write it to the pipe delimited 
# CSV file. To perform extraction it uses cursor oject from the pymysql library to execute the SELECT query

# Full Extraction (Extracts entire data from mysql database into csv format ready for s3)

m_query = "SELECT * FROM Orders;"
local_filename = "order_extract.csv"
m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()
with open(local_filename, 'w') as fp:
 csv_w = csv.writer(fp, delimiter='|')
 csv_w.writerows(results)
 fp.close()
m_cursor.close()
conn.close()

# load the aws_boto_credentials values

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials",
"access_key")
secret_key = parser.get("aws_boto_credentials",
"secret_key")
bucket_name = parser.get("aws_boto_credentials",
"bucket_name")
s3 = boto3.client('s3',
aws_access_key_id=access_key,
aws_secret_access_key=secret_key)
s3_file = local_filename

# uploads the full extraction csv data into aws s3 storage

s3.upload_file(local_filename, bucket_name,
s3_file)