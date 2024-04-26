import boto3
import requests
import re
import json
import os

from datetime import date
from datetime import datetime
from botocore.exceptions import ClientError

AWS_ID   = # AWS KEY ID GOES HERE
AWS_KEY  = # AWS KEY GOES HERE

def get_api_json(api_call):
    """Grab the JSON data from the given API

    :param files: API call
    """
    
    try:
        request = requests.get(api_call)
    except Exception as error:
        print("ERROR UNABLE TO ACCESS API:", error)
        return ""
        
    return request.json()

def write_json_to_file_and_upload_to_s3():
    """After getting the JSON data, push into a temp JSON file and then push it up to the s3 bucket
       Remove temp files at the end
    """

    json_output = get_api_json("https://datausa.io/api/data?drilldowns=Nation&measures=Population")

    # Create a JSON file
    filename = "JSON_Data_USA_Pull.json"
    with open(filename, "w") as outfile:
        json_object = json.dumps(json_output)
        outfile.write(json_object)

    # Upload the file
    # Using my own AWS creds for this, once pushed to Git these will be removed
    s3_client = boto3.client('s3', aws_access_key_id = AWS_ID, aws_secret_access_key = AWS_KEY)
    try:
        print(f"ATTEMPTING TO ADD {filename} FILE TO S3 BUCKET...")
        response = s3_client.upload_file(filename, "rearc-quest-2024", "Datasets-Part2/" + filename)
        print(f"SUCCESFULLY ADDED {filename} FILE TO S3 BUCKET!")
    except ClientError as error:
        print(f"ERROR WHILE UPLOADING {filename} FILE TO S3:", error)

    #Remove temp files from local directory
    try:
        os.remove(filename)

    except Exception as error:
        print("ERROR WHILE REMOVING TEMP FILES:", error)

def main():
    write_json_to_file_and_upload_to_s3()

if __name__ == "__main__":
    main()
