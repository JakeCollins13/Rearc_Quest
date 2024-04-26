import boto3
import requests
import re
import pandas as pd
import os

from dataclasses import dataclass
from datetime import datetime
from botocore.exceptions import ClientError

AWS_ID   = # AWS KEY ID GOES HERE
AWS_KEY  = # AWS KEY GOES HERE
BASE_URL = "https://download.bls.gov/"

@dataclass
class BLS_Files:
    """Class for keeping track of a file and its metadata"""
    filename: str
    filename_ext: str
    insert_date: str

def get_xml_from_bls_website(bls_website_url = "https://download.bls.gov/pub/time.series/pr/"):
    """Download xml from BLS website and then grab filenames + dates within the XML

    :param bls_website_url: BLS website URL
    :return: list of filenames and their accompanying dates
    """

    # Grab XML from BLS download website
    headers = {"user-agent":"jakecollins0613@gmail.com"
             , "registrationkey" : "f846f74fbf954f73ae5a11f2896c50a4"}
    
    files_and_dates_xml = []
    
    try:
        r = requests.get(bls_website_url, headers = headers)
    
        # Use REGEX to parse through the XML for both the filename and datetimes
        # If the BLS website changes its formatting at all, this WILL break
        files_and_dates_xml = re.findall("<br>\s*\d+\/\d+\/\d+\s+.*?HREF=\"[^\"]+\"\>", str(r.content))
    
    except Exception as error:
        print("UNABLE TO DOWNLOAD FROM DLS WEBSITE:", error)

    return files_and_dates_xml

def create_list_of_files(files_and_dates_xml):
    """Create a list of cleaned filenames and their upload dates

    :param files_and_dates_xml: file XML
    :return: list of cleaned filenames and dates
    """

    files = []
    
    # Loop through the found items and create file metadata
    for item in files_and_dates_xml:
    
        try:
            date         = re.search("\s*(\d+\/\d+\/\d{4})\s", item).group(1)
            filename_ext = BASE_URL + re.search("HREF=\"([^\"]+)\"\>", item).group(1)
            filename     = re.search("\/([^\/]+)$", filename_ext).group(1).replace(".", "_")
    
            date_format      = '%m/%d/%Y'
            date             = datetime.strptime(date, date_format).date()
            file_insert_date = str(date)
            
            file = BLS_Files(filename, filename_ext, file_insert_date)
            files.append(file)
            
        except Exception as error:
            print("ERROR WHILE PARSING AND FORMATING FILES:", error)

    return files

def upload_file(filename, bucket, object_name=None, folder_path=""):
    """Upload a file to an S3 bucket

    :param file_name  : File to upload
    :param bucket     : Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :param folder_path: path to the folder in which the files will be inserted into
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(filename)

    # Upload the file
    # Using my own AWS creds for this, once pushed to Git these will be removed
    s3_client = boto3.client('s3', aws_access_key_id = AWS_ID, aws_secret_access_key = AWS_KEY)
    try:
        response = s3_client.upload_file(filename, bucket, folder_path + object_name)
    except ClientError as error:
        print(f"ERROR WHILE UPLOADING {filename} FILE TO S3:", error)
        return False
    return True

def remove_old_files_in_s3_no_longer_on_BLS_site(bucket, folder_path, files_uploaded):
    """Deletes old files in the given s3 bucket folder if their filenames can no longer
       be found on the BLS pr website page

    :param bucket     : Bucket to delete from
    :param folder_path: path to the folder in which the files will be deleted from
    :return: True if no issues deleteting files occured, otherwise false
    """

    no_issues_deleting_files = False

    # Grab current filenames just downloaded from BLS website
    current_files_list = []
    for file in files_uploaded:
        current_files_list.append(file.filename + "_" + file.insert_date)

    # Once again using my personal AWS creds for this
    try:
        s3_client = boto3.client('s3', aws_access_key_id = AWS_ID, aws_secret_access_key = AWS_KEY)
        result    = s3_client.list_objects(Bucket = bucket, Prefix = folder_path)
    except ClientError as error:
        print("ERROR WHILE ATTEMPTING TO LIST FILES IN S3:", error)

    for object in result.get('Contents'):
        
        filepath = object['Key']
        file     = filepath.replace(folder_path, "").replace(".csv", "").replace(".txt", "")

        if file not in current_files_list:
            
            try:
                response = s3_client.delete_object(Bucket = bucket, Key = filepath)
                print(f"DELETED OLD FILE {filepath} FROM S3 BUCKET!")
            except ClientError as error:
                print(f"ERROR WHILE DELETING {filepath} FILE TO S3:", error)
                no_issues_deleting_files = True
                
    return no_issues_deleting_files

# Grab text for each file from BLS website and then create a new file to push to s3 bucket
# NOTE: Unsure how to handle the pure text file (pr.txt), I could hardcode it to not convert that one to a CSV but that seems like cheating
#       I'll just keep it as is in a CSV for now, looks kinda funky but no data is lost as far as I'm aware and can still be read
def upload_files_to_s3(files):
    """Upload files to s3 bucket

    :param files: list of files to upload 
    """

    for file in files:

        headers = {"user-agent":"jakecollins0613@gmail.com"
             , "registrationkey" : "f846f74fbf954f73ae5a11f2896c50a4"}
    
        try:
            request = requests.get(file.filename_ext, headers = headers)
    
        except Exception as error:
            print("ERROR WHILE DOWNLOADING TEXT FROM DLS FILES:", error)
    
        try:
            
            filename_txt = file.filename + "_" + file.insert_date + ".txt"
            filename_csv = file.filename + "_" + file.insert_date + ".csv"
    
            # Create a temp text file which will be converted to csv
            f = open(filename_txt, "w")
            f.write(request.text.replace("\n", ""))
            f.close()
    
            # Convert txt to csv using Pandas
            df = pd.read_table(filename_txt, sep='\t', engine='python', header=None)
            df.to_csv(filename_csv, header=False, index=False)
    
            # Push files to s3 bucket
            try:
    
                print(f"ATTEMPTING TO ADD {filename_csv} FILE TO S3 BUCKET...")
                upload_file(filename_csv, 'rearc-quest-2024', folder_path = "Datasets-Part1/")
                print(f"SUCCESFULLY ADDED {filename_csv} FILE TO S3 BUCKET!")
            
            except Exception as error:
                print(f"***ERROR WHILE PUSHING {filename_csv} FILE TO S3 BUCKET:", error)
        
        except Exception as error:
            print("ERROR WHILE CREATING FILES:", error)
    
        # Remove temp files from local directory
        try:
            os.remove(filename_txt)
            os.remove(filename_csv)
    
        except Exception as error:
            print("ERROR WHILE REMOVING TEMP FILES:", error)

def main():

    files_and_dates_xml = get_xml_from_bls_website(bls_website_url = "https://download.bls.gov/pub/time.series/pr/")

    files = create_list_of_files(files_and_dates_xml)
    
    upload_files_to_s3(files)
    
    # Remove old files no longer on BLS website
    # Using the filenames as a unique ID of sorts
    remove_old_files_in_s3_no_longer_on_BLS_site('rearc-quest-2024', folder_path = "Datasets-Part1/", files_uploaded = files)

if __name__ == "__main__":
    main()