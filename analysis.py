import boto3
import io
import json
import pandas as pd

from botocore.exceptions import ClientError

import logging
logging.getLogger().setLevel(logging.INFO)

AWS_ID   = # AWS KEY ID GOES HERE
AWS_KEY  = # AWS KEY GOES HERE

def lambda_handler(event, context):
    """
        Ingests the CSV data and JSON data from s3
        Formats and transforms DataFrames as needed
        Logs:
            mean population yearly in the US within 2013-2018
            standard deviation of yearly population in the US within 2013-2018
            best year for each series_id
            best year for each series_id with accompaying US population for that year
    """

    bucket = "rearc-quest-2024"
    folder_path_part1 = "Datasets-Part1/pr_data_0_Current"

    # Once again using my personal AWS creds for this
    try:
        s3_client = boto3.client('s3', aws_access_key_id = AWS_ID, aws_secret_access_key = AWS_KEY)
        result    = s3_client.list_objects(Bucket = bucket, Prefix = folder_path_part1).get('Contents')[0]
        obj       = s3_client.get_object(Bucket = bucket, Key = result['Key'])
        df_part1  = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        
    except ClientError as error:
        print("ERROR WHILE ATTEMPTING TO GET JSON FILE IN S3:", error)

    # Fix column names having leading/ending spaces
    for column in df_part1.columns:
        df_part1 = df_part1.rename(columns = {column : column.strip()})
    # Fix row values having leading/ending spaces
    for column, column_dtype in zip(df_part1.columns, df_part1.dtypes):
        if column_dtype == "object":
            df_part1[column] = df_part1[column].apply(lambda x : x.strip() if isinstance(x, str) else x)

    folder_path_part2 = "Datasets-Part2/JSON"

    # Once again using my personal AWS creds for this
    try:
        s3_client    = boto3.client('s3', aws_access_key_id = AWS_ID, aws_secret_access_key = AWS_KEY)
        result       = s3_client.list_objects(Bucket = bucket, Prefix = folder_path_part2).get('Contents')[0]
        obj          = s3_client.get_object(Bucket = bucket, Key = result['Key'])
        json_data    = json.loads(obj['Body'].read())

    except ClientError as error:
        print("ERROR WHILE ATTEMPTING TO GET JSON FILE IN S3:", error)

    df_part2 = pd.json_normalize(json_data['data'])

    df_part2_2013_to_2018 = df_part2[(df_part2['ID Year'] >= 2013) & (df_part2['ID Year'] <= 2018)]
    mean_and_std_df       = df_part2_2013_to_2018.groupby(['Nation'], as_index=False).agg({'Population' : ['mean', 'std']}).round(2)

    msg = "MEAN POPULATION: " + str(mean_and_std_df['Population']['mean'][0])
    logging.info(msg)

    msg = "STANDARD DEVIATION OF POPULATION: " + str(mean_and_std_df['Population']['std'][0])
    logging.info(msg)

    df_part1_yearly_sum     = df_part1.groupby(['series_id', 'year'], as_index=False)['value'].sum().round(2)
    df_best_year_per_series = df_part1_yearly_sum.groupby(['series_id'], as_index=False).agg({'value' : ['max']})

    # Used head() to slim down the log output
    msg = df_best_year_per_series.head().to_string().replace('\n', '\n\t')
    logging.info(msg)

    # Edit prior DFs to remove unneccessary columns and rename 'ID Year' column to make the merge on possible
    df_part1_edit = df_part1[['series_id', 'year', 'period', 'value']]
    df_part2_edit = df_part2[['ID Year', 'Population']].rename(columns = {'ID Year' : 'year'})
    df_merge      = df_part1_edit.merge(df_part2_edit, how='inner', on='year')

    # Used head() to slim down the log output
    msg = df_merge.head().to_string().replace('\n', '\n\t')
    logging.info(msg)

    return {
        "statusCode": 200,
    }
