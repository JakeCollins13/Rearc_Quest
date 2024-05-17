# Rearc_Quest

**Welcome to my source code repo for the Rearc Data Quest!**

Also thank you for the hints in the Data Quest Repo. Helped me along with BLS asking for the User-Agent email header, which was taking me longer than I would have liked, to figure out. Also the whitespace thing during Part3 because at first I though I was going crazy and there was a bug in my code that I couldn't find but the hint saved me from going completely insane and realizing it was just part of the dataset..

## Main Source Code Files

- **PyScript_Part1.py:** Python script that runs the data ingest for the BLS website and pushes the CSV files to s3
- **PyScript_Part2.py:** Python script that runs the data ingest for the Data USA API and pushes the JSON file to s3
- **Part_3_Data_Analysis.ipynb:** Jupyter Notebook for the data analysis portion of the project with necessary outputs shown

## CDK Source Code Files
- **cdk_app_stack.py:** Generates the AWS resources in the cloud (two lambda functions, s3 bucket, and SQS along with their events/triggers)
- **ingest.py:** Ingests the data for Part 1 and 2 which is called through the first lambda function
- **analysis.py:** Analyzes the data as needed for Part 3, logging outputs where necessary, and called through the second lambda function

## [S3 Bucket Link](https://us-east-1.console.aws.amazon.com/s3/buckets/rearc-quest-2024?region=us-east-1&bucketType=general&tab=objects)
- **Datasets-Part1** folder contains all the CSV files from the Bureau of Labor Statistics
- **Datasets-Part2** folder contains the JSON file from the Data USA API return

## Known/Potential Issues
List of known/potential issues that I couldn't figure out a way to fix in time or by design
1) BLS data ingest relies on REGEX patterns to scrape the BLS website if this site's formatting ever changes, ingest breaks.
   - I thought of only scraping the href link for each file, which would significantly reduce the chance of this happening, but I wanted to get the file's upload date as well.
2) I create tempprary txt files, which is quickly converted to a CSV, which use up local memory temporarily. This shouldn't be a problem on a local machine which should have enough memory BUT on the Cloud (in this case AWS Lambda which only allows 512MB) this could become an issue if a large file is stored in the /tmp/ folder.
3) CDK would sometimes send **two** messages to SQS, which would cause the Lambda Function to trigger the data analysis script twice, and I could not figure out why.
4) Not a major issue but I did shrink the DataFrame logging in Lambda for Part 4, using head(), so it wouldn't fill it up the log. I assumed this wouldn't be an issue but putting it here just in case.
