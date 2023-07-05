'''Combine multiple excel files into one for QAQC Report pipeline.'''
from google.cloud import storage
import glob
import pandas as pd
from google.cloud import storage
import re

bucket_name = "qaqc_reports" # stg
project = "nih-nci-dceg-connect-stg-5519"

#bucket_name = "qaqc_reports_prod" # prod
#project = "nih-nci-dceg-connect-prod-6d04"

def generate_output_filename(filename):
    # Define the patterns to be matched and replaced
    patterns = [
        (r'_rules_range_', '_export2box_'),
        (r'[A-Z]\d+:[A-Z]\d+_', ''),
    ]
    
    # Apply the patterns sequentially
    for pattern, replacement in patterns:
        filename = re.sub(pattern, replacement, filename)
        
    return filename

def check_substrings(filename, substrings):
    for substring in substrings:
        if substring not in filename:
            return False
    return True

def main(bucket_name=bucket_name, project=project):
    
    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)

    # List blobs in storage bucket
    blobs = storage_client.list_blobs(bucket_name)
    blob_list = [blob.name for blob in blobs]

    # Get file names from bucket that have xlsx and other appropriate tags
    substrings_to_check = ['recruitment', 'rules_range','.xlsx']
    xlsx_list = [fname for fname in blob_list if check_substrings(fname, substrings_to_check)]

    # Loop thru blobs, download them as bytes, read as df, append to list
    excl_list = []
    for blob_name in xlsx_list:
        blob = bucket.blob(blob_name)
        data_bytes = blob.download_as_bytes()
        excl_list.append(pd.read_excel(data_bytes))
    
    print("excl_list = " + excl_list)
    # Concatenate data frames
    excl_merged = pd.concat(excl_list, ignore_index=True)
    print("excl_merged = \n", + excl_merged)

    # Convert to excel file
    excl_merged.to_excel('out.xlsx', index=False)

    # Move to Google Cloud Storage bucket
    # Name of the file on the GCS once uploaded

    # First set name of new blob to be created in bucket
    output_file_name = generate_output_filename(xlsx_list[0])
    output_blob = bucket.blob(output_file_name)

    # Path of the local file to be uploaded
    output_blob.upload_from_filename('out.xlsx')
    
if "__name__" == "__main__":
    main(bucket_name=bucket_name, project=project)