import os
import boto3
import urllib.request
import json

def source_dataset(new_filename, s3_bucket, new_s3_key):

    urllib.request.urlretrieve(
        'https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide.xlsx', '/tmp/' + new_filename + '.xlsx')

    source_base_url = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/'
    for frmt in ['csv', 'json', 'xml']:
        urllib.request.urlretrieve(
            source_base_url + frmt, '/tmp/' + new_filename + '.' + frmt)
    
    asset_list = []

    # Creates S3 connection
    s3 = boto3.client('s3')

    # Looping through filenames, uploading to S3
    for filename in os.listdir('/tmp'):

        s3.upload_file('/tmp/' + filename, s3_bucket,
                        new_s3_key + filename)

        asset_list.append(
            {'Bucket': s3_bucket, 'Key': new_s3_key + filename})

    return asset_list