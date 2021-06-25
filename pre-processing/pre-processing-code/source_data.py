import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool

def data_to_s3(data):

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	try:
		response = urlopen(data['url'])

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, data['url'])

	except URLError as e:
		raise Exception('URLError: ', e.reason, data['url'])

	else:
		data_set_name = os.environ['DATASET_NAME']
		filename = data_set_name + data['frmt']
		file_location = '/tmp/' + filename

		with open(file_location, 'wb') as f:
			f.write(response.read())

		# variables/resources used to upload to s3
		s3_bucket = os.environ['ASSET_BUCKET']
		new_s3_key = data_set_name + '/dataset/'
		s3 = boto3.client('s3')

		s3.upload_file(file_location, s3_bucket, new_s3_key + filename)

		print('Uploaded: ' + filename)

		# deletes to preserve limited space in aws lamdba
		os.remove(file_location)

		# dicts to be used to add assets to the dataset revision
		return {'Bucket': s3_bucket, 'Key': new_s3_key + filename}

def source_dataset():

	# list of enpoints to be used to access data included with product
	data_endpoints = [
		{'url': 'https://opendata.ecdc.europa.eu/covid19/casedistribution/json', 'frmt': '.json'},
		{'url': 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv', 'frmt': '.csv'},
		{'url': 'https://opendata.ecdc.europa.eu/covid19/casedistribution/xml', 'frmt': '.xml'},
		{'url': 'https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide.xlsx', 'frmt': '.xlsx'}
	]

	# multithreading speed up accessing data, making lambda run quicker
	with (Pool(4)) as p:
		asset_list = p.map(data_to_s3, data_endpoints)

	# asset_list is returned to be used in lamdba_handler function
	return asset_list
