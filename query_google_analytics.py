import argparse
import configparser
from datetime import date, timedelta
import glob
import subprocess
import socket
from math import ceil
import os
import random
import time

from apiclient.discovery import build
from apiclient.errors import HttpError
import pandas as pd
from pandas.io.common import EmptyDataError
from oauth2client.service_account import ServiceAccountCredentials

from utils import write_csv_file, upload_csv_file_to_bq, upload_blob

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'api.ini'))


ap = argparse.ArgumentParser()
ap.add_argument("-table", "--table_name",
                required=True, help="1st operand")
ap.add_argument("-metric", "--metric_name",
                required=True, help="2nd operand")
ap.add_argument("-dimension", "--dimension_name",
                required=True, help="3nd operand")
args = vars(ap.parse_args())

# TABLE_NO = 'query_1'
TABLE_NO = args['table_name']

#### GA Metric/Dimension Names ####

dimension_name = str(args['dimension_name'])
dimension_names = config['ga_fields'][dimension_name].split(',')
print(dimension_names)

metric_name = str(args['metric_name'])
metric_names = config['ga_fields'][metric_name].split(',')
print(metric_names)

header = [each.replace('ga:', '') for each in metric_names]
header += [each.replace('ga:', '') for each in dimension_names]


##########
print(os.path.dirname(
    __file__))

SCOPES = config['default']['SCOPES']
KEY_FILE_LOCATION = os.path.join(os.path.dirname(
    __file__), config['default']['KEY_FILE_LOCATION'])
VIEW_ID = os.getenv('VIEW_ID')


#### BQ CONFIG ####
PROJECT_NAME = config['bq_import']['PROJECT_NAME']
DATASET_NAME = config['bq_import']['DATASET_NAME']
TABLE_NAME = config['bq_import']['TABLE_NAME']

#### GCS CONFIG ####
BUCKET_NAME = '<bucket_name>'
BUCKET_FOLDER = f'{TABLE_NO}/'


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    socket.setdefaulttimeout(300) 
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics
    
def makeRequestWithExponentialBackoff(analytics):
    for n in range(0, 5):
        try:
            return analytics.execute()

        except HttpError as error:
            if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded',
                                'internalServerError', 'backendError', 'Service Unavailable']:
                print('Found error from API limit, Start using Exponential Backoff')
                time.sleep((2 ** n) + random.random())
            else:
                print('Error from: '+ error.resp.reason)
                print('Error message: ' + error.resp.message)
                break

    print('There has been an error, the request never succeeded.')

def get_report(analytics, page_token='0'):
    metrics = [{'expression': each} for each in metric_names]
    dimensions = [{'name': each} for each in dimension_names]

    ga_config = {
        'viewId': VIEW_ID,
        'dateRanges': [{'startDate': 'yesterday', 'endDate': 'yesterday'}],
        'metrics': metrics,
        'dimensions': dimensions,
        "orderBys": [{
            "fieldName": "ga:dateHourMinute"
        }],
        "samplingLevel": "LARGE",
        'pageSize': 1000,
        'pageToken': page_token
    }
    request = analytics.reports().batchGet(
        body={
            'reportRequests': [ga_config]
        }
    )

    return request

def get_ga_data_from_api(response):
    count_loop = 0
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        pageToken = report.get('nextPageToken', None)
        rowCount = report.get('data', {}).get('rowCount', None)

        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get(
            'metricHeader', {}).get('metricHeaderEntries', [])

        all_rows = []
        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            row = []
            # print('-----------------------------------------')
            for i, values in enumerate(dateRangeValues):  # metric
                # print('----- Here are Metrics: ----- ', str(i))
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    # print(metricHeader.get('name') + ':', value)
                    row.append(value)

            # print('----- Here are Dimensions: -----')
            for header, dimension in zip(dimensionHeaders, dimensions):
                # print(header + ': ', dimension)
                row.append(dimension)

            count_loop += 1
            all_rows.append(row)
            # print('-----------------------------------------')

        # print(count_loop)

        return [all_rows, pageToken, rowCount]

def main():
    today = date.today()
    yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")
    analytics = initialize_analyticsreporting()

    print('run index: ', 1)

    request = get_report(analytics)
    response = makeRequestWithExponentialBackoff(request)
    all_rows, next_page_token, row_count = get_ga_data_from_api(response)
    print('next_page_token: ', next_page_token)

    CSVFILE_NAME = f'data/{yesterday}/{TABLE_NO}_{yesterday}_1.csv'

    try:
        os.makedirs(os.path.join(os.path.dirname(__file__), 'data', yesterday))
    except FileExistsError:
        print('Folder already exists')

    write_csv_file(
        os.path.join(os.path.dirname(__file__), CSVFILE_NAME),
        header,
        all_rows
    )
    print('row count: ', row_count)

    if next_page_token is None:
        row_count = 0

    cycle = ceil(int(row_count) / 1000)
    for i in range(0, cycle):
        index = i + 2
        print('run index: ', index)

        request = get_report(analytics, page_token=next_page_token)
        response = makeRequestWithExponentialBackoff(request)
        all_rows, next_page_token, row_count = get_ga_data_from_api(response)

        if next_page_token == 'None':
            break

        CSVFILE_NAME = f'data/{yesterday}/{TABLE_NO}_{yesterday}_{index}.csv'

        write_csv_file(
            os.path.join(os.path.dirname(__file__), CSVFILE_NAME),
            header,
            all_rows
        )
        time.sleep(2)

    SUFFIX_CSV_FILE = f'data/{yesterday}/{TABLE_NO}_{yesterday}_*.csv'
    SUFFIX_CSV_FILE = os.path.join(os.path.dirname(__file__), SUFFIX_CSV_FILE)
    CSV_FILE_NAME = f'data/{yesterday}/{TABLE_NO}_{yesterday}.csv'
    CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), CSV_FILE_NAME)
    csv_list = glob.glob(SUFFIX_CSV_FILE)

    try:
        combined_csv = pd.concat([pd.read_csv(f, header=None) for f in csv_list])
    except EmptyDataError:
        combined_csv = pd.DataFrame()
    combined_csv.to_csv(CSV_FILE_PATH, header=False, index=False)
    upload_blob(
        BUCKET_NAME,
        CSV_FILE_PATH,
        BUCKET_FOLDER + CSV_FILE_NAME
    )

    ## delete file on local machine
    for csv_file in csv_list:
        os.remove(csv_file)
    os.remove(CSV_FILE_PATH)


if __name__ == '__main__':
    main()