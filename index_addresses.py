import sys
import csv
import re
import os
import time
import collections
from urllib.request import urlparse
from elasticsearch import Elasticsearch, helpers


def f_len(file_path):
    length = 0
    for line in open(file_path):
        length += 1
    return length


def print_progress_summary(start_time, current_row, file_length):
    elapsed_time = time.time() - start_time
    frac_complete = current_row/file_length
    est_time = elapsed_time * (1/frac_complete - 1)
    print(
        '%.3f, %i addresses indexed, elapsed time %s, est. time remaining %s'
        % (
            frac_complete,
            current_row,
            time.strftime('%H:%M:%S', time.gmtime(elapsed_time)),
            time.strftime('%H:%M:%S', time.gmtime(est_time))
        )
    )

def genereate_actions(file_path, file_length):
    with open(file_path, 'r') as csvfile:
        print('open file')
        csv_reader = csv.DictReader(
            csvfile, fieldnames=[], restkey='undefined-fieldnames',
            delimiter=','
        )

        start_time = time.time()
        current_row = 0
        for row in csv_reader:
            current_row += 1

            if current_row == 1:
                csv_reader.fieldnames = row['undefined-fieldnames']
                continue

            address = row
            if current_row % 100000 == 0:
                print_progress_summary(start_time, current_row, file_length)

            yield {
                '_op_type': 'index',
                '_index': 'addresses',
                '_type': 'address',  # '_doc_type'
                '_id': current_row-1,
                '_source': {  # '_body'
                    'NUMBER': address['number'],
                    'STREET': address['street'],
                    'ADDRESS': address['number'] + ' ' + address['street'],
                    'X': address['lon'],
                    'Y': address['lat'],
                    # 'UNIT': address['unit'],
                    # 'CITY': address['city'],
                    # 'POSTCODE': address['postcode'],
                    # 'REGION': address['region'],
                    # 'ACCURACY': address['accuracy'],
                }
            }


if os.environ.get('BONSAI_URL'):
    url = urlparse(os.environ['BONSAI_URL'])
    ELASTICSEARCH_HOST = url.hostname
    ELASTICSEARCH_AUTH = url.username + ':' + url.password
    es = Elasticsearch(
        [{'host': ELASTICSEARCH_HOST}], http_auth=ELASTICSEARCH_AUTH
    )
else:
    es = Elasticsearch()

files_given = sys.argv
for file_name in files_given:
    if file_name == 'index_addresses.py':
        continue
    else:
        file_path = file_name
        print('adding ' + file_path)
        for success, info in helpers.parallel_bulk(
            client=es,
            actions=genereate_actions(file_path, 15000000),
            thread_count=4,
            chunk_size=5000,
            max_chunk_bytes=200*1024**2,
            queue_size=10,
            raise_on_error=True,
            raise_on_exception=True,
            # expand_action_callback=<function expand_action>
        ):
            if not success: print('Doc failed', info)
