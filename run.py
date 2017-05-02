

import requests
import requests_cache
import re
from bs4 import BeautifulSoup
import json
from datetime import datetime

requests_cache.install_cache('celery_cache')

COLLECTION_RESOURCE_IDS = [
    '05ff2255-c38a-40c9-b657-4ccb55ab2feb',
    'bb909597-dedf-427d-8c04-4c02b3a24db3',
    'ec61d82a-748d-4b53-8e99-3e708e76bc4d'
]

def _get_soup(url):
    r = requests.get(url, auth=('admin', 'paizuiW7'))
    return BeautifulSoup(r.text)

def main():

    result = {}

    url = 'http://dp-nlb-2.nhm.ac.uk:5555/tasks?limit=10010&worker=All&type=All&state=All'
    soup = _get_soup(url)
    task_links = soup.findAll('a', href=re.compile('^/task/'))

    re_resource_id = re.compile("'resource_id': u'([0-9a-z\-]+)'")
    re_limit = re.compile("'limit': u'([0-9]+)'")

    count = 0
    for a in task_links:
        count += 1
        task_url = '{0}{1}'.format(
            'http://dp-nlb-2.nhm.ac.uk:5555',
            a['href']
        )
        # print('Retrieving: ', task_url)
        task_soup = _get_soup(task_url)
        basic_options_table, advd_options_table = task_soup.findAll("table")

        for row in basic_options_table.findAll('tr'):
            th, td = row.findAll('td')
            if th.getText() == 'args':
                args = td.getText()
                resource_id = re_resource_id.search(args).group(1)
                try:
                    limit = re_limit.search(args).group(1)
                except AttributeError:
                    limit = 0

        for row in advd_options_table.findAll('tr'):
            th, td = row.findAll('td')
            if th.getText() == 'Received':
                date_str = td.getText().strip()
                date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f').date()

                result.setdefault(date.year, {})

                result[date.year].setdefault(date.month, {
                    'collection': {
                        'download_events': 0,
                        'records': 0
                    },
                    'research': {
                        'download_events': 0,
                        'records': 0
                    }                     
                })

                key = 'collection' if resource_id in COLLECTION_RESOURCE_IDS else 'research'

                result[date.year][date.month][key]['download_events'] += 1
                result[date.year][date.month][key]['records'] += int(limit)

    print(result)


if __name__ == "__main__":
    main()
