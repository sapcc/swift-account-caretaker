# Copyright 2016 SAP SE
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import sqlite3

from swift.account.backend import AccountBroker


ACCOUNT_FIELDS = ['domain', 'id', 'project', 'object_count', 'bytes_used', 'quota_bytes',
                  'created_at', 'status_deleted', 'deleted',
                  'created_at', 'status_changed_at', 'put_timestamp', 'delete_timestamp']


def format(accounts, delimiter=',', with_header=False):
    result = ''

    if with_header:
        result = delimiter.join(ACCOUNT_FIELDS) + "\n"

    for account in accounts:
        line = []
        for field in ACCOUNT_FIELDS:
            line.append(str(account[field]))

        result += delimiter.join(line) + "\n"

    return result


def collect(device_dir='/srv/node', stale_reads_ok=False,
            reseller_prefix='AUTH_'):
    matches = glob.glob(os.path.join(device_dir, '*/accounts/*.db'))
    accounts = []

    # Evaluate the Account information
    for match in matches:
        broker = AccountBroker(match, stale_reads_ok=stale_reads_ok)
        try:
            info = broker.get_info()
            meta = broker.metadata

            account = {'id': info['id'],
                       'account': info['account'],
                       'project': info['account'].lstrip(reseller_prefix),
                       'object_count': info['object_count'],
                       'bytes_used': info['bytes_used'],
                       'status_deleted': broker.is_status_deleted(),
                       'deleted': broker.is_deleted(),
                       'created_at': info['created_at'],
                       'status_changed_at': info['status_changed_at'],
                       'put_timestamp': info['put_timestamp'],
                       'delete_timestamp': info['delete_timestamp']}
            if 'X-Account-Sysmeta-Project-Domain-Id' in meta:
                account['domain'] = str(meta['X-Account-Sysmeta-Project-Domain-Id'].pop(0))
            else:
                account['domain'] = '_unknown'
            if 'X-Account-Meta-Quota-Bytes' in meta:
                account['quota_bytes'] = int(meta['X-Account-Meta-Quota-Bytes'][0])
            else:
                account['quota_bytes'] = 0

            accounts.append(account)
        except sqlite3.OperationalError as err:
            # TODO Log error
            print err.message

    return accounts


def merge(contents):
    accounts = {}
    for line in contents.split("\n"):
        if line:
            account = {}
            i = 0
            values = line.split(',')

            # Reconstruct Account Dict
            for field in ACCOUNT_FIELDS:
                account[field] = values[i]
                i += 1

            # Only collect the IDs to skip duplicates
            accounts[account['id']] = account

    return accounts.values()

