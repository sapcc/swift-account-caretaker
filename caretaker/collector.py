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
import socket
import sqlite3

from swift.account.backend import AccountBroker


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

            account = {'id': info['id'], 'account': info['account'],
                       'project': info['account'].lstrip(reseller_prefix),
                       'object_count': info['object_count'],
                       'bytes_used': info['bytes_used'], 'created_at': info['created_at'],
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


def output(accounts):
    print _format_accounts(accounts, "\t")
    print "\n---\n"


def upload(accounts, connection, container='caretaker'):
    obj_name = 'raw/' + socket.getfqdn() + '_accounts.csv'
    content = _format_accounts(accounts)
    connection.put_container(container)
    connection.put_object(container, obj_name, contents=content, content_type='text/plain')

    print obj_name + " uploaded"


def _format_accounts(accounts, delimiter=','):
    result = delimiter.join(['domain', 'id', 'project', 'created_at', 'delete_timestamp',
                             'object_count', 'bytes_used', 'quota_bytes'])
    for account in accounts:
        line = delimiter.join([account['domain'], account['id'], account['project'], account['created_at'],
                               account['delete_timestamp'], str(account['object_count']),
                               str(account['bytes_used']), str(account['quota_bytes'])])
        result = "{0}\n{1}".format(result, line)

    return result
