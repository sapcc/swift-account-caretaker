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

from caretaker.common import keystone_session
from operator import itemgetter
from swift.account.backend import AccountBroker


UNKNOWN = '_unknown'
ACCOUNT_FIELDS = ['id', 'account', 'domain_id', 'domain_name', 'project_id', 'project_name', 'status',
                  'object_count', 'bytes_used', 'quota_bytes', 'status_deleted', 'deleted',
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
                       'domain_name': UNKNOWN,
                       'project_id': info['account'].lstrip(reseller_prefix),
                       'project_name': UNKNOWN,
                       'status': UNKNOWN,
                       'object_count': info['object_count'],
                       'bytes_used': info['bytes_used'],
                       'status_deleted': broker.is_status_deleted(),
                       'deleted': broker.is_deleted(),
                       'created_at': info['created_at'],
                       'status_changed_at': info['status_changed_at'],
                       'put_timestamp': info['put_timestamp'],
                       'delete_timestamp': info['delete_timestamp']}
            if 'X-Account-Sysmeta-Project-Domain-Id' in meta:
                account['domain_id'] = str(meta['X-Account-Sysmeta-Project-Domain-Id'].pop(0))
            else:
                account['domain_id'] = UNKNOWN
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
            account = _construct(line)
            # Only collect the IDs to skip duplicates
            accounts[account['id']] = account

    return sorted(accounts.values(), key=itemgetter('domain_id', 'project_id'))


def verify(contents, args):
    accounts = []
    for line in contents.split("\n"):
        if line:
            account = _construct(line)
            accounts.append(account)

    domain_id = None
    domain_name = UNKNOWN
    keystone = None
    for account in accounts:
        if account['domain_id'] == UNKNOWN:
            continue

        if domain_id != account['domain_id']:
            domain_id = account['domain_id']
            try:
                keystone = keystone_session(
                    auth_url=args.os_auth_url,
                    admin_username=args.os_ks_admin_username,
                    admin_user_id=args.os_ks_admin_user_id,
                    admin_password=args.os_ks_admin_password,
                    admin_user_domain_name=args.os_ks_admin_user_domain_name,
                    admin_user_domain_id=args.os_ks_admin_user_domain_id,
                    insecure=args.os_ks_insecure,
                    domain_id=domain_id)
                domain_name = keystone.domains.get(domain_id).name
            except Exception as err:
                keystone = None
                domain_name = UNKNOWN
                # TODO Log error
                print err.message

        if keystone:
            account['domain_name'] = domain_name
            try:
                keystone_project = keystone.projects.get(account['project_id'])
                if keystone_project:
                    account['project_name'] = keystone_project.name
                    account['status'] = 'Valid'
            except Exception as err:
                account['status'] = 'Orphan'
                # TODO Log error
                print err.message

    return accounts


def _construct(content):
    account = {}
    i = 0
    values = content.split(',')
    # Reconstruct Account Dict
    for field in ACCOUNT_FIELDS:
        account[field] = values[i]
        i += 1

    return account
