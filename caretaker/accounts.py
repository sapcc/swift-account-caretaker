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

import fnmatch
import glob
import logging
import os
import sqlite3

from caretaker.common import keystone_session
from operator import itemgetter
from swift.account.backend import AccountBroker, DATADIR


LOG = logging.getLogger(__name__)
ACCOUNT_FIELDS = ['account', 'domain_id', 'domain_name', 'project_id', 'project_name', 'status',
                  'object_count', 'bytes_used', 'quota_bytes',
                  'status_deleted', 'created_at', 'delete_timestamp']
SEP = ';'
STATUS_UNKNOWN = '_unknown'
STATUS_VALID = 'VALID'
STATUS_ORPHAN = 'ORPHAN'


def format(accounts, delimiter=SEP, with_header=False):
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
    matches = []

    account_dirs = glob.glob(os.path.join(device_dir, '*/', DATADIR))
    for account_dir in account_dirs:
        LOG.debug("scanning {0} for account databases".format(account_dir))
        for root, dirnames, filenames in os.walk(account_dir, topdown=False):
            for filename in fnmatch.filter(filenames, '*.db'):
                matches.append(os.path.join(root, filename))

    LOG.info("{0} account databases found".format(len(matches)))
    accounts = []

    # Evaluate the Account information
    for match in matches:
        broker = AccountBroker(match, stale_reads_ok=stale_reads_ok)
        try:
            info = broker.get_info()
            meta = broker.metadata

            account = {'account': info['account'],
                       'domain_name': STATUS_UNKNOWN,
                       'project_id': info['account'].lstrip(reseller_prefix),
                       'project_name': STATUS_UNKNOWN,
                       'status': STATUS_UNKNOWN,
                       'object_count': info['object_count'],
                       'bytes_used': info['bytes_used'],
                       'status_deleted': broker.is_status_deleted(),
                       'created_at': info['created_at'],
                       'delete_timestamp': info['delete_timestamp']}
            if 'X-Account-Sysmeta-Project-Domain-Id' in meta:
                account['domain_id'] = str(meta['X-Account-Sysmeta-Project-Domain-Id'].pop(0))
            else:
                account['domain_id'] = STATUS_UNKNOWN
            if 'X-Account-Meta-Quota-Bytes' in meta:
                account['quota_bytes'] = int(meta['X-Account-Meta-Quota-Bytes'][0])
            else:
                account['quota_bytes'] = 0

            LOG.debug("Account {}".format(account))

            accounts.append(account)
        except sqlite3.OperationalError as err:
            LOG.error(err.message)

    LOG.info("{0} accounts collected".format(len(accounts)))
    return accounts


def merge(contents):
    accounts = {}
    i = 0
    for line in contents.split("\n"):
        if line:
            i += 1
            account = _construct(line)
            # Only collect the IDs to skip duplicates
            accounts[account['account']] = account

    LOG.info("{0} accounts merged into {1} unique".format(i, len(accounts)))
    return sorted(accounts.values(), key=itemgetter('domain_id', 'project_id'))


def verify(contents, args):
    accounts = []
    for line in contents.split("\n"):
        if line:
            account = _construct(line)
            accounts.append(account)

    domain_id = None
    domain_name = STATUS_UNKNOWN
    keystone = None
    i = 0

    for account in accounts:
        if account['domain_id'] == STATUS_UNKNOWN:
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
                domain_name = STATUS_UNKNOWN
                LOG.warning(err.message)

        if keystone:
            account['domain_name'] = domain_name
            try:
                keystone_project = keystone.projects.get(account['project_id'])
                if keystone_project:
                    account['project_name'] = keystone_project.name
                    account['status'] = STATUS_VALID
                    i += 1
                    LOG.debug("Account {0} is valid in {1}/{2}".format(
                        account['account'], domain_name, keystone_project))
            except Exception as err:
                account['status'] = STATUS_ORPHAN
                LOG.warning(err.message)

    LOG.info("{0} of {1} accounts were valid".format(i, len(accounts)))
    return accounts


def _construct(content):
    account = {}
    i = 0
    values = content.split(SEP)
    # Reconstruct Account Dict
    for field in ACCOUNT_FIELDS:
        account[field] = values[i]
        i += 1

    return account
