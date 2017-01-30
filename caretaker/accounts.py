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

from caretaker.common import keystone_session, keystone_client, keystone_scrape_projects, keystone_get_backend_info
from operator import itemgetter
from swift.account.backend import AccountBroker, DATADIR
from keystoneclient import exceptions as ke


LOG = logging.getLogger(__name__)
ACCOUNT_FIELDS = ['account', 'domain_id', 'domain_name', 'project_id', 'project_name', 'status',
                  'object_count', 'bytes_used', 'quota_bytes',
                  'status_deleted', 'created_at', 'delete_timestamp']
SEP = ';'
STATUS_UNKNOWN = '_unknown'
STATUS_VALID = 'VALID'
STATUS_INVALID = 'INVALID'
STATUS_ORPHAN = 'ORPHAN'
STATUS_DELETED = 'DELETED'


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

            # This account is used by the object expirer
            if info['account'] == '.expiring_objects':
                continue

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

    if args.os_ks_scrape_auth_url:
        # Getting a whole project scrape
        sess = keystone_session(
            auth_url=args.os_ks_scrape_auth_url,
            admin_username=args.os_ks_scrape_admin_username,
            admin_user_id=args.os_ks_scrape_admin_user_id,
            admin_password=args.os_ks_scrape_admin_password,
            admin_user_domain_name=args.os_ks_scrape_admin_user_domain_name,
            admin_user_domain_id=args.os_ks_scrape_admin_user_domain_id,
            insecure=args.os_ks_scrape_insecure)
        kclnt = keystone_client(session=sess, endpoint_override=args.os_ks_scrape_auth_url)
        scraped_domains = keystone_scrape_projects(kclnt)
        LOG.info("{0} domains scraped".format(len(scraped_domains)))

    domain_id = None
    domain_name = STATUS_UNKNOWN
    kclnt = None
    valid = 0
    orphan = 0
    deleted = 0

    for account in accounts:
        if account['status_deleted'] == 'True':
            account['status'] = STATUS_DELETED
            deleted += 1

        if account['domain_id'] == STATUS_UNKNOWN:
            continue

        if domain_id != account['domain_id']:
            domain_id = account['domain_id']

            # Getting a domain scoped session
            sess = keystone_session(
                auth_url=args.os_auth_url,
                admin_username=args.os_ks_admin_username,
                admin_user_id=args.os_ks_admin_user_id,
                admin_password=args.os_ks_admin_password,
                admin_user_domain_name=args.os_ks_admin_user_domain_name,
                admin_user_domain_id=args.os_ks_admin_user_domain_id,
                insecure=args.os_ks_insecure,
                domain_id=domain_id)
            kclnt = keystone_client(session=sess)

            try:
                domain_name = kclnt.domains.get(domain_id).name
                backend = keystone_get_backend_info(kclnt)

            except ke.BadRequest as err:
                # Invalid Domain, e.g. it was disabled
                LOG.warning("DomainID {0}: {1}".format(domain_id, err.message))
                domain_name = STATUS_INVALID
                backend = STATUS_UNKNOWN
                kclnt = None
            except ke.Unauthorized as err:
                if domain_id in scraped_domains:
                    backend = scraped_domains[domain_id]['backend']
                    domain_name = scraped_domains[domain_id]['name']
                else:
                    LOG.warning("DomainID {0}: {1}".format(domain_id, err.message))
                    domain_name = STATUS_UNKNOWN
                    backend = STATUS_UNKNOWN

                kclnt = None
            except Exception as err:
                LOG.warning("DomainID {0}: {1}".format(domain_id, err.message))
                domain_name = STATUS_UNKNOWN
                backend = STATUS_UNKNOWN
                kclnt = None

        account['domain_name'] = domain_name
        account['backend'] = backend

        if kclnt:
            try:
                keystone_project = kclnt.projects.get(account['project_id'])
                if keystone_project:
                    # project exist
                    account['project_name'] = keystone_project.name
                    account['status'] = STATUS_VALID
                    valid += 1
                    LOG.debug("Account {0} is valid in {1}/{2}".format(
                        account['account'], domain_name, keystone_project))
            except ke.NotFound as err:
                # Project does not exists in domain
                if not account['status_deleted'] == 'True':
                    account['status'] = STATUS_ORPHAN
                    orphan += 1
                LOG.debug("DomainID {0}/ProjectID {1}: {2}".format(domain_id, account['project_id'], err.message))
            except Exception as err:
                LOG.warning("DomainID {0}/ProjectID {1}: {2}".format(domain_id, account['project_id'], err.message))
        elif domain_name == STATUS_INVALID:
            # Project in invalid domain
            account['status'] = STATUS_INVALID
            orphan += 1
        elif domain_name != STATUS_UNKNOWN:
            if account['project_id'] in scraped_domains[domain_id]['projects']:
                # Project in scraped domains
                account['project_name'] = scraped_domains[domain_id]['projects'][account['project_id']]['name']
                account['status'] = STATUS_VALID
                valid += 1
                LOG.debug("Account {0} is valid in {1}/{2}".format(
                    account['account'], domain_name, account['project_name']))

    LOG.info("Account verification: Valid {0}, Orphans {1}, Deleted {2}, Overall {3}".format(valid, orphan, deleted,
                                                                                             len(accounts)))
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
