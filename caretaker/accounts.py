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

from builtins import str
from builtins import object
import fnmatch
import glob
import logging
import os
import sqlite3

from caretaker.common import keystone_session, keystone_client, keystone_get_backend_info, \
    DomainWrapper, ProjectWrapper
from operator import itemgetter
from keystoneclient import exceptions as ke
from swift.account.backend import AccountBroker, DATADIR

LOG = logging.getLogger(__name__)
ACCOUNT_FIELDS = ['account', 'domain_id', 'project_id', 'object_count', 'bytes_used', 'quota_bytes',
                  'status_deleted', 'created_at', 'delete_timestamp']
ADD_ACCOUNT_FIELDS = ['backend', 'domain_name', 'project_name', 'status']
ALL_ACCOUNT_FIELDS = ADD_ACCOUNT_FIELDS + ACCOUNT_FIELDS
SEP = ';'
STATUS_UNKNOWN = '_unknown'
STATUS_VALID = 'VALID'
STATUS_INVALID = 'INVALID'
STATUS_ORPHAN = 'ORPHAN'
STATUS_DELETED = 'DELETED'


def format(accounts, all_fields=False, delimiter=SEP, with_header=False):
    result = ''
    fields = ACCOUNT_FIELDS
    if all_fields:
        fields = ALL_ACCOUNT_FIELDS

    if with_header:
        result = delimiter.join(fields) + "\n"

    for account in accounts:
        line = []
        for field in fields:
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
                       'object_count': info['object_count'],
                       'bytes_used': info['bytes_used'],
                       'status_deleted': broker.is_status_deleted(),
                       'created_at': info['created_at'],
                       'delete_timestamp': info['delete_timestamp']}
            if 'X-Account-Sysmeta-Project-Domain-Id' in meta:
                account['domain_id'] = str(meta['X-Account-Sysmeta-Project-Domain-Id'].pop(0))
            else:
                account['domain_id'] = STATUS_UNKNOWN
            if 'X-Account-Meta-Quota-Bytes' in meta and meta['X-Account-Meta-Quota-Bytes'][0] != '':
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

            # This account is used by the object expirer
            if account['account'] == '.expiring_objects':
                continue

            # Only collect the IDs to skip duplicates
            accounts[account['account']] = account

    LOG.info("{0} accounts merged into {1} unique".format(i, len(accounts)))
    return sorted(list(accounts.values()), key=itemgetter('domain_id', 'project_id'))


def verify(contents, os_config, statsd):
    accounts = []
    for line in contents.split("\n"):
        if line:
            account = _construct(line)
            for field in ADD_ACCOUNT_FIELDS:
                account[field] = STATUS_UNKNOWN
            accounts.append(account)

    helper = _DomainHelper(os_config)

    valid = 0
    orphan = 0
    deleted = 0

    for account in accounts:
        if account['status_deleted'] == 'True':
            account['status'] = STATUS_DELETED
            deleted += 1
            LOG.warning("Account {0} is DELETED in {1}".format(account['account'], account['domain_id']))
            continue

        if account['domain_id'] == STATUS_UNKNOWN:
            continue

        domain = helper.get_domain(account['domain_id'])
        if not domain:
            # Try to find domain in default domain with project is
            domain = helper.get_default_domain(account['project_id'])
            if not domain:
                LOG.warning("Account {0} could not be verified in any keystone backend".format(account['account']))
                continue

        account['domain_name'] = domain.name
        account['backend'] = domain.backend

        project = domain.get_project(account['project_id'])
        if project:
            account['project_name'] = project.name
            if domain.enabled and project.enabled:
                account['status'] = STATUS_VALID
                valid += 1
                LOG.debug("Account {0} is VALID in {1}/{2}".format(account['account'], domain.name, project.name))
            else:
                account['status'] = STATUS_INVALID
                LOG.warning("Account {0} is INVALID in {1}/{2}".format(account['account'], domain.name, project.name))
        else:
            if not account['status_deleted'] == 'True':
                account['status'] = STATUS_ORPHAN
                LOG.warning("Account {0} is ORPHAN in {1}".format(account['account'], domain.name))

    orphan = len(accounts) - valid - deleted
    LOG.info("Account verification: Valid {0}, Orphans {1}, Deleted {2}, Overall {3}".format(valid, orphan, deleted,
                                                                                             len(accounts)))

    if statsd is not None:
        statsd.gauge('accounts.valid', valid)
        statsd.gauge('accounts.orphan', orphan)
        statsd.gauge('accounts.deleted', deleted)

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


class _DomainHelper(object):
    os_config = None
    domains = {}

    def __init__(self, os_config):
        self.os_config = os_config
        self._scrape()

    def _scrape(self):
        if len(self.os_config['scrape']) > 0:
            for scraper in self.os_config['scrape']:
                # Getting a whole project scrape
                sess = keystone_session(
                    auth_url=scraper.get('os_auth_url'),
                    admin_username=scraper.get('os_username'),
                    admin_user_id=scraper.get('os_user_id'),
                    admin_password=scraper.get('os_password'),
                    admin_user_domain_name=scraper.get('os_user_domain_name'),
                    admin_user_domain_id=scraper.get('os_user_domain_id'),
                    insecure=scraper.get('insecure'))

                kclnt = keystone_client(session=sess, interface=scraper.get('os_interface'))
                backend = keystone_get_backend_info(kclnt)

                count = 0
                try:
                    for domain in kclnt.domains.list():
                        count += 1
                        dom = DomainWrapper(domain.id)

                        dom.name = domain.name
                        dom.backend = backend
                        dom.enabled = domain.enabled

                        for project in kclnt.projects.list(domain=domain):
                            prj = ProjectWrapper(project.id)
                            prj.name = project.name
                            prj.enabled = project.enabled
                            dom.add_project(prj)

                        self.domains[dom.id] = dom

                    LOG.info("{0}: {1} domains scraped".format(scraper['cluster_name'], count))
                except (ke.BadRequest, ke.Unauthorized, ke.Forbidden, ke.NotFound) as err:
                    LOG.warn("{0}: scraping failed: {1}".format(scraper['cluster_name'], err.message))

    def get_domain(self, domain_id):
        if domain_id in self.domains:
            return self.domains[domain_id]
        else:
            for verifier in self.os_config['verify']:
                # Getting a domain scoped session
                sess = keystone_session(
                    auth_url=verifier.get('os_auth_url'),
                    admin_username=verifier.get('os_username'),
                    admin_user_id=verifier.get('os_user_id'),
                    admin_password=verifier.get('os_password'),
                    admin_user_domain_name=verifier.get('os_user_domain_name'),
                    admin_user_domain_id=verifier.get('os_user_domain_id'),
                    insecure=verifier.get('insecure'),
                    domain_id=domain_id)
                kclnt = keystone_client(session=sess, interface=verifier.get('os_interface'))

                try:
                    domain = kclnt.domains.get(domain_id)
                    backend = keystone_get_backend_info(kclnt)

                    dom = DomainWrapper(domain.id)
                    dom.name = domain.name
                    dom.enabled = domain.enabled
                    dom.backend = backend
                    dom.keystone_client = kclnt

                    self.domains[dom.id] = dom
                    return dom
                except (ke.BadRequest, ke.Unauthorized, ke.Forbidden, ke.NotFound) as err:
                    LOG.warn("{0}: {1} domain not in cluster: {2}".format(verifier['cluster_name'], domain_id,
                                                                          err.message))

    def get_default_domain(self, project_id):
        for dom in [v for k, v in list(self.domains.items()) if k.startswith('default_')]:
            if dom.get_project(project_id):
                return dom
