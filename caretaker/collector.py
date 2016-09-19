import fnmatch
import os
import sqlite3

from swift.account.backend import AccountBroker


def collect(device_dir='/srv/node', stale_reads_ok=False,
            reseller_prefix='AUTH_'):
    matches = []
    # Search the account DB files in the device dir
    for root, dirnames, filenames in os.walk(device_dir):
        if fnmatch.fnmatch(root, '*/accounts/*'):
            for filename in fnmatch.filter(filenames, '*.db'):
                matches.append(os.path.join(root, filename))

    accounts = []
    # Evaluate the Account information
    for match in matches:
        broker = AccountBroker(match, stale_reads_ok=stale_reads_ok)
        try:
            info = broker.get_info()
            meta = broker.metadata

            account = {'id': info['id'], 'account': info['account'],
                       'project': info['account'].replace(reseller_prefix, ''), 'object_count': info['object_count'],
                       'bytes_used': info['bytes_used'], 'created_at': info['created_at'],
                       'delete_timestamp': info['delete_timestamp']}
            if 'X-Account-Sysmeta-Project-Domain-Id' in meta:
                account['domain'] = str(meta['X-Account-Sysmeta-Project-Domain-Id'].pop(0))
            else:
                account['domain'] = '_unknown'
            if 'X-Account-Meta-Quota-Bytes' in meta:
                account['quota_bytes'] = int(meta['X-Account-Meta-Quota-Bytes'].pop(0))
            else:
                account['quota_bytes'] = 0

            accounts.append(account)
        except sqlite3.OperationalError as err:
            # TODO Log error
            print err.message

    return accounts


def output(accounts):
    print "Domain\tID\tProject\tCreated\tDeleted\tObject Count\tBytes Used\tQuota Bytes"
    for account in accounts:
        print '{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}'.format(account['domain'], account['id'],
                                                              account['project'], account['created_at'],
                                                              account['delete_timestamp'], account['object_count'],
                                                              account['bytes_used'], account['quota_bytes'])
