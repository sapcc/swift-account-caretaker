# swift-account-caretaker
Swift account maintenance workflows like collecting stats or setting account ratelimits.

# Binaries

## swift-account-caretaker
Collecting and merging swift account stats on swift account servers for the purpose of cleaning up orphaned accounts
without a corresponding keystone project.

## swift-account-ratelimit
Quickly blacklist or whitelist an account with the internal client
(https://docs.openstack.org/swift/latest/ratelimit.html#black-white-listing).

```
# Get Account ratelimit setting
swift-account-ratelimit --account AUTH_123

# Set Account ratelimit
swift-account-ratelimit --account AUTH_123 --limit 25

# Blacklist Account
swift-account-ratelimit --account AUTH_123 --blacklist

# Whitelist Account
swift-account-ratelimit --account AUTH_123 --whitelist

# remove Account ratelimit setting
swift-account-ratelimit --account AUTH_123 --remove

```

# Caretaker - How it works

## Phase 1 - Collect
In order to get a list of all known swift accounts, we need to ask swift itself to collect this list. Doing a project
list via keystone is not sufficient because keystone projects might not use swift at all or are already deleted
without deleting the corresponding swift account.
Therefore, a collector job needs to run on all account servers in order to collect account information from the db files stored
on the disks used by the account ring. The most prominent information is the account name `AUTH_<project_id>`,
the domain information from the system meta data tags, container counts, used bytes, ...
The collected information will be stored in swift itself and uploaded in a special caretaker project with one container
per account server.

## Phase 2 - Merge
As account metadata for the same swift account is replicated in the cluster (usally 3 replicas), the collected data
needs to be consolidated. For all account server information there is a list compiled for every domain containing
the account list. The condensed per domain information will be stored again in swift.

## Phase 3 - Verify
The list of swift accounts is verified against keystone to check whether the corresponding project is still present in the
domain. Information like domain name and project name will be added to the account info. Accounts which can not be
found in keystone will be marked with status Orphan. Domains and projects where a keystone connection can not
be established (e.g. no authorizations) will be marked with status unknown.

## Phase 2+3 - Mergify
Do step 2 and 3 in one step.

## Phase 4 - Cleanup
Accounts with status Orphan can be cleaned up by the swift reseller. Currently not supported - manual step.

# Installation
```
pip install git+https://github.com/sapcc/swift-account-caretaker.git
```

or

```
git clone https://github.com/sapcc/swift-account-caretaker.git
cd swift-account-caretaker
pip install -r requirements.txt
python setup.py install
```

# Usage
```
# Get help
swift-account-caretaker --help
swift-account-caretaker collect --help
swift-account-caretaker merge --help
swift-account-caretaker verify --help

swift-account-caretaker --config-file path/to/config.yaml collect
```

# Details
`swift-account-caretaker` is able to handle multiple keystone backends. This is helpful, if your swift cluster is
set up to serve multiple keystone clusters (e.g. if you deploy multiple swift-proxies, connected to different keystones).
Caretaker expects in general, that the configured user can get a domain scoped token to verify the project. It can also
be configured with a keystone admin wide access. In that case `scraped: true ` must be set and all domains and projects
of a keystone project will be scraped.

Example config:
```yaml
common:
  os_auth_url: https://<KEYSTONE>:5000/v3
  os_user_domain_name: <USERDOMAIN_FOR_CARETAKER>
  os_username: <USER_FOR_CARETAKER>
  os_password: <PASSWORD>
  os_project_domain_name: <PROJECTDOMAIN_FOR_CARETAKER>
  os_project_name: <PROJECT_FOR_CARETAKER>

verify:
  - cluster_name: cluster-1
    os_auth_url: https://<KEYSTONE>:5000/v3
    os_user_domain_name: <USERDOMAIN_FOR_KEYSTONE_ADMIN>
    os_username: <USER_FOR_KEYSTONE_ADMIN>
    os_password: <PASSWORD>
    os_project_domain_name: <PROJECTDOMAIN_FOR_KEYSTONE_ADMIN>
    os_project_name: <PROJECT_FOR_KEYSTONE_ADMIN>

  - cluster_name: cluster-2
    scrape: true
    os_auth_url: https://<KEYSTONE-2>:5000/v3
    os_user_domain_name: <USERDOMAIN_FOR_KEYSTONE_ADMIN>
    os_username: <USER_FOR_KEYSTONE_ADMIN>
    os_password: <PASSWORD>
    os_project_domain_name: <PROJECTDOMAIN_FOR_KEYSTONE_ADMIN>
    os_project_name: <PROJECT_FOR_KEYSTONE_ADMIN>
```

Instead of providing `os_password` as plain text in the config file, you can
use a special syntax to read the respective password from an exported
environment variable:

```yaml
os_password: { fromEnv: ENVIRONMENT_VARIABLE }
```
