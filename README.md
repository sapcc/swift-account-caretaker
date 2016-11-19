# swift-account-caretaker
Collecting and merging swift account stats on swift account servers for the purpose of cleaning up orphaned accounts without a corresponding keystone project.

# How it works

## Phase 1 - Collect
In order to get a list of all known swift accounts, one need to ask swift itself to collect this list. Doing a project list via keystone is not suffient, because keystone projects might not use swift at all or are already deleted without deleting the corresponding swift account.
Therefore a collector job needs to run on all account servers, to collect account information from the db files stored on the disks used by the account ring. The most prominent information is the account name `AUTH_<project_id>`, the domain information from the system meta data tags, container counts, used bytes, ...
The collected information will be stored in swift itself and uploaded in a special caretaker project with one container per account server.

## Phase 2 - Merge
As account metadata for the same swift account is replicated in the cluster (usally 3 replicas), the collected data needs to be consolidated. For all account server information there is a list compiled for every domain, containing the account list. The condensed per domain information will be stored again in swift.

## Phase 3 - Verify
The list of swift accounts is verified against keystone, whether the corresponding project is still present in the domain. Information like domain name and project name will be added to the account info. Accounts which can not be found in keystone, will be marked with status Orphan. Domains and projects which where a keystone connection can not be established (e.g. no authorizations) will be marked with status unknown.

## Phase 2+3 - Mergify
Do step 2 and 3 in one step.

## Phase 4 - Cleanup
Accounts with status Ophan can be cleaned up by the swift reseller. Currently not supported - manual step.

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

source .caretaker
swift-account-caretaker --log-level=info collect
```
