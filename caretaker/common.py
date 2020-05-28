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

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
import logging
import random

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient import exceptions as ke
from keystoneclient.v3 import client
from swiftclient.client import Connection
from urllib.parse import urlparse


LOG = logging.getLogger(__name__)


def swift_connection(os_config):
    authurl = os_config.get('os_auth_url')
    username = os_config.get('os_username')
    password = os_config.get('os_password')
    auth_version = os_config.get('os_auth_version')
    cacert = os_config.get('os_cacert')
    insecure = os_config.get('insecure')

    os_options = {}
    for key, value in list(os_config.items()):
        if key.startswith('os_'):
            key = key.lstrip('os_')
            os_options[key] = value

    return Connection(authurl=authurl,
                      user=username,
                      key=password,
                      auth_version=auth_version,
                      cacert=cacert,
                      insecure=insecure,
                      os_options=os_options)


def swift_upload(connection, container, object_name, contents, content_type='text/plain', headers=None):
    connection.put_container(container)
    connection.put_object(container, object_name, contents=contents, content_type=content_type, headers=headers)
    LOG.info("{0}/{1}/{2} successfully uploaded".format(connection.url, container, object_name))


def swift_download(connection, container, object_name):
    content = connection.get_object(container, object_name)[1]
    LOG.info("{0}/{1}/{2} successfully downloaded".format(connection.url, container, object_name))
    return content


def swift_download_all(connection, container, prefix):
    contents = ''
    i = 0
    for object_data in connection.get_container(container, prefix=prefix)[1]:
        body = connection.get_object(container, object_data['name'])[1]
        contents += body
        i += 1

    LOG.info("{0} objects from {1}/{2}/{3} successfully downloaded".format(i, connection.url, container, prefix))
    return contents


def keystone_session(auth_url, admin_username, admin_user_id, admin_password,
                     admin_user_domain_name, admin_user_domain_id,
                     domain_id=None, insecure=False):

    auth = v3.Password(auth_url=auth_url, username=admin_username, user_id=admin_user_id, password=admin_password,
                       user_domain_name=admin_user_domain_name, user_domain_id=admin_user_domain_id,
                       domain_id=domain_id)
    return session.Session(auth=auth, verify=(not insecure))


def keystone_client(session, interface=None):
    return client.Client(session=session, interface=interface)


def keystone_get_backend_info(kclnt):
    try:
        svc = kclnt.services.list(type='identity')
        svc = svc.pop()
        parsed_uri = urlparse(svc.links['self'])
        backend = parsed_uri.hostname
        LOG.debug("Backend Service Self URL {0}".format(backend))

        return backend
    except Exception as err:
        LOG.warning("Backend could not be determined: {0}".format(err.message))
        return '_unknown'


class DomainWrapper(object):
    def __init__(self, domain_id):
        if domain_id == 'default':
            # Don't rely on default domain ID from scraper, that can be in every keystone backend present
            domain_id = domain_id + '_' + str(random.randint(0, 1000))

        self.id = domain_id
        self.name = None
        self.enabled = False
        self.projects = {}
        self.keystone_client = None

    def add_project(self, project):
        self.projects[project.id] = project

    def get_project(self, project_id):
        if project_id in self.projects:
            return self.projects[project_id]
        elif self.keystone_client:
            try:
                project = self.keystone_client.projects.get(project_id)
                prj = ProjectWrapper(project.id)
                prj.name = project.name
                prj.enabled = project.enabled
                self.add_project(project)
                return prj
            except ke.NotFound as err:
                LOG.debug("DomainID {0}/ProjectID {1}: {2}".format(self.id, project_id, err.message))
            except Exception as err:
                LOG.warning("DomainID {0}/ProjectID {1}: {2}".format(self.id, project_id, err.message))


class ProjectWrapper(object):
    def __init__(self, project_id):
        self.id = project_id
        self.name = None
        self.enabled = False
