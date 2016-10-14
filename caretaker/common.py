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

import logging

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
from swiftclient.client import Connection


LOG = logging.getLogger(__name__)


def swift_connection(args):
    authurl = args.os_auth_url
    username = args.os_username
    password = args.os_password
    auth_version = args.os_auth_version
    cacert = args.os_cacert
    insecure = args.insecure

    os_options = {}
    args_dict = vars(args)
    for key, value in args_dict.iteritems():
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


def swift_upload(connection, container, object_name, contents, content_type='text/plain'):
    connection.put_container(container)
    connection.put_object(container, object_name, contents=contents, content_type=content_type)
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
                     domain_id, insecure=False):

    auth = v3.Password(auth_url=auth_url, username=admin_username, user_id=admin_user_id, password=admin_password,
                       user_domain_name=admin_user_domain_name, user_domain_id=admin_user_domain_id,
                       domain_id=domain_id)
    sess = session.Session(auth=auth, verify=(not insecure))

    return client.Client(session=sess)
