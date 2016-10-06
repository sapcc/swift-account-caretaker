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

from swiftclient.client import Connection


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


def swift_upload(connection, container, obj_name, contents, content_type='text/plain'):
    connection.put_container(container)
    connection.put_object(container, obj_name, contents=contents, content_type=content_type)

    print container + '/' + obj_name + " uploaded"


def swift_download(connection, container, prefix=None):
    contents = ''
    for object_data in connection.get_container(container, prefix)[1]:
        body = connection.get_object(container, object_data['name'])[1]
        contents += body
        print container + '/' + object_data['name'] + " downloaded"

    return contents
