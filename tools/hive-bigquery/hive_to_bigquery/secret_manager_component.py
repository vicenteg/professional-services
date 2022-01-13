# Copyright 2022 Google Inc.
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
"""Retrieves the MySQL password from Secret Manager"""

from google.cloud import secretmanager

from hive_to_bigquery import client_info


def access_secret(project_id, location_id, secret_id):
    """Retrieves the MySQL password from Secret Manager"""

    # Creates an API client for the Secret Manager API.
    client = secretmanager.SecretManagerServiceClient()

    secret = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=secret)

    return response.payload.data
