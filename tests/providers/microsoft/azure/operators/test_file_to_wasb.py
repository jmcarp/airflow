#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import datetime
import unittest

import mock

from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.file_to_wasb import FileToWasbOperator


class TestFileToWasbOperator(unittest.TestCase):

    _config = {
        'file_path': 'file',
        'container_name': 'container',
        'blob_name': 'blob',
        'wasb_conn_id': 'wasb_default',
        'retries': 3,
    }

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_init(self):
        operator = FileToWasbOperator(
            task_id='wasb_operator',
            dag=self.dag,
            **self._config
        )
        assert operator.file_path == self._config['file_path']
        assert operator.container_name == \
                         self._config['container_name']
        assert operator.blob_name == self._config['blob_name']
        assert operator.wasb_conn_id == self._config['wasb_conn_id']
        assert operator.load_options == {}
        assert operator.retries == self._config['retries']

        operator = FileToWasbOperator(
            task_id='wasb_operator',
            dag=self.dag,
            load_options={'timeout': 2},
            **self._config
        )
        assert operator.load_options == {'timeout': 2}

    @mock.patch('airflow.providers.microsoft.azure.operators.file_to_wasb.WasbHook',
                autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = FileToWasbOperator(
            task_id='wasb_sensor',
            dag=self.dag,
            load_options={'timeout': 2},
            **self._config
        )
        operator.execute(None)
        mock_instance.load_file.assert_called_once_with(
            'file', 'container', 'blob', timeout=2
        )


if __name__ == '__main__':
    unittest.main()
