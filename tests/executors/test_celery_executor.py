# -*- coding: utf-8 -*-
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
import os
import sys
import unittest
import mock

from celery import Celery
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop
from parameterized import parameterized

from airflow.executors.celery_executor import app
from airflow.executors.celery_executor import CeleryExecutor
from airflow.executors.celery_executor import execute_command
from airflow.executors.celery_executor import celery_configuration
from airflow.executors.celery_executor import CELERY_FETCH_ERR_MSG_HEADER
from airflow.utils.state import State

from airflow import configuration
configuration.load_test_config()

# leave this it is used by the test worker
import celery.contrib.testing.tasks  # noqa: F401


def _prepare_test_bodies():
    return [
        (url, )
        for url in os.environ['CELERY_BROKER_INTEGRATION_URLS'].split(',')
    ]


class CeleryExecutorTest(unittest.TestCase):

    @unittest.skipIf('sqlite' in configuration.conf.get('core', 'sql_alchemy_conn'),
                     "sqlite is configured with SequentialExecutor")
    @parameterized.expand(_prepare_test_bodies())
    def test_celery_integration(self, broker_url):
        test_config = dict(celery_configuration)
        test_config.update({'broker_url': broker_url})
        test_app = Celery(
            broker_url,
            config_source=test_config)
        test_execute_command = test_app.task(execute_command.__wrapped__)
        patch_execute_command = mock.patch(
            'airflow.executors.celery_executor.execute_command',
            test_execute_command)

        executor = CeleryExecutor()
        executor.start()
        worker = start_worker(app=test_app, logfile=sys.stdout, loglevel='debug')

        with worker, patch_execute_command:

            success_command = ['true', 'some_parameter']
            fail_command = ['false', 'some_parameter']

            executor.execute_async(key='success', command=success_command)
            # errors are propagated for some reason
            try:
                executor.execute_async(key='fail', command=fail_command)
            except Exception:
                pass
            executor.running['success'] = True
            executor.running['fail'] = True

            executor.end(synchronous=True)

        self.assertTrue(executor.event_buffer['success'], State.SUCCESS)
        self.assertTrue(executor.event_buffer['fail'], State.FAILED)

        self.assertNotIn('success', executor.tasks)
        self.assertNotIn('fail', executor.tasks)

        self.assertNotIn('success', executor.last_state)
        self.assertNotIn('fail', executor.last_state)

        # Clear event loop to tear down each celery instance
        set_event_loop(None)

    def test_exception_propagation(self):
        @app.task
        def fake_celery_task():
            return {}

        mock_log = mock.MagicMock()
        executor = CeleryExecutor()
        executor._log = mock_log

        executor.tasks = {'key': fake_celery_task()}
        executor.sync()
        mock_log.error.assert_called_once()
        args, kwargs = mock_log.error.call_args_list[0]
        log = args[0]
        # Result of queuing is not a celery task but a dict,
        # and it should raise AttributeError and then get propagated
        # to the error log.
        self.assertIn(CELERY_FETCH_ERR_MSG_HEADER, log)
        self.assertIn('AttributeError', log)


if __name__ == '__main__':
    unittest.main()
