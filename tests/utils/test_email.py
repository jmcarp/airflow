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
import tempfile
import unittest
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import mock

from airflow import utils
from airflow.configuration import conf
from airflow.utils.email import get_email_address_list
from tests.test_utils.config import conf_vars
import pytest

EMAILS = ['test1@example.com', 'test2@example.com']

send_email_test = mock.MagicMock()


class TestEmail(unittest.TestCase):

    def test_get_email_address_single_email(self):
        emails_string = 'test1@example.com'

        assert get_email_address_list(emails_string) == [emails_string]

    def test_get_email_address_comma_sep_string(self):
        emails_string = 'test1@example.com, test2@example.com'

        assert get_email_address_list(emails_string) == EMAILS

    def test_get_email_address_colon_sep_string(self):
        emails_string = 'test1@example.com; test2@example.com'

        assert get_email_address_list(emails_string) == EMAILS

    def test_get_email_address_list(self):
        emails_list = ['test1@example.com', 'test2@example.com']

        assert get_email_address_list(emails_list) == EMAILS

    def test_get_email_address_tuple(self):
        emails_tuple = ('test1@example.com', 'test2@example.com')

        assert get_email_address_list(emails_tuple) == EMAILS

    def test_get_email_address_invalid_type(self):
        emails_string = 1

        with pytest.raises(TypeError):
            get_email_address_list(emails_string)

    def test_get_email_address_invalid_type_in_iterable(self):
        emails_list = ['test1@example.com', 2]

        with pytest.raises(TypeError):
            get_email_address_list(emails_list)

    def setUp(self):
        conf.remove_option('email', 'EMAIL_BACKEND')

    @mock.patch('airflow.utils.email.send_email')
    def test_default_backend(self, mock_send_email):
        res = utils.email.send_email('to', 'subject', 'content')
        mock_send_email.assert_called_once_with('to', 'subject', 'content')
        assert mock_send_email.return_value == res

    @mock.patch('airflow.utils.email.send_email_smtp')
    def test_custom_backend(self, mock_send_email):
        with conf_vars({('email', 'email_backend'): 'tests.utils.test_email.send_email_test'}):
            utils.email.send_email('to', 'subject', 'content')
        send_email_test.assert_called_once_with(
            'to', 'subject', 'content', files=None, dryrun=False,
            cc=None, bcc=None, mime_charset='utf-8', mime_subtype='mixed')
        assert not mock_send_email.called


class TestEmailSmtp(unittest.TestCase):
    def setUp(self):
        conf.set('smtp', 'SMTP_SSL', 'False')

    @mock.patch('airflow.utils.email.send_mime_email')
    def test_send_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name])
        assert mock_send_mime.called
        call_args = mock_send_mime.call_args[0]
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == call_args[0]
        assert ['to'] == call_args[1]
        msg = call_args[2]
        assert 'subject' == msg['Subject']
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == msg['From']
        assert 2 == len(msg.get_payload())
        filename = 'attachment; filename="' + os.path.basename(attachment.name) + '"'
        assert filename == msg.get_payload()[-1].get('Content-Disposition')
        mimeapp = MIMEApplication('attachment')
        assert mimeapp.get_payload() == msg.get_payload()[-1].get_payload()

    @mock.patch('airflow.utils.email.send_mime_email')
    def test_send_smtp_with_multibyte_content(self, mock_send_mime):
        utils.email.send_email_smtp('to', 'subject', '🔥', mime_charset='utf-8')
        assert mock_send_mime.called
        call_args = mock_send_mime.call_args[0]
        msg = call_args[2]
        mimetext = MIMEText('🔥', 'mixed', 'utf-8')
        assert mimetext.get_payload() == msg.get_payload()[0].get_payload()

    @mock.patch('airflow.utils.email.send_mime_email')
    def test_send_bcc_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name], cc='cc', bcc='bcc')
        assert mock_send_mime.called
        call_args = mock_send_mime.call_args[0]
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == call_args[0]
        assert ['to', 'cc', 'bcc'] == call_args[1]
        msg = call_args[2]
        assert 'subject' == msg['Subject']
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == msg['From']
        assert 2 == len(msg.get_payload())
        assert 'attachment; filename="' + os.path.basename(attachment.name) + '"' == \
                         msg.get_payload()[-1].get('Content-Disposition')
        mimeapp = MIMEApplication('attachment')
        assert mimeapp.get_payload() == msg.get_payload()[-1].get_payload()

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        msg = MIMEMultipart()
        utils.email.send_mime_email('from', 'to', msg, dryrun=False)
        mock_smtp.assert_called_once_with(
            conf.get('smtp', 'SMTP_HOST'),
            conf.getint('smtp', 'SMTP_PORT'),
        )
        assert mock_smtp.return_value.starttls.called
        mock_smtp.return_value.login.assert_called_once_with(
            conf.get('smtp', 'SMTP_USER'),
            conf.get('smtp', 'SMTP_PASSWORD'),
        )
        mock_smtp.return_value.sendmail.assert_called_once_with('from', 'to', msg.as_string())
        assert mock_smtp.return_value.quit.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_ssl(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({('smtp', 'smtp_ssl'): 'True'}):
            utils.email.send_mime_email('from', 'to', MIMEMultipart(), dryrun=False)
        assert not mock_smtp.called
        mock_smtp_ssl.assert_called_once_with(
            conf.get('smtp', 'SMTP_HOST'),
            conf.getint('smtp', 'SMTP_PORT'),
        )

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_noauth(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({
            ('smtp', 'smtp_user'): None,
            ('smtp', 'smtp_password'): None,
        }):
            utils.email.send_mime_email('from', 'to', MIMEMultipart(), dryrun=False)
        assert not mock_smtp_ssl.called
        mock_smtp.assert_called_once_with(
            conf.get('smtp', 'SMTP_HOST'),
            conf.getint('smtp', 'SMTP_PORT'),
        )
        assert not mock_smtp.login.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_dryrun(self, mock_smtp, mock_smtp_ssl):
        utils.email.send_mime_email('from', 'to', MIMEMultipart(), dryrun=True)
        assert not mock_smtp.called
        assert not mock_smtp_ssl.called
