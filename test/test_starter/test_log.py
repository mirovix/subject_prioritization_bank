#!/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: test log functions
@TODO:
"""
import logging
import smtplib
import unittest
from unittest.mock import MagicMock, patch

from kassandra.starter.log import logging_config, notify_with_email
from kassandra.config_module import app_config as apc


class TestLog(unittest.TestCase):
    @patch('smtplib.SMTP', MagicMock())
    def test_notify_with_email(self):
        body = "Test email body"
        apc.mail_notification = True
        apc.user_mail = "test@mail.com"
        apc.pass_mail = "test_password"
        apc.host_mail = "smtp.gmail.com"
        apc.users_receiver_mail = ["user1@mail.com", "user2@mail.com"]
        notify_with_email(body)
        smtplib.SMTP().sendmail.assert_called()

    def test_log_config_instance(self):
        self.assertIsInstance(logging_config(), logging.Logger)


if __name__ == '__main__':
    unittest.main()
