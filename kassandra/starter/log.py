#!/usr/bin/env python3

"""
@Author: apadoan
@Date: 15/06/2023
@Version: 1.0
@Objective: application logging and email
@TODO:
"""
import logging
import os.path
import smtplib
import sys
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import TimedRotatingFileHandler

from kassandra.config_module import app_config as conf


def notify_with_email(body: str, subject: str = "Kassandra execution error", ssl: bool = False) -> None:
    """
    Send email to users if there is an error
    :param body: body of the email
    :param subject: subject of the email
    :param ssl: if True, use SSL connection
    :return: None
    """

    if conf.mail_notification:
        log.debug(">> SENDING MAIL NOTIFICATION")

        # create a multipart message and set the headers
        message = MIMEMultipart()
        message['From'] = conf.user_mail
        message['Subject'] = subject

        # attach body to the email
        message.attach(MIMEText(body, 'plain'))

        try:
            with open(conf.logging_file, 'rb') as attachment:
                part = MIMEApplication(attachment.read())
            part.add_header('Content-Disposition', 'kassandra_log', filename='kassandra_log.txt')
            message.attach(part)
        except Exception as e:
            log.error(f"Error attaching log file: {str(e)}")

        # SMPT connection
        if ssl:
            server = smtplib.SMTP_SSL(conf.host_mail, conf.port_mail)
        else:
            server = smtplib.SMTP(conf.host_mail, conf.port_mail)

        # if verbose is True, print the debug
        if conf.verbose > 1:
            server.set_debuglevel(1)

        server.starttls()
        if conf.pass_mail != '':
            server.login(conf.user_mail, conf.pass_mail)
        server.ehlo()
        for receiver in conf.users_receiver_mail:
            message['To'] = receiver
            server.sendmail(conf.user_mail, receiver, message.as_string())
        server.quit()
        log.info(">> error mail sent to: " + str(conf.users_receiver_mail))


def logging_config() -> logging.Logger:
    """
    Configure logging file
    :return: logger
    """
    os.makedirs(conf.logging_base_path, exist_ok=True)

    logger = logging.getLogger()
    handler = TimedRotatingFileHandler(filename=conf.logging_file,
                                       when=conf.when_rotate_log,
                                       interval=1,
                                       backupCount=conf.log_backup_count,
                                       encoding='utf-8',
                                       delay=False)
    formatter = logging.Formatter(conf.logging_format, datefmt=conf.logging_date_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))

    if conf.verbose > 1:
        logger.setLevel(logging.DEBUG)
        logger.debug(">> DEBUG MODE ON")
    else:
        logger.setLevel(logging.INFO)

    if conf.verbose == 0:
        sys.stdout = open(os.devnull, 'w')

    logger.debug(f">> LOGGING IN {conf.logging_base_path}")

    return logger


log = logging_config()
