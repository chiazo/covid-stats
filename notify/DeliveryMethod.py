import os
from os import environ as env
from dotenv import load_dotenv
from datetime import date
from .PyMessenger import Email, Messenger
from etext import send_sms_via_email


class DeliveryMethod:
    carriers = {
        "att": "@txt.att.net",
        "tmobile": "@tmomail.net",
        "verizon": "@vtext.com",
        "sprint": "@messaging.sprintpcs.com"
    }

    def __init__(self, email, phone, carrier, message) -> None:
        self.email = email
        self.phone = phone
        self.carrier = carrier
        self.message = message
        self.send()

    def send(self):
        load_dotenv(
            f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.env")
        today = date.today().strftime("%m/%d/%Y")
        subject = f'[covid-stats] - {today}'
        self.send_text(self.message, subject)
        self.send_email(self.message, subject)

    def send_text(self, content, subject):
        sender_credentials = (env['GMAIL_USER'], env['GMAIL_PASSWORD'])

        return send_sms_via_email(
            self.phone, content, self.carrier, sender_credentials, subject=subject
        )

    def send_email(self, content, subject):
        my_messenger = Messenger(env['GMAIL_USER'], env['GMAIL_PASSWORD'])
        to = self.email
        msg = Email(to, subject, content, is_HTML=False)
        return my_messenger.send_email(msg, one_time=True)