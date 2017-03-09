import smtplib
from configparser import ConfigParser
import os


class send_email():
    def __init__(self):
        self.parser = ConfigParser()
        self.parser.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "read_email.ini"))
        self.fromaddr = self.parser.get('email_configuration', 'user_email')
        self.toaddrs  = self.parser.get('email_configuration', 'to_email')
        self.username = self.parser.get('email_configuration', 'user_email')
        self.password = self.parser.get('email_configuration', 'pwd_email')
        self.server = smtplib.SMTP(self.parser.get('email_configuration', 'imap_smtp_server'))

    def send_msg(self, msg):

        msg_to_send = "\r\n".join(["From: " + self.parser.get('email_configuration', 'user_email'),
                                   "To: " + self.parser.get('email_configuration', 'to_email'),
                                   "Subject: Errore associazione codici.",
                                   "", msg])


        self.server.ehlo()
        self.server.starttls()
        self.server.login(self.username, self.password)
        self.server.sendmail(self.fromaddr, self.toaddrs, msg_to_send.encode())
        self.server.quit()
