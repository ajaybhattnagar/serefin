
#
# Central Collection of routines
#

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import sys


def left(s, amount):
    return s[:amount]


def right(s, amount):
    return s[-amount:]


def mid(s, offset, amount):
    return s[offset:offset+amount]


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def send_mail(send_from, send_to, send_cc, subject, msg_text, msg_html, files, server, port, reply_to,
              username='', password='',
              priority = False, istls=True):
    #
    # 2019-10-21 FSB 1: Added X-Priority header item
    # 2019-11-02 FSB 1: Added msg_html to provide HTML based Email handling
    #                2: Renamed parameter text to msg_text
    #
    msg = MIMEMultipart('alternative')
    msg['From'] = send_from
    msg['To'] = send_to
    if send_cc is not None:
        msg['Cc'] = send_cc
    if priority:
        msg['X-Priority'] = '2'
    if reply_to is None:
        msg['reply_to'] = 'noreply@travelnation.com'
    else:
        msg['reply-to'] = reply_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    if files is not None:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(files, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="' + files + '" ')
        msg.attach(part)

    if msg_text is not None:
        msg.attach(MIMEText(msg_text ,'plain'))
    if msg_html is not None:
        msg.attach(MIMEText(msg_html, 'html'))

    destination = send_to.split(",") + send_cc.split(",")
    #destination = send_to
    # context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
    # SSL connection only working on Python 3+
    try:
        smtp = smtplib.SMTP(server, port)
        smtp.ehlo()
        if istls:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(username, password)
        smtp.sendmail(send_from, destination, msg.as_string())
        smtp.quit()
    except smtplib.SMTPResponseException as e:
        sys.exit(1)


