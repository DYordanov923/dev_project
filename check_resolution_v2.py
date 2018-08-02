import Queue
import cPickle
import logging
import os
import signal
import sys
import threading
import time
import datetime
import email.utils
import smtplib

import datetime
from time import mktime
from datetime import datetime

from resolution_log_script import store_log_info, load_df, create_df
from upload_img import auto_resize_img, upload_to_S3
from download_image import download_from_S3
from pathlib import Path

import boto3
import click
import pygerduty
from PIL import ImageFile
from boto.s3.connection import S3Connection
from email.mime.text import MIMEText


lock = threading.Lock()

EMAIL_DISTRIBUTION = ["dyordanov@xi-group.com"]
EMAIL_SERVER = "mail.xi-group.com"
EMAIL_PORT = "465"
EMAIL_USERNAME = "dyordanov@xi-group.com"
EMAIL_PASSWORD = "aepheeCe9ax9"

PAGERDUTY_SUBDOMAIN = "cognet"
PAGERDUTY_TOKEN = "ab002c857a9547a7a56521f293678feb"
pickle_filepath = "/home/ubuntu/environments/project_files/ingest-wrong-resolution.pickle"

VALID_RESOLUTIONS = [
    (1920, 1080),
    (1280, 720),
    (720, 480),
    (704, 480),
]

WHITELIST = [
    'ZLVINGH',
    'SMTHHD',
    'KSCIDT',
    'BHERHD',
    'INSPHD',
    'LOGOHD',
    'KENVDT',
]

path = '/home/ubuntu/environments/img_dir2/log_dataframe_4.csv'
path_2 = '/home/ubuntu/environments/img_dir4/'


class MailSender(object):

    def __init__(self, host, sender, password, port=587, type='plain', verbose=False):
        self.host = host
        self.sender = sender
        self.password = password
        self.port = port
        self.type = type
        self.verbose = verbose

    def __call__(self, message, *recipients, **headers):

        if not recipients:
            return  # nothing to do

        # construct the message
        msg = MIMEText(message, self.type)
        headers.setdefault('From', self.sender)
        headers.setdefault('To', ','.join(recipients))
        for key, value in headers.items():
            msg[key] = value
       # print("construct - ok ")
        # connect to mail server
       # print(self.host, self.port)
        server = smtplib.SMTP_SSL(self.host, self.port)
       # print("connect - ok")
        try:
            if self.verbose:
                server.set_debuglevel(True)

            # identify ourselves, prompting server for supported features
            server.ehlo()

            # If we can encrypt this session, do it
            if server.has_extn('STARTTLS'):
                server.starttls()
                server.ehlo() # re-identify ourselves over TLS connection

            # login
            server.login(self.sender, self.password)
        #    print("login - ok")
            # send the email
            server.sendmail(self.sender, recipients, msg.as_string())
         #   print("send - ok")
        finally:
            server.quit()


def get_bucket(bucket_name):
    aws_connection = S3Connection()
    return aws_connection.get_bucket(bucket_name)


def get_image_resolution(s3, bucket=None, key=None):
    end = 1024
    parser = ImageFile.Parser()
    chunk = s3.get_object(Bucket=bucket, Key=key, Range='bytes={}-{}'.format(0, end))
    while chunk:
        parser.feed(chunk["Body"].read())
        if parser.image:
            break
        end += 1024
        time.sleep(0.1)
        chunk = s3.get_object(Bucket=bucket, Key=key, Range='bytes={}-{}'.format(0, end))
    return parser.image.size


def sigterm_handler(*args):
    raise SystemExit("Exiting on signal %s" % args[0])


def pull_size(input_queue, output, conn, mybucket, my_df, iteration):
    try:
        while not input_queue.empty():
            job = input_queue.get(True, 1)
            image = job["image"]
            time_st = job["time_st"]
          #  print(image, time_st)
            key = image
            size = None
            try:
                size = get_image_resolution(conn, bucket=mybucket, key=key)
                duration = store_log_info(key, size, time_st, path, VALID_RESOLUTIONS, my_df, iteration)
           #     print(image+" duration " + str(duration))

            except Exception as e:
                logging.info("S3 Exception on key=%s" % key)
                logging.error(e)
                time.sleep(1)
            if size:
                valid = True if size in VALID_RESOLUTIONS else False
                channel = key.split('_')[-1].replace(".jpg", "")
                server = key.replace('/', '_').split('_')[-2]
                if not valid and channel not in WHITELIST:
                    output.put({
                        'image': key,
                        'channel': channel,
                        'server': server,
                        'size': size,
                        'valid': valid,
                        'duration':duration
                    })
                    logging.warning("invalid resolution %s %s" % (key, size))
    except Exception as e:
        raise SystemExit("Exception occured %s" % e)


def push_sizes(input_queue, output):
    errors_text = []
    old_errors = []
    errors = []
  #  print("push")
    while not input_queue.empty() or not output.empty():
        if not output.empty():
            job = output.get(True, 1)
            valid = job.get('valid')
            if not valid:
                errors.append(job)
                errors_text.append("%s on %s have wrong resolution of %s x %s. Duration: %s" % (
                    job['channel'], job['server'], job['size'][0], job['size'][1], job['duration']))
        else:
            time.sleep(1)
    lock.acquire()
    if errors:
        print("-------------------------\n")
        print("Found errors: "+ str(len(errors)))
        if not os.path.exists(pickle_filepath):
            with open(pickle_filepath, 'w') as pickle_handle:
                cPickle.dump(errors, pickle_handle)
        else:
            with open(pickle_filepath) as pickle_handle:
                old_errors = cPickle.load(pickle_handle)
        if errors != old_errors:
            msg = '\n'.join(errors_text)
            header = errors_text[0]
            if len(errors) > 1:
                header += " (more channels listed below)"
            send_alert(header, msg, 'ingest-wrong-resolution',)
        else:
            logging.warning("Ignoring previously reported errors.")
    else:
        logging.info("No errors found.")
        resolve_alert("", "", 'ingest-wrong-resolution')
    with open(pickle_filepath, 'w') as pickle_handle:
        cPickle.dump(errors, pickle_handle)
    lock.release()

def send_alert(description, details, event_name):
    try:
   #     print("alert-pd")
       # pager = pygerduty.PagerDuty(PAGERDUTY_SUBDOMAIN, PAGERDUTY_TOKEN)
       # pager.create_event(PAGERDUTY_TOKEN, description, "trigger", details, event_name)
    #    print("try mail")
        sender = MailSender(EMAIL_SERVER, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_PORT)
        sender(details, *EMAIL_DISTRIBUTION, subject = details)
     #   print("aler-mail")
    finally:
        pass


def resolve_alert(description, details, event_name):
    try:
        pager = pygerduty.PagerDuty(PAGERDUTY_SUBDOMAIN, PAGERDUTY_TOKEN)
        pager.create_event(PAGERDUTY_TOKEN, description, "resolve", details, event_name)
    finally:
        pass


def main_v4(profile, region, bucket_name, verbose):
    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO if verbose else logging.ERROR,
        format='%(asctime)s [%(levelname)s] (%(process)d:%(threadName)s:%(filename)s:%(lineno)s) %(message)s')

    session = boto3.session.Session(profile_name=profile or None)
    client = session.client('s3', region_name=region)
    bucket = get_bucket(bucket_name)
    images = [i.name for i in bucket.list()]
    img_ts = []
    for key in bucket.list():
        dt = time.strptime(key.last_modified[:19], "%Y-%m-%dT%H:%M:%S")
        dt2 = datetime.fromtimestamp(mktime(dt))
        ts = time.mktime(dt2.timetuple())
        img_ts.append(ts)


    _start = time.time()
    threads = []
    input_queue = Queue.Queue()
    output_queue = Queue.Queue()

    for image,ts in zip(images, img_ts):
        input_queue.put({
            'image': image,
            'time_st': ts,
        })
    file_ = Path(path)
    if file_.exists():
	   my_df = load_df(path)
	   rnd = 1 
	   iteration = my_df["Iteration"].iloc[-1]+1
    else:
	   my_df=create_df()
	   rnd = 0
	   iteration = 0


    _queue_size = input_queue.qsize()
   # print("queue size: " + str(_queue_size))
    for thread in range(10):
        t = threading.Thread(target=pull_size, args=[input_queue, output_queue, client, bucket_name, my_df, iteration])
        t.daemon = True
        t.start()
        threads.append(t)

    for thread in threads:
	thread.join()

   # print("output_queue size:" + str(output_queue.qsize()))
    t = threading.Thread(target=push_sizes, args=[input_queue, output_queue])
    t.daemon = True
    t.start()


    threads.append(t)
    for thread in threads:
        thread.join()
    _end = time.time() - _start
    logging.info("Processed %s screenshots. %.1f screenshots/s" % (_queue_size, _queue_size / _end))
    log_df = load_df(path)
    print(log_df) 
    print("Done")

@click.command(name='my-cli', context_settings={'ignore_unknown_options': True, 'allow_extra_args': True})
@click.option('--profile', default='', show_default=True)
@click.option('--region', default='us-east-1', show_default=True)
@click.option('--bucket_name', default='ops-sofia-dev', show_default=True)
@click.option('-v', '--verbose/--no-verbose', default=False, show_default=True)
def main(profile, region, bucket_name, verbose):
    for rnd in range(10):
        main_v4(profile, region, bucket_name, verbose)
        time.sleep(4)
        if rnd %4 ==0:
           download_from_S3()
           print("\n   Resize...")
           for i in range(4):
               auto_resize_img(path_2)
           upload_to_S3()
        



if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigterm_handler)
    main()

