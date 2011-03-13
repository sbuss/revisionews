from __future__ import division

import argparse
import datetime
import logging
import logging.config
import time

from tracker import sqs_utils
from tracker import store

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("process")
log.level = logging.DEBUG

class Processor(object):
    def __init__(self, queue_name, max_wait, min_wait, log):
        self.q = sqs_utils.get_queue(queue_name)
        self.max_wait = max_wait
        self.min_wait = min_wait
        self.current_wait = min_wait
        self.log = log
        self.h = httplib2.Http()

    def reset_timer(self):
        self.last_access = datetime.datetime.now()

    def wait(self):
        d = datetime.datetime.now() - self.last_access
        dt = (d.microseconds / 1000) + (d.seconds * 1000)
        log.debug("dt is %s" % dt)
        if dt < 0:
            # uh, just in case
            dt = dt * -1;

        if dt > self.current_wait:
            log.debug("not sleeping")
            return
        else:
            time.sleep((self.current_wait - dt)/1000)

    def decrease_wait(self):
        self.current_wait = self.min_wait

    def increase_wait(self):
        self.current_wait = min(self.current_wait * 2, self.max_wait)

    def process(self, message):
        if message is None:
            return
        url = message.get_body()
        try:
            headers = {}
            netloc = urlparse.urlsplit(url).netloc
            # Set a fake referer to bypass the NYT paywalls
            # This will probably have to be a randomly generated string in the 
            # future
            headers = {"Referer": "http://google.com/search?q=lol"}
            #if netloc == "feeds.nytimes.com":
            #    # This should probably be in a private file or database
            #    headers = {'Cookie': 'RMID=27fdc70e626f4cff2ea78bc5; '\
            #                         'news_people_toolbar=NO; '\
            #                         'NYT-S=1MFOBsMFQ5HrthY2AudgX82CsxqX2R3FWQNFTzEsjE7ewxMbeVLMH5z8lsz4u7c8EtdeFz9JchiAKlcdq98aPCnV8mQ24pJudJ5ndB4SPd4U0kugGLwnLmijbHvRv.3TVugi0M8YQw/rMN20SBRlEJ1w00'}

            # because the link in SQS was the feed url, fetch the page so we
            # can follow redirects
            (response, content) = self.h.request(url, headers=headers)
            # find the actual content

            # check SimpleDB to see if this url has been stored before

            # compare checksums of content, if different create new entry and 
            # post an alert

            # store a compressed snapshot in S3
            print(url)
        except Exception as e:
            log.error("Could not fetch %s: %s" % (url, e))

    def run(self):
        while True:
            m = sqs_utils.fetch_message(self.q)
            self.reset_timer()
            if m is None:
                self.increase_wait()
            else:
                self.decrease_wait()
            self.process(m)
            self.wait()

def start(queue_name, max_wait, min_wait):
    p = Processor(queue_name, max_wait, min_wait, log)
    p.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a queue")
    parser.add_argument("queue", help="Name of the queue to process. This should correspond to a named feed from the feeds.txt file. ")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=5000, type=int, help="The maximum number of milliseconds to wait between polls to SQS, defaults to 5000 (5 sec)")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=500, type=int, help="The minimum number of milliseconds to wait between polls to SQS, defaults to 500 (0.5 sec)")
    args = parser.parse_args()
    # Lower the logging verbosity, since this is probably run as a daemon
    log.level = logging.INFO
    start(args.queue, args.max_wait, args.min_wait)
