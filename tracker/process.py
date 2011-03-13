from __future__ import division

import argparse
import datetime
import logging
import logging.config
import time

import httplib2

from tracker import sqs_utils
from tracker import store

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("process")
log.level = logging.DEBUG

class Processor(object):
    """Daemon that periodically checks an SQS queue for URLs to process.

    Usage
    =====
    $> python process.py --max 5000 --min 500 "BBC News"
    """
    def __init__(self, queue_name, max_wait, min_wait, log=None):
        """Initializes the Processor daemon.
        .. function: __init__(queue_name, max_wait, min_wait[, log])

        :param queue_name: The name of the queue, corresponding to an entry in feeds.txt
        :param max_wait: The maximum amount of time to wait (in milliseconds) 
        between SQS polls.
        :param min_wait: The minimum amount of time to wait (in milliseconds)
        between SQS polls.
        :param log: The logging object.
        """
        self.log = log
        self.q = sqs_utils.get_queue(queue_name)
        self.log.debug("Queue is %s" % self.q)
        self.max_wait = max_wait
        self.min_wait = min_wait
        self.current_wait = min_wait
        self.h = httplib2.Http()

    def reset_timer(self):
        self.last_access = datetime.datetime.now()

    def wait(self):
        """Wait the appropriate amount of time between SQS polls."""
        d = datetime.datetime.now() - self.last_access
        dt = (d.microseconds / 1000) + (d.seconds * 1000)
        self.log.debug("dt is %s" % dt)
        if dt < 0:
            # uh, just in case
            dt = dt * -1;

        if dt > self.current_wait:
            self.log.debug("not sleeping")
            return
        else:
            time.sleep((self.current_wait - dt)/1000)

    def decrease_wait(self):
        self.current_wait = self.min_wait

    def increase_wait(self):
        self.current_wait = min(self.current_wait * 2, self.max_wait)

    def process(self, message):
        """Process a :class:`~boto.sqs.message.Message` received from SQS.
        
        :type message: :class:`boto.sqs.message.Message`
        :param message: The :class:`~boto.sqs.message.Message` to process. 
        Processing means download the article, strip out the article text, 
        check if we have a copy. If we don't, store & checksum it, if we do then
        compare the stored version to the fetched version. If stored != fetched 
        then store a new copy and send an alert.
        """
        if message is None:
            return
        url = message.get_body()
        try:
            headers = {}
            # Set a fake referer to bypass the NYT paywall
            # This will probably have to be a randomly generated string in the 
            # future
            headers = {"Referer": "http://google.com/search?q=lol"}

            # because the link in SQS was the feed url, fetch the page so we
            # can follow redirects
            self.log.debug("url is %s" % url)
            (response, content) = self.h.request(url, headers=headers)
            self.log.debug("Response from server was %s" % response)
            # find the actual content

            # check SimpleDB to see if this url has been stored before

            # compare checksums of content, if different create new entry and 
            # post an alert

            # store a compressed snapshot in S3
            print(url)
        except Exception as e:
            self.log.error("Could not fetch %s: %s" % (url, e))

    def run(self):
        """Run the processor daemon."""
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
    log.info("Starting process.py with queue %s" % queue_name)
    p = Processor(queue_name, max_wait, min_wait, log)
    p.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a queue")
    parser.add_argument("queue", help="Name of the queue to process. This should correspond to a named feed from the feeds.txt file. ")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=5000, type=int, help="The maximum number of milliseconds to wait between polls to SQS, defaults to 5000 (5 sec)")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=500, type=int, help="The minimum number of milliseconds to wait between polls to SQS, defaults to 500 (0.5 sec)")
    parser.add_argument("-log", action="store", dest="log_level",
                        default="INFO", help="Log level (INFO or DEBUG)")
    args = parser.parse_args()
    # Lower the logging verbosity, since this is probably run as a daemon
    log.level = logging.INFO
    if args.log_level == "DEBUG":
        log.level = logging.DEBUG
    start(args.queue, args.max_wait, args.min_wait)
