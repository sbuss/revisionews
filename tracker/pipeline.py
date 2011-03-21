"""Handle reading and writing to the article processing pipeline"""
import argparse
import logging
import logging.config
import json
import sys
import zlib

from tracker import sqs_utils
from tracker.daemons import SQSDaemon

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("process")
log.level = logging.DEBUG

class PipelineDaemon(SQSDaemon):
    def __init__(self, *args, **kwargs):
        super(Pipeline, self).__init__(queue_name="revisionews_pipeline", 
            pidfile="/tmp/pipeline.pid", *args, **kwargs)

class Pipeline(object):
    def __init__(self, queue_name="revisionews_pipeline"):
        self.queue = sqs_utils.get_queue(queue_name)
        
    def read(self):
        """Read from the pipeline
        
        :rtype: :class:`~tracker.pipeline.Message`
        :return: The :class:`~tracker.pipeline.Message` fetched, or None if the queue is empty.
        """
        m = sqs_utils.fetch_message(self.queue)
        return m

    def write(self, message):
        """Write a :class:`~tracker.pipeline.Message` to the pipeline

        :type message: :class:`~tracker.pipeline.Message`
        :param message: The :class:`~tracker.pipeline.Message` to write to the queue.
        """
        m = sqs_utils.build_message(message.prepare())
        try:
            sqs_utils.add_to_queue(self.queue, m)
        except Exception as e:
            log.error("Could not add url %s to queue %s: %s" % 
                    (url, self.queue.name, e))
    
    def process(self):
        # check SimpleDB to see if this url has been stored before

        # compare checksums of content, if different create new entry and 
        # post an alert

        # store a compressed snapshot in S3
        pass


class Message(object):
    """Message stored in the article processing pipeline."""
    def __init__(self, url=None, body=None, fetch_date=None):
        self._wire = {"url":"", "body":""}
        self.set_url(url)
        self.set_body(zlib.compress(body))
        self.set_fetch_date(fetch_date)

    @classmethod
    def from_json(cls, jstr):
        j = json.loads(jstr)
        return cls(j['url'], j['body'], j['fetch_date'])

    def _get(self, field):
        return self._wire[field]

    def _set(self, field, value):
        if value is not None:
            self._wire[field] = value
        else:
            self._wire[field] = ""
    
    def _del(self, field):
        self._wire[field] = ""

    def get_url(self):
        return self._get('url')

    def set_url(self, url):
        self._set('url', url)

    def del_url(self):
        self._del['url'] = ""

    def get_body(self):
        return zlib.decompress(self._get('body'))

    def set_body(self, body):
        self._set('body', zlib.compress(body))

    def del_body(self):
        self._del('body')

    def get_fetch_date(self):
        return self._get('fetch_date')

    def set_fetch_date(self, fetch_date):
        self._set('fetch_date', fetch_date)

    def del_fetch_date(self):
        self._del('fetch_date')

    url = property(get_url, set_url, del_url, 
                    "URL of the article being processed")
    body = property(get_body, set_body, del_body,
                    "Contents of the article being processed")
    fetch_date = property(get_fetch_date, set_fetch_date, del_fetch_date,
                    "Date the article was fetched")

    def __unicode__(self):
        return json.dumps(self._wire)
    
    def __str__(self):
        return self.__unicode__().encode("utf-8")

    def prepare(self):
        """Return a :class:`~boto.sqs.message.Message`-compatible representation 
        of this object"""
        return "%s" % self.__unicode__()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a the pipeline queue.",
                                     parents=[SQSDaemon.parser],
                                     conflict_handler="resolve")
    # Remove queue argument
    parser.add_argument("-queue", help="ignored")
    args = parser.parse_args()
    # Lower the logging verbosity, since this is probably run as a daemon
    log.level = logging.INFO
    if args.log_level == "DEBUG":
        log.level = logging.DEBUG
    pd = PipelineDaemon(max_wait = args.max_wait, 
                         min_wait = args.min_wait, 
                         log = log)
    if args.command == "start":
        log.info("Starting pipeline daemon")
        pd.start()
        sys.exit(0)
    elif args.command == "stop":
        log.info("Stopping pipeline daemon")
        pd.stop()
        sys.exit(0)
    elif args.command == "restart":
        log.info("Restarting pipeline daemon")
        pd.restart()
        sys.exit(0)
    else:
        print("Invalid command. Expected 'start', 'stop', or 'restart'.")
        parser.print_usage()
        sys.exit(2)


