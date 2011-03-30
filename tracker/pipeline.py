"""Handle reading and writing to the article processing pipeline"""
import argparse
import base64
import datetime
import logging
import logging.config
import json
from StringIO import StringIO
import sys
import time
import urllib
import urlparse

import gzip

from boto import connect_sdb, connect_s3
from boto.s3.bucket import Bucket
from boto.s3.key import Key
from boto.exception import S3ResponseError

from tracker import sqs_utils
from tracker.daemons import SQSDaemon
from tracker import store

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("pipeline")
log.level = logging.DEBUG

QUEUE_NAME = "pipeline"

class PipelineDaemon(SQSDaemon):
    def __init__(self, queue_name=QUEUE_NAME, log=log, *args, **kwargs):
        self.pipeline = Pipeline(queue_name, log)
        super(PipelineDaemon, self).__init__(queue_name=queue_name, 
            pidfile="/tmp/pipeline.pid", log=log, *args, **kwargs)

    def process(self, message):
        article = Article.from_snapshot(message.get_body())
        article.log = self.log
        return self.pipeline.process(article)

class Pipeline(object):
    def __init__(self, queue_name=QUEUE_NAME, log=log):
        self.queue = sqs_utils.get_queue(queue_name)
        self.log = log

    def read(self):
        """Read from the pipeline
        
        :rtype: :class:`~tracker.pipeline.Message`
        :return: The :class:`~tracker.pipeline.Message` fetched, or None if the queue is empty.
        """
        m = sqs_utils.fetch_message(self.queue)
        return m
        
    def write(self, article):
        """Write a :class:`~tracker.pipeline.Article` to the pipeline

        :type message: :class:`~tracker.pipeline.Article`
        :param message: The :class:`~tracker.pipeline.Article` to write to the queue.
        """
        m = sqs_utils.build_message(article.prepare())
        try:
            return sqs_utils.add_to_queue(self.queue, m)
        except Exception as e:
            log.error("Could not add url %s to queue %s: %s" % 
                    (article.url, self.queue.name, e))
            return False

    def remove(self, article):
        sqs_utils.delete_message(self.queue, article)
    
    def process(self, article):
        # check SimpleDB to see if this url has been stored before
        snapshots = article.get_all_snapshots()
        if len(snapshots) > 1:
            self.log.info("Article at <%s> has been seen before" % article.url)
            # Article has been seen before

            # compare checksums of content, 
            # if different 
                # store a snapshot
                # create new entry 
                # post an alert
                # schedule a revisit in the near future (reset revisit counter)
            # else
                # increment revisit counter and schedule
        else:
            self.log.info("Article at <%s> has not been seen before." % article.url)
            article.schedule()
            # need to also process the article
            return True
    

class Article(object):
    """Message stored in the article processing pipeline."""
    def __init__(self, url=None, body=None, fetch_date=None, log=log, zbody=None):
        """
        :type url: :py:`String`
        :param url: URL of the article being processed
        :type body: :py:`String`
        :param body: Contents of the article being processed
        :type zbody: `StringIO`
        :param zbody: Contents of the article, gzipped. Can be file or file-like 
            string. If zbody is set, the body parameter will be ignored.
        :type fetch_date: :py:class:`datetime.datetime`
        :param fetch_date: Date the article was fetched
        :type log: `logging.log`
        """
        self.log = log
        self.date_format = u"%a, %d %b %Y %H:%M:%S %Z"
        self.url = urllib.unquote_plus(url)
        if zbody is not None:
            z = gzip.GzipFile(fileobj=zbody, mode='rb')
            self.body = z.read()
        else:
            self.body = body
        if isinstance(fetch_date, datetime.datetime):
            self.fetch_date = fetch_date
        elif fetch_date is not None:
            self.fetch_date = datetime.datetime.fromtimestamp(float(fetch_date))
        self.sdbconn = connect_sdb()
        self.s3conn = connect_s3()

    def url_key(self):
        return urllib.quote_plus(self.url)

    @classmethod
    def from_json(cls, jstr):
        """
        expects jstr['fetch_date'] to be a unix timestamp
        """
        j = json.loads(jstr)
        return cls(j['url'], j['body'], j['fetch_date'])

    @classmethod
    def from_snapshot(cls, jstr):
        j = json.loads(jstr)
        c = cls(j['url'], j['fetch_date'])
        snapshot = c.sdbconn.select("snapshots",  
                    "select * from snapshots where \
                    url='%s' and fetch_date is not null order by fetch_date \
                    desc limit 1" % c.url_key())
        bucket = snapshot[0]['bucket']
        key = snapshot[0]['key']
        c.body = gzip.GzipFile(mode='rb', fileobj=StringIO(c.s3conn.get_bucket(bucket).get_key(key).get_contents_as_string())).read()
        return c
        
    def __unicode__(self):
        s = json.dumps({'url':self.url, 
                       'fetch_date':self._unix_timestamp(self.fetch_date)})
        return unicode(s);
    
    def __str__(self):
        return self.__unicode__().encode("utf-8")

    def _unix_timestamp(self, ts):
        return int(time.mktime(ts.timetuple()))

    def prepare(self):
        """Return a :class:`~boto.sqs.article.Message`-compatible representation 
        of this object"""
        return self.__unicode__()

    def add_to_queue(self):
        """Store a snapshot and add this article to the pipeline"""
        self.create_snapshot()
        pipe = Pipeline(QUEUE_NAME)
        return pipe.write(self)

    def get_key(self):
        return "%s/%s" % (self.url_key(),
                          self._unix_timestamp(self.fetch_date))

    def get_all_snapshots(self):
        return self.sdbconn.select("snapshots",  "select * from snapshots where \
                    url='%s' and fetch_date is not null order by fetch_date \
                    desc" % self.url_key())

    def create_snapshot(self):
        """Store a snapshot of the article in s3 and insert a record in simpledb

        :return: True if successful, false otherwise"""
        log.info("Creating snapshot for <%s> at %s" % \
                (self.url, self.fetch_date))
        bucket_name = 'revisionews_articles_%s' % \
                urlparse.urlsplit(self.url).netloc
        bucket = Bucket(self.s3conn, bucket_name)
        k = Key(bucket)
        k.key = self.get_key()
        headers={"Content-Type":"text/html", "Content-Encoding":"gzip"}
        zbuf = StringIO()
        zfile = gzip.GzipFile(mode = 'wb',  fileobj = zbuf, compresslevel = 6)
        zfile.write(self.body)
        zfile.close()
        try:
            k.set_contents_from_file(zbuf, headers=headers)
        except S3ResponseError:
            # the bucket probably doesn't exist
            self.s3conn.create_bucket(bucket_name)
            k.set_contents_from_file(zbuf, headers=headers)

        stored = self.sdbconn.put_attributes("snapshots", 
            self.get_key(),
            {   "url":self.url_key(), 
                "domain":urlparse.urlsplit(self.url).netloc,
                "fetch_date":self._unix_timestamp(self.fetch_date),
                "bucket":bucket_name,
                "key":k.key})

        if not stored:
            bucket.delete_key(k)
            log.error("Failed to store snapshot for <%s> at %s" % \
                (self.url, self.fetch_date))

        return stored

    def get_last_schedule(self):
        item_name = self.url_key()
        last_schedule = None
        try:
            last_schedule = self.sdbconn.select("schedule",
                "select * from schedule where itemName()='%s' and next_access \
                is not null order by next_access desc" % item_name)
        except Exception as e:
            log.error("Couldn't get schedule for <%s>: %s" % (self.url, e))

        if last_schedule is None or len(last_schedule) == 0:
            return None
        else:
            return (datetime.datetime.fromtimestamp(
                        int(last_schedule[0]['last_access'])),
                    datetime.datetime.fromtimestamp(
                        int(last_schedule[0]['next_access'])))

    def schedule(self, reset=False):
        """Schedule a revisit of the article.
        
        This uses a quadratic backoff schedule starting at 1 hour then doubling. 
        eg:
            now
            +1 hour
            +2 hours
            +4 hours
            +8 hours
            +16 hours
            +32 hours
            ...
        This assumes that most unannounced changes to the article happen soon
        after the article goes live. 1 hour might be too large of a window;
        something like 5 minutes might be better.

        :type reset: :py:boolean
        :param reset: Set to True if the revisit schedule should be reset, 
            this would usually mean the article contents have changed and the 
            article should be checked again sooner
        :return: True if successful, false otherwise
        """
        item_name = self.url_key()
        last_schedule = self.get_last_schedule()
        if last_schedule is None:
            # For items which haven't been seen before
            next_access = self.fetch_date + datetime.timedelta(hours=1)
            log.info("Scheduling <%s> for update at %s" % \
                        (self.url, next_access))
            return self.sdbconn.put_attributes("schedule",
                item_name,
                {"last_access":self._unix_timestamp(self.fetch_date),
                 "next_access":self._unix_timestamp(next_access)})
        else:
            (last_access, next_access) = last_schedule
            if reset:
                dt = 0.5
            else:
                dt = last_schedule[1] - last_schedule[0]
            next_access = self.fetch_date + dt * 2
            log.info("Scheduling <%s> for update at %s" % \
                        (self.url, next_access))
            return self.sdbconn.put_attributes("schedule",
                item_name,
                {"last_access":self._unix_timestamp(self.fetch_date),
                 "next_access":self._unix_timestamp(next_access)})

        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a the pipeline queue.",
                                     parents=[SQSDaemon.parser],
                                     conflict_handler="resolve")
    # Remove queue argument
    parser.add_argument("-queue", help="ignored")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=10000, type=int, help="The maximum number of milliseconds to wait between runs, defaults to 10000 (10 sec)")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=1000, type=int, help="The minimum number of milliseconds to wait between runs, defaults to 1000 (1 sec)")
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


