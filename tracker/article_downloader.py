import argparse
import datetime
import httplib2
import logging
import logging.config
import sys
import time

from tracker import sqs_utils
from tracker.daemons import SQSDaemon
from tracker.pipeline import Pipeline, Article

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("articleDownloader")
log.level = logging.DEBUG

class ArticleDownloaderDaemon(SQSDaemon):
    """Daemon that periodically checks an SQS queue for URLs to process.

    Usage
    =====
    $> python process.py --max 5000 --min 500 "BBC News"
    """
    def __init__(self, queue_name, *args, **kwargs):
        self.h = httplib2.Http()
        super(ArticleDownloaderDaemon, self).__init__(
            queue_name=queue_name,
            pidfile="/tmp/article_downloader_%s" % queue_name,
            *args, **kwargs)

    def process(self, message):
        """Process a :class:`~boto.sqs.message.Message` received from SQS.
        
        :type message: :class:`boto.sqs.message.Message`
        :param message: The :class:`~boto.sqs.message.Message` to process. 
        Processing means download the article, strip out the article text, 
        check if we have a copy. If we don't, store & checksum it, if we do then
        compare the stored version to the fetched version. If stored != fetched 
        then store a new copy and send an alert.
        """
        self.log.debug("Processing for %s" % self.q.name)
        if message is None:
            return
        url = message.get_body()
        response = None
        content = None
        try:
            headers = {}
            # Set a fake referer and user agent to bypass the NYT paywall
            # This will probably have to be a randomly generated string in the 
            # future
            headers = {"Referer": "http://google.com/search?q=lol", 
                       "User-Agent":"Mozilla/5.0 (X11; U; Linux i686; en-US) \
                                     AppleWebKit/534.16 (KHTML, like Gecko) \
                                     Chrome/10.0.648.127 Safari/534.16"}

            # because the link in SQS was the feed url, fetch the page so we
            # can follow redirects
            self.log.info("Fetching url: %s" % url)
            (response, content) = self.h.request(url, headers=headers)
            self.log.debug("Response from server was %s" % response)
        except Exception as e:
            self.log.error("Could not fetch %s: %s" % (url, e))

        if response is not None and content is not None:
            try:
                # Write the contents of the page, zipped, to the processing pipeline
                pipeline = Pipeline()
                fetch_date = response['date']
                fetch_date = datetime.datetime.strptime(fetch_date,
                    u'%a, %d %b %Y %H:%M:%S %Z')
                fetch_date = time.mktime(fetch_date.timetuple())
                article = Article(response['content-location'], 
                                  content, 
                                  fetch_date)
                return article.add_to_queue()
            except Exception as e:
                self.log.error("Couldn't add the article at <%s> to the pipeline: %s" % (url, e))
        else:
            return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a queue",
                                     parents=[SQSDaemon.parser], 
                                     conflict_handler="resolve")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=10000, type=int, help="The maximum number of milliseconds to wait between runs, defaults to 10000 (10 sec)")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=2000, type=int, help="The minimum number of milliseconds to wait between runs, defaults to 2000 (2 sec)")
    args = parser.parse_args()
    # Lower the logging verbosity, since this is probably run as a daemon
    log.level = logging.INFO
    if args.log_level == "DEBUG":
        log.level = logging.DEBUG
    add = ArticleDownloaderDaemon(queue_name = args.queue, 
                                  max_wait = args.max_wait, 
                                  min_wait = args.min_wait, 
                                  log = log)
    if args.command == "start":
        log.info("Starting article downloader daemon for %s" % \
            args.queue)
        add.start()
        sys.exit(0)
    elif args.command == "stop":
        log.info("Stopping article downloader daemon for %s" % \
            args.queue)
        add.stop()
        sys.exit(0)
    elif args.command == "restart":
        log.info("Restarting article downloader daemon for %s" % \
            args.queue)
        add.restart()
        sys.exit(0)
    else:
        print("Invalid command. Expected 'start', 'stop', or 'restart'.")
        parser.print_usage()
        sys.exit(2)

