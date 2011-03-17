import argparse
import logging
import logging.config

import httplib2

from tracker import sqs_utils
from tracker import store
from tracker.daemons import SQSDaemon

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("process")
log.level = logging.DEBUG

class ArticleDownloaderDaemon(SQSDaemon):
    """Daemon that periodically checks an SQS queue for URLs to process.

    Usage
    =====
    $> python process.py --max 5000 --min 500 "BBC News"
    """
    def __init__(self, *args, **kwargs):
        self.h = httplib2.Http()
        super(Processor, self).__init__(*args, **kwargs)

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
            # Set a fake referer and user agent to bypass the NYT paywall
            # This will probably have to be a randomly generated string in the 
            # future
            headers = {"Referer": "http://google.com/search?q=lol", 
                       "User-Agent":"Mozilla/5.0 (X11; U; Linux i686; en-US) \
                                     AppleWebKit/534.16 (KHTML, like Gecko) \
                                     Chrome/10.0.648.127 Safari/534.16"}

            # because the link in SQS was the feed url, fetch the page so we
            # can follow redirects
            self.log.debug("url is %s" % url)
            (response, content) = self.h.request(url, headers=headers)
            self.log.debug("Response from server was %s" % response)

            # Write the contents of the page, zipped, to the processing pipeline
            pipeline = Pipeline()
            pipeline.write(Message(response['content-location'], 
                                   content, 
                                   response['date']))
            print(url)
            return true
        except Exception as e:
            self.log.error("Could not fetch %s: %s" % (url, e))

def start(queue_name, max_wait, min_wait):
    log.info("Starting article downloader daemon with queue %s" % queue_name)
    add = ArticleDownloaderDaemon(queue_name = queue_name, 
                                  max_wait = max_wait, 
                                  min_wait = min_wait, 
                                  log = log)
    add.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a queue",
                                     parents=[SQSDaemon.parser])
    args = parser.parse_args()
    # Lower the logging verbosity, since this is probably run as a daemon
    log.level = logging.INFO
    if args.log_level == "DEBUG":
        log.level = logging.DEBUG
    start(args.queue, args.max_wait, args.min_wait)

