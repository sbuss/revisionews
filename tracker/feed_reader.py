"""Read the article feeds and add the urls to SQS for processing."""

import argparse
from datetime import datetime
import logging
import logging.config
import operator

import feedparser

from tracker.daemons import PeriodicDaemon
from tracker import feed_utils
from tracker import sqs_utils

# File to store the timestamp of the last feed read
LAST_ACCESS = ".last-access.%s"

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("feedReader")
log.level = logging.DEBUG

class FeedReaderDaemon(PeriodicDaemon):
    """Daemon for reading RSS feeds."""
    def __init__(self, feed, *args, **kwargs):
        """Initialize the FeedReaderDaemon.

        :type feed: :class:`tracker.feed_utils.Feed`
        :param feed: The :class:`~tracker.feed_utils.Feed` to monitor."""
        self.feed = feed
        self.queue = sqs_utils.get_queue(feed.domain)
        self.dateformat = "%Y-%m-%dT%H:%M:%SZ"
        super(FeedReaderDaemon, self).__init__(*args, **kwargs)

    def do_work(self):
        new_items = 0
        # Get the feed
        f = feedparser.parse(self.feed.url)

        # Sort the list of items in descending order of updated
        sorted_entries = sorted(f.entries, 
                                key=operator.itemgetter('updated_parsed'),
                                reverse=True)

        self.log.debug("Last access time for %s is %s" % \
                        (self.feed.domain, self.get_last_access()))
        # Add new items to the queue
        for entry in sorted_entries:
            if self.add_to_queue(entry):
                new_items += 1
            else:
                # We can assume we've seen these entries before, so quit
                break

        if new_items == 0:
            self.log.info("No work to do for %s" % feed.domain)
        else:
            self.log.info("Added %s new items to %s" % (new_items, feed.domain))
        self.log.info("Setting last access for %s as %s" % \
                        (feed.domain, sorted_entries[0].updated))
        self.update_last_access(sorted_entries[0].updated)

    def add_to_queue(self, entry):
        last_access = self.get_last_access()
        if datetime.strptime(entry.updated, self.dateformat) > last_access:
            """
            m = sqs_utils.build_message(entry.link)
            try:
                sqs_utils.add_to_queue(self.queue, m)
            except Exception as e:
                log.error("Could not add url %s to queue %s: %s" % \
                        (self.link, self.queue.name, e))
                """
            # NOTE: entry.link is the URL given to the feed, not the item's
            # actual URL
            self.log.debug("Adding %s" % entry.link)
            return True
        else:
            return False
    
    def get_last_access(self):
        # Get the last access time
        f = open(LAST_ACCESS % self.feed.domain , 'r')
        last_access = datetime.strptime(f.readline().strip(), self.dateformat)
        f.close()
        return last_access

    def update_last_access(self, last_access_time):
        # Update the last access time
        f = open(LAST_ACCESS % self.feed.domain, 'w')
        self.log.debug("Setting last access time as %s" % last_access_time)
        f.write(last_access_time)
        f.close()


def start(feed, max_wait, min_wait):
    log.info("Starting feed reader daemon with feed %s" % feed.domain)
    frd = FeedReaderDaemon(feed = feed, 
                           max_wait = max_wait, 
                           min_wait = min_wait, 
                           log = log)
    frd.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download RSS feeds",
                                     parents=[PeriodicDaemon.parser],
                                     conflict_handler="resolve")
    parser.add_argument("-feed", dest="feed_name",
                        default="all", help="Name of the feed to track, \
                                        corresponding with a feed name in \
                                        feeds.txt.")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=(1000 * 60 * 5), # 5 minutes
                        type=int, help="The maximum number of milliseconds to wait between runs, defaults to 5 minutes")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=(1000 * 60 * 5), type=int, help="The minimum number of milliseconds to wait between runs, defaults to 5 minutes")
    args = parser.parse_args()
    feeds = feed_utils.get_feeds()
    feed = None
    if args.feed_name not in feeds:
        raise Exception("Unknown feed name: %s" % args.feed_name)
    else:
        feed = feeds[args.feed_name]
    # Lower the logging verbosity, since this is probably run as a daemon
    log.level = logging.INFO
    if args.log_level == "DEBUG":
        log.level = logging.DEBUG
    start(feed, args.max_wait, args.min_wait)

