from datetime import datetime
import feedparser
import logging
import logging.config
import operator
import re
import urlparse

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message

from tracker import __version__
from tracker import feed_utils

# File to store the timestamp of the last feed read
LAST_ACCESS = ".last-access.%s"

# Configure logger
logging.config.fileConfig("logging.conf")
log = logging.getLogger("monitor")
log.level = logging.DEBUG

def run():
    feeds = feed_utils.get_feeds()
    for feed in feeds:
        fetch_feed(feed['domain'], feed['url'])

def fetch_feed(label, url):
    """Download the feed.

    Only return items fetched after our last request five minutes ago. This 
    means we need to keep track of the last access time. The easiest way to do 
    this is in a file, LAST_ACCESS.
    """
    dateformat = "%Y-%m-%dT%H:%M:%SZ"
    new_items = 0
    queue = get_queue(label)

    # Get the feed
    feed = feedparser.parse(feed_utils.format_feed_url(url))

    # Sort the list of items in descending order of updated
    sorted_entries = sorted(feed.entries, 
                            key=operator.itemgetter('updated_parsed'),
                            reverse=True)
    log.debug(sorted_entries[0].updated)
    # Get the last access time
    f = open(LAST_ACCESS % label , 'r')
    last_access = datetime.strptime(f.readline().strip(), dateformat)
    f.close()

    # Add new items to the queue
    for entry in sorted_entries:
        if datetime.strptime(entry.updated, dateformat) > last_access:
            add_to_queue(queue, entry.link)
            new_items += 1
            log.debug("Adding %s" % entry.link)
        else:
            continue
    log.debug("Added %s new items" % new_items)

    # Update the last access time
    f = open(LAST_ACCESS % label, 'w')
    f.write(sorted_entries[0].updated)
    f.close()

def get_queue(label):
    conn = SQSConnection()
    qname = ("revisionews_%s" % re.sub(r"\W","",label))[:80]
    log.debug("queue name is %s" % qname)
    return conn.create_queue(qname)
    
def add_to_queue(queue, url):
    """Add the url to the process queue."""
    m = Message()
    m.set_body(url)
    try:
        status = queue.write(m)
        if not isinstance(status, Message):
            raise Exception("Status was not a Message. URL = %s, Q = %s" 
                            % (url, queue.name))
    except Exception as e:
        log.error("Could not add url %s to queue %s: %s" % (url, queue.name, e))

def fetch_message(queue_name):
    conn = SQSConnection()
    q = conn.create_queue(queue_name)
    message = q.get_messages(1)[0]
    return message

def delete_message(queue_name, message):
    conn = SQSConnection()
    q = conn.create_queue(queue_name)
    return q.delete_message(message)

def delete_queue(queue_name):
    conn = SQSConnection()
    q = conn.create_queue(queue_name)
    return q.delete()

if __name__ == "__main__":
    log.level = logging.INFO
    run()
