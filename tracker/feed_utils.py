from tracker import __version__

FEED_FILE = "feeds.txt"

def get_feeds():
    f = open(FEED_FILE, 'r')
    feeds = []
    for line in f:
        (title, domain, url) = line.strip().split("\t")
        feeds.append({"domain":domain,"title":title, "url":url})
    return feeds

def format_feed_url(url) :
    return url + "?client=revisionews.com/tracker/v%s&n=50" % __version__
