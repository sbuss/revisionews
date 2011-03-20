from tracker import __version__

FEED_FILE = "feeds.txt"

def get_feeds():
    f = open(FEED_FILE, 'r')
    feeds = {}
    for line in f:
        (name, domain, url) = line.strip().split("\t")
        feeds[name] = Feed(name, domain, url)
    return feeds

class Feed(object):
    def __init__(self, name, domain, url):
        self._name = name
        self._domain = domain
        self._url = url
        self._num_results = 50

    def get_name(self):
        return self._name

    def set_name(self, name):
        self._name = name

    def get_domain(self):
        return self._domain

    def set_domain(self, domain):
        self._domain = domain

    def get_url(self):
        return self._url + "?client=revisionews.com/tracker/v%s&n=%s" % \
                (__version__, self.num_results)

    def set_url(self, url):
        self._url = url

    def get_num_results(self):
        return self._num_results

    def set_num_results(self, num_results):
        self._num_results = 50

    name = property(get_name, set_name)
    domain = property(get_domain, set_domain)
    url = property(get_url, set_url)
    num_results = property(get_num_results, set_num_results)

