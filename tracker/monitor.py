import httplib2
import urlparse

URL = "https://www.google.com/reader/atom/user%2F16528950307354065263/state/com.google/reading-list?n=100&r=o&ot=%s"

URL % "1289839514"

def fetch_feed():
    """This runs every five minutes via a cron job"""
    pass
