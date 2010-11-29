import httplib2
import urlparse
import base64
import zlib

from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key

def fetch(url) :
    h = httplib2.Http()
    (response, content) = h.request(url)
    if (response['status'] != '200') :
        raise Exception("Status was %s for url %s" % (response['status'], url))
    else:
        return content


def download(url):
    content = fetch(url)
    s3conn = S3Connection() # Read AWS_ACCES_KEY_ID and AWS_SECRET_ACCESS_KEY from environ
    bucket = Bucket(s3conn, 'revisionewsdata')
    k = Key(bucket)
    k.key = "%s/%s" % (urlparse.urlsplit(url).netloc, base64.b64encode(url))
    k.set_contents_from_string(zlib.compress(content))
        
def retrieve(url):
    s3conn = S3Connection()
    bucket = Bucket(s3conn, 'revisionewsdata')
    k = Key(bucket)
    k.key = "%s/%s" % (urlparse.urlsplit(url).netloc, base64.b64encode(url))
    return zlib.decompress(k.get_contents_as_string())
    
