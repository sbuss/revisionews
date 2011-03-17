"""Handle reading and writing to the article processing pipeline"""
import json
import sqs_utils

class Pipeline(object):
    QUEUE_NAME = "revisionews.pipeline"
    def __init__(self):
        self.queue = sqs_utils.get_queue(QUEUE_NAME)

    def read(self):
        """Read from the pipeline
        
        :rtype: :class:`Message`
        :return: The :class:`Message` fetched, or None if the queue is empty.
        """
        m = sqs_utils.fetch_message(self.queue)
        return m

    def write(self, message):
        """Write a :class:`Message` to the queue

        :type message: :class:`Message`
        :param message: The :class:`Message` to write to the queue.
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
        self.set_body(body)
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
        return self._get('body')

    def set_body(self, body):
        self._set('body', body)

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
        
