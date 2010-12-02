import re

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
from boto.sqs.queue import Queue
from boto.sqs import regions

# Queues
def get_queue(label):
    conn = SQSConnection(region=regions()[2]) # us-west region
    qname = ("revisionews_%s" % re.sub(r"\W","",label))[:80]
    return conn.create_queue(qname)

def delete_queue(queue):
    return queue.delete()

# Messages
def build_message(body):
    m = Message()
    m.set_body(body)
    return m

def add_to_queue(queue, message):
    status = queue.write(message)
    if not isinstance(status, Message):
        raise Exception("Status was not a Message. Q = %s, M = %s" 
                        % (queue.name, message))
    elif status == False:
        raise Exception("Failed to write message. Q = %s, M = %s"
                        % (queue.name, message))
    
def fetch_message(queue):
    return queue.read(60)

def delete_message(queue, message):
    return queue.delete_message(message)

