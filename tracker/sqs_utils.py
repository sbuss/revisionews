import re

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
from boto.sqs import regions

# Queues
def get_queue(label):
    conn = SQSConnection(region=regions()[2]) # us-west region
    qname = ("revisionews_%s" % re.sub(r"\W","",label))[:80]
    return conn.create_queue(qname)

def delete_queue(queue_name):
    q = get_queue(queue_name)
    return q.delete()

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
    
def fetch_message(queue_name):
    try:
        q = get_queue(queue_name)
        message = q.get_messages(1)[0]
        return message
    except:
        return None

def delete_message(queue_name, message):
    q = get_queue(queue_name)
    return q.delete_message(message)

