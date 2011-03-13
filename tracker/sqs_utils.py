""" Common utilities for Amazon SQS"""
import re

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
from boto.sqs.queue import Queue
from boto.sqs import regions

# Queues
def get_queue(label):
    """Get a :class:`~boto.sqs.queue.Queue` by label.
    
    Get a :class:`~boto.sqs.queue.Queue` with the given label, creating it 
    if it doesn't already exist. This assumes we are in the US-West region.
    
    :type label: String
    :param label: The label of the :class:`~boto.sqs.queue.Queue` to fetch.

    :rtype: :class:`boto.sqs.queue.Queue`
    :return: The :class:`boto.sqs.queue.Queue` object requested.
    """
    conn = SQSConnection(region=regions()[2]) # us-west region
    qname = ("revisionews_%s" % re.sub(r"\W","",label))[:80]
    return conn.create_queue(qname)

def delete_queue(queue):
    """Delete a :class:`~boto.sqs.queue.Queue`.
    
    You probably don't want to do this. This will delete the queue on SQS, 
    not the local :class:`~boto.sqs.queue.Queue` object.
    
    :type queue: boto.sqs.queue.Queue
    :param queue: The :class:`~boto.sqs.queue.Queue` to delete.
    """
    return queue.delete()

# Messages
def build_message(body):
    """Create a :class:`~boto.sqs.message.Message` with the given body text.
    
    :param body: Any byte sequence to place in the message body.

    :rtype: :class:`boto.sqs.message.Message`
    :return: The :class:`boto.sqs.message.Message` object.
    """
    m = Message()
    m.set_body(body)
    return m

def add_to_queue(queue, message):
    """Add the :class:`~boto.sqs.message.Message` to the :class:`~boto.sqs.queue.Queue`.
    
    :type queue: boto.sqs.queue.Queue
    :param queue: The :class:`~boto.sqs.queue.Queue` to add a message to.
    :type message: boto.sqs.message.Message
    :param message: The :class:`~boto.sqs.message.Message` to add.

    :rtype: bool
    :return: True if successful    
    """
    status = queue.write(message)
    if not isinstance(status, Message):
        raise Exception("Status was not a Message, your message was probably not written to the Queue. Details:\n  Queue Name = %s\n  Your message = %s\n  Returned message = %s" 
                        % (queue.name, message, status))
    elif status == False:
        raise Exception("Failed to write message. Q = %s, M = %s"
                        % (queue.name, message))
    # else we can assume it succeeded?
    return true
    
def fetch_message(queue):
    """Fetch a :class:`~boto.sqs.message.Message` from the :class:`~boto.sqs.queue.Queue`.
    
    The default visibility timeout is 60 seconds, which means we have to delete 
    the message within 60 seconds of fetching it or it becomes available for 
    fetching again. This might be too short, depending on the size and 
    complexity of a webpage.
    
    :type queue: boto.sqs.queue.Queue
    :param queue: The :class:`~boto.sqs.queue.Queue` to add a message to.

    :rtype: :class:`boto.sqs.message.Message`
    :return: A :class:`~boto.sqs.message.Message` if successful, or :py:`None` it the queue is empty."""
    return queue.read(60)

def delete_message(queue, message):
    """Delete the :class:`~boto.sqs.message.Message` from the :class:`~boto.sqs.queue.Queue`.
    
    :type queue: boto.sqs.queue.Queue
    :param queue: The :class:`~boto.sqs.queue.Queue` from which to delete a message.
    :type message: boto.sqs.message.Message
    :param message: The :class:`~boto.sqs.message.Message` to delete.

    :rtype: bool
    :return: True if successful, false otherwise
    """
    return queue.delete_message(message)

