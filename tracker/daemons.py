from __future__ import division

import argparse
import datetime
import time

class SQSDaemon(object):
    parser = argparse.ArgumentParser(add_help=False, # argparse issue
                                     description="Monitor an SQS queue.")
    parser.add_argument("queue", help="Name of the queue to process. This should correspond to a named feed from the feeds.txt file. ")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=5000, type=int, help="The maximum number of milliseconds to wait between polls to SQS, defaults to 5000 (5 sec)")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=500, type=int, help="The minimum number of milliseconds to wait between polls to SQS, defaults to 500 (0.5 sec)")
    parser.add_argument("-log", action="store", dest="log_level",
                        default="INFO", help="Log level (INFO or DEBUG)")
    """Daemon that periodically checks an SQS queue.
    """
    def __init__(self, queue_name, max_wait, min_wait, log=None):
        """Initializes the Processor daemon.
        .. function: __init__(queue_name, max_wait, min_wait[, log])

        :param queue_name: The name of the queue, corresponding to an entry in feeds.txt
        :param max_wait: The maximum amount of time to wait (in milliseconds) 
        between SQS polls.
        :param min_wait: The minimum amount of time to wait (in milliseconds)
        between SQS polls.
        :param log: The logging object.
        """
        self.log = log
        self.q = sqs_utils.get_queue(queue_name)
        self.log.debug("Queue is %s" % self.q)
        self.max_wait = max_wait
        self.min_wait = min_wait
        self.current_wait = min_wait

    def reset_timer(self):
        self.last_access = datetime.datetime.now()

    def wait(self):
        """Wait the appropriate amount of time between SQS polls."""
        d = datetime.datetime.now() - self.last_access
        dt = (d.microseconds / 1000) + (d.seconds * 1000)
        self.log.debug("dt is %s" % dt)
        if dt < 0:
            # uh, just in case
            dt = dt * -1;

        if dt > self.current_wait:
            self.log.debug("not sleeping")
            return
        else:
            time.sleep((self.current_wait - dt)/1000)

    def decrease_wait(self):
        self.current_wait = self.min_wait

    def increase_wait(self):
        self.current_wait = min(self.current_wait * 2, self.max_wait)

    def run(self):
        """Run the processor daemon."""
        while True:
            m = sqs_utils.fetch_message(self.q)
            self.reset_timer()
            if m is None:
                self.increase_wait()
            else:
                self.decrease_wait()
                if(self.process(m)):
                    # Success, so delete the message
                    sqs_utils.delete_message(m)
            self.wait()

    def process(self):
        """Method must be overridden with code to process message fetched."""
        pass

