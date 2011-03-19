from __future__ import division

import argparse
import datetime
import time

class PeriodicDaemon(object):
    parser = argparse.ArgumentParser(add_help=False, # argparse issue
                                     description="Monitor an SQS queue.")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=5000, type=int, help="The maximum number of milliseconds to wait between runs, defaults to 5000 (5 sec)")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=500, type=int, help="The minimum number of milliseconds to wait between runs, defaults to 500 (0.5 sec)")
    parser.add_argument("-log", action="store", dest="log_level",
                        default="INFO", help="Log level (INFO or DEBUG)")

    def __init__(self, max_wait, min_wait, log=None):
        """Initializes a PeriodicDaemon.
        .. function: __init__(, max_wait, min_wait[, log])

        :param max_wait: The maximum amount of time to wait (in milliseconds) 
        between SQS polls.
        :param min_wait: The minimum amount of time to wait (in milliseconds)
        between SQS polls.
        :param log: The logging object."""
        self.log = log
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
        """Reset the wait time to min_wait."""
        self.current_wait = self.min_wait

    def increase_wait(self):
        """Double the wait time, capped at max_wait."""
        self.current_wait = min(self.current_wait * 2, self.max_wait)

    def run(self):
        """Basic structure of a periodic daemon loop.

        If you want the least amount of effort, just implement a do_work method
        and the Daemon will periodically run it for you, with exponential 
        backoff and fast starting."""
        while True:
            success = self.do_work()
            self.reset_timer() # Update timer with time of last run
            if success:
                # do_work did work, so there is probably more to do and
                # we should reset the wait time to min_wait
                self.decrease_wait()
            else: 
                # No work for the daemon, so back off
                self.increase_wait()
            self.wait()

    def do_work(self):
        """Must be overridden if you want to use the default run method.
        
        :rtype: boolean
        :return: True if the daemon did work, and there is probably more work to
                 do, False if no work was done and we want to increase the 
                 amount of time to wait between running again.
        """
        pass
        
    
class SQSDaemon(PeriodicDaemon):
    parser = argparse.ArgumentParser(add_help=False, # argparse issue
                                     parents=[PeriodicDaemon.parser],
                                     description="Monitor an SQS queue.")
    parser.add_argument("-queue", help="Name of the queue to process. This should correspond to a named feed from the feeds.txt file. ")
    """Daemon that periodically checks an SQS queue.
    """
    def __init__(self, queue_name, *args, **kwargs):
        """Initializes the Processor daemon.
        .. function: __init__(queue_name, *args, **kwargs)

        :param queue_name: The name of the queue, corresponding to an entry in feeds.txt
        """
        super(self, SQSDaemon).__init__(*args, **kwargs)
        self.q = sqs_utils.get_queue(queue_name)
        self.log.debug("Queue is %s" % self.q)

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

