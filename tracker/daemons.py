#!/usr/bin/env python
from __future__ import division

import sys, os, time, atexit
import argparse
import datetime
import time
from signal import SIGTERM 

from tracker import sqs_utils

class Daemon(object):
    """A generic daemon class.

    Daemon class from http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/

    This is written by Sander Marechal and was obtained from his website at 
    http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/

    It has been slightly modified to work with new-style classes
    
    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        super(Daemon, self).__init__()
    
    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
    
        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 
    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
    
        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
    
    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError as e:
            pid = None
    
        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        
        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process    
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """

class PeriodicDaemon(Daemon):
    parser = argparse.ArgumentParser(add_help=False, # argparse issue
                                     description="Monitor an SQS queue.")
    parser.add_argument("-max", action="store", dest="max_wait", 
                        default=5000, type=int, help="The maximum number of milliseconds to wait between runs, defaults to 5000 (5 sec)")
    parser.add_argument("-min", action="store", dest="min_wait", 
                        default=500, type=int, help="The minimum number of milliseconds to wait between runs, defaults to 500 (0.5 sec)")
    parser.add_argument("-log", action="store", dest="log_level",
                        default="INFO", help="Log level (INFO or DEBUG)")
    parser.add_argument("command", help="Command for the daemon: start, stop, or restart")

    def __init__(self, max_wait, min_wait, log=None, 
                  *args, **kwargs):
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
        super(PeriodicDaemon, self).__init__(*args, **kwargs)

    def reset_timer(self):
        self.last_access = datetime.datetime.now()

    def wait(self):
        """Wait the appropriate amount of time between SQS polls."""
        d = datetime.datetime.now() - self.last_access
        dt = (d.microseconds / 1000) + (d.seconds * 1000)
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
        self.log.info("Entering run loop")
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
    def __init__(self, queue_name, pidfile="/tmp/sqs_daemon.pid", 
                 *args, **kwargs):
        """Initializes the Processor daemon.
        .. function: __init__(queue_name, *args, **kwargs)

        :param queue_name: The name of the queue, corresponding to an entry in feeds.txt
        """
        super(SQSDaemon, self).__init__(pidfile=pidfile, 
                                        *args, **kwargs)
        self.q = sqs_utils.get_queue(queue_name)
        self.log.debug("Queue is %s" % self.q.name)

    def run(self):
        """Run the processor daemon."""
        while True:
            m = sqs_utils.fetch_message(self.q)
            self.log.debug("Fetched message for %s" % self.q.name)
            self.log.debug("Message is %s" % m)
            self.reset_timer()
            if m is None:
                self.increase_wait()
            else:
                self.log.info("Found a message for %s: %s" % (self.q.name, m.get_body()))
                self.decrease_wait()
                if(self.process(m)):
                    # Success, so delete the message
                    sqs_utils.delete_message(self.q, m)
            self.wait()
    
    def process(self):
        """Method must be overridden with code to process message fetched."""
        pass

