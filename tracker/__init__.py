"""Revisionews tracker watches a feed for new items and schedules re-visits.

The Revisionews tracker does two things:
    1. Watch an RSS feed for items to fetch (monitor.py)
    2. Schedule periodic re-vists of the fetched items (scheduler.py)

Monitor:
    Check on the feed url every few minutes and add the new urls to a queue to 
    be processed (downloaded, parsed, stored).

Scheduler:
    When an item is processed, it should schedule a time in the future to be 
    reprocessed.
"""
__version__ = "0.01"

