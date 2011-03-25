"""Revisionews tracker watches RSS feeds, processes articles, and schedules re-visits.

The Revisionews tracker does four things
=========================================
    1. Watch an RSS feed for items to fetch (feed_reader.py)
    2. Download article data (article_downloader.py)
    3. Process an article (pipeline.py) & store extracts to disk
    4. Schedule a re-visit sometime in the future (scheduler.py)

FeedReader
----------
    FeedReaderDaemon is given a list of RSS feeds from which to find content. 
    The daemon periodically (5 minutes by default) checks the RSS feeds for new 
    items and, if they exist, add the URLs in the feeds to a domain-specific 
    SQS queue. The queue is exclusive to a given domain so that throttling is 
    easy to do.

ArticleDownloader
-----------------
    The ArticleDownloaderDaemon is given a domain to be responsible for. It 
    monitors the SQS queue for that domain and downloads article content as soon 
    as a URL is available in the queue. Once the article is downloaded the 
    content is written to the article processing pipeline queue.

Pipeline
--------
    PipelineDaemon watches the article processing pipeline (see, these names all 
    make sense) for content to process. Text extraction is separate from article 
    downloading because many extractors will be run against the same text and we 
    don't have to worry about throttling the processing.

Scheduler
---------
    When an item is processed, it should schedule a time in the future to be 
    reprocessed.

Utilities
---------
There are also two handy scripts
./start_all.sh
  Starts all:
    * RSS reader daemons (feed_reader.FeedReaderDaemon)
    * Artcile downloader daemons (article_downloader.ArticleDownloaderDaemon)
    * Article processor pipelines (pipeline.PipelineDaemon)
./stop_all.sh
  Stops all above

"""
__version__ = "0.01"

