#!/bin/bash
python feed_reader.py -log INFO start
IFS=$'\n'
feeds=$(cut -f2 feeds.txt)
for f in $feeds; do
    python article_downloader.py -log DEBUG -queue \"$f\" start
done
