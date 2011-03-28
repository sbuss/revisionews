#!/bin/bash
python feed_reader.py -log INFO stop
IFS=$'\n'
feeds=$(cut -f2 feeds.txt)
for f in $feeds; do
    python article_downloader.py -log INFO -queue \"$f\" stop
done
python pipeline.py -log INFO stop
