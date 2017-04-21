#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('..')
from whoosh.index import open_dir
from whoosh.query import *

search_word = ''
if len(sys.argv) > 1:
    search_word = sys.argv[1]
else:
    print "Use ../tools2/test_search.py topic"
    exit()

print "Opening index"
ix = open_dir("index_dir")

print "Searching %s" % search_word

from whoosh.qparser import QueryParser
with ix.searcher() as searcher:
    query = QueryParser("title", ix.schema).parse(unicode(search_word))
    results = searcher.search(query, limit=None)
    print "Found %d articles" % results.scored_length()
    print dir(results)
    for n in range(results.scored_length()):
        print results[n]
