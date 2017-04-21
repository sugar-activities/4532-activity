#!/usr/bin/env python
# -*- coding: utf-8 -*-
# create index

import codecs
from subprocess import Popen, PIPE, STDOUT
import re
import logging

def normalize_title(title):
    return title.strip().replace(' ', '_').capitalize()


class RedirectParser:

    def __init__(self, file_name):
        self.link_re = re.compile('\[\[.*?\]\]')
        # Load redirects
        input_redirects = codecs.open('%s.redirects_used' % file_name,
                encoding='utf-8', mode='r')

        self.redirects = {}
        for line in input_redirects.readlines():
            links = self.link_re.findall(unicode(line))
            if len(links) == 2:
                origin = links[0][2:-2]
                destination = links[1][2:-2]
                self.redirects[origin] = destination
            #print "Processing %s" % normalize_title(origin)
        logging.debug("Loaded %d redirects" % len(self.redirects))
        input_redirects.close()

    def get_redirected(self, article_title):
        try:
            logging.debug("get_redirect %s" % article_title)
            article_title = article_title.capitalize()
            redirect = self.redirects[article_title]
        except:
            redirect = None
        return redirect


class DataRetriever():

    def __init__(self, system_id, data_files_base):
        self.system_id = system_id
        self._bzip_file_name = '%s.processed.bz2' % data_files_base
        self._bzip_table_file_name = '%s.processed.bz2t' % data_files_base
        self._index_file_name = '%s.processed.idx' % data_files_base
        self.template_re = re.compile('({{.*?}})')
        self.redirects_checker = RedirectParser(data_files_base)
        # TODO: I need control cache size
        self.templates_cache = {}

    def _get_article_position(self, article_title):
        article_title = normalize_title(article_title)
        index_file = codecs.open(self._index_file_name, encoding='utf-8',
                mode='r')
        #index_file = open(self._index_file_name, mode='r')

        num_block = -1
        position = -1
        for index_line in  index_file.readlines():
            words = index_line.split()
            article = words[0]
            if article == article_title:
                num_block = int(words[1])
                position = int(words[2])
                break
        index_file.close()

        if num_block == -1:
            # look at redirects
            logging.debug("looking for '%s' at redirects" % article_title)
            redirect = self.redirects_checker.get_redirected(article_title)
            if redirect is not None:
                if redirect == article_title:
                    # to avoid infinite recursion
                    return -1, -1
                return self._get_article_position(redirect)

        return num_block, position

    def _get_block_start(self, num_block):
        bzip_table_file = open(self._bzip_table_file_name, mode='r')
        n = num_block
        table_line = ''
        while n > 0:
            table_line = bzip_table_file.readline()
            n -= 1
        if table_line == '':
            return -1
        parts = table_line.split()
        block_start = int(parts[0])
        bzip_table_file.close()
        return block_start

    def get_expanded_article(self, article_title):
        """
        This method does not do real template expansion
        is only used to test all the needed templates and redirects are
        available.
        """
        text_article = self.get_text_article(article_title)
        expanded_article = ''
        parts = self.template_re.split(text_article)
        for part in parts:
            if part.startswith('{{'):
                part = part[2:-2]
                #print "TEMPLATE: %s" % part
                if part.find('|') > -1:
                    template_name = part[:part.find('|')]
                else:
                    template_name = part
                # TODO: Plantilla should be a parameter
                template_name = normalize_title('Plantilla:%s' % template_name)
                if template_name in self.templates_cache:
                    expanded_article += self.templates_cache[template_name]
                else:
                    templates_content = self.get_text_article(template_name)
                    expanded_article += templates_content
                    self.templates_cache[template_name] = templates_content
            else:
                expanded_article += part
        return expanded_article

    def get_text_article(self, article_title):
        #print "Looking for article %s" % article_title
        num_block, position = self._get_article_position(article_title)
        #print "Found at block %d position %d" % (num_block, position)
        return self._get_block_text(num_block, position)

    def _get_block_text(self, num_block, position):
        output = ''
        block_start = self._get_block_start(num_block)
        #print "Block %d starts at %d" % (num_block, block_start)
        if block_start == -1:
            return ""

        # extract the block
        bzip_file = open(self._bzip_file_name, mode='r')
        cmd = ['./bin/%s/seek-bunzip' % self.system_id, str(block_start)]
        p = Popen(cmd, stdin=bzip_file, stdout=PIPE, stderr=STDOUT,
                close_fds=True)

        while position > 0:
            line = p.stdout.readline()
            position -= len(line)

        finish = False
        while not finish:
            line = p.stdout.readline()
            if line == '':
                # end of block?
                output += self._get_block_text(num_block + 1, 0)
                break
            if len(line) == 2:
                if ord(line[0]) == 3:
                    finish = True
                    break
            output += line
        p.stdout.close()
        return output
