#!/usr/bin/env python
#
# Copyright (C) 2007, One Laptop Per Child
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
from sugar.activity import bundlebuilder
import sys
import os
import shutil
import urllib2
import subprocess
import zipfile
import ConfigParser
import codecs


def download_file(url):
    web_file = urllib2.urlopen(url)
    local_file_name = 'cache/' + url.split('/')[-1]
    local_file = open(local_file_name, 'w')
    chunk = 4096
    while 1:
        data = web_file.read(chunk)
        if not data:
            print "done."
            break
        local_file.write(data)
        print ".",
    web_file.close()
    local_file.close()
    return local_file_name

if len(sys.argv) == 1:
    print "Use ./setup prepare lang"
    exit()

prepare_ok = True
if sys.argv[1] == 'prepare':
    if len(sys.argv) < 3:
        print 'You must select a language. For example: ./setup.py prepare es'
        prepare_ok = False
    else:
        prepare_ok = False
        language = sys.argv[2]
        print "Preparing", language
        config = ConfigParser.ConfigParser()
        config.readfp(open('data_repository.cfg'))
        data_repository = config.get('repository', 'url')

        last_version = config.get('last_versions', language)
        print "Check data", last_version
        already_downloaded = False
        if not os.path.exists('cache'):
            os.mkdir('cache')

        flag_file_name = 'download_complete.' + language
        if os.path.exists(flag_file_name):
            flag_file = open(flag_file_name, 'r')
            for line in flag_file:
                if line == last_version:
                    already_downloaded = True
            flag_file.close()
        if not already_downloaded:
            url_data = data_repository + last_version
            url_md5 = url_data.replace('.zip', '.md5sum')
            print "Downloading data", url_data
            print "md5 data", url_md5
            local_md5_file_name = download_file(url_md5)
            local_data_file_name = download_file(url_data)
        else:
            local_data_file_name = 'cache/' + last_version.split('/')[-1]
            local_md5_file_name = local_data_file_name.replace('.zip', \
                    '.md5sum')

        real_md5 = subprocess.check_output(['/usr/bin/md5sum', \
                local_data_file_name])
        real_md5_value = real_md5.split(' ')[0]
        print "md5sum %s = %s" % (local_data_file_name, real_md5_value)
        md5_ok = False
        for line in open(local_md5_file_name):
            if line.find(real_md5_value) == 0:
                md5_ok = True
        if md5_ok:
            print "MD5 Ok"
            # Unzip data files
            zf = zipfile.ZipFile(local_data_file_name, 'r')
            list_data_files = zf.namelist()
            root_data_directory = list_data_files[0]
            # Create temporary directory
            tmp_directory = 'TMP'
            if not os.path.exists(tmp_directory):
                os.mkdir(tmp_directory)
            zf.extractall(tmp_directory)
            zf.close()
            if os.path.exists(root_data_directory):
                shutil.rmtree(root_data_directory)
            shutil.move(tmp_directory + '/' + root_data_directory, root_data_directory)
            shutil.rmtree(tmp_directory)
            # Create flag file
            flag_file = open(flag_file_name, 'w')
            flag_file.write(last_version)
            flag_file.close()

            # Copy activity.info file
            print "Create activity.info for", language
            shutil.copyfile('activity/activity.info.' + language,
                'activity/activity.info')

            # Create MANIFEST
            print "Create MANIFEST"
            list_git_files = subprocess.check_output(['git', 'ls-files'])
            manifest_file = codecs.open('MANIFEST', 'w', encoding='utf-8')
            manifest_file.write('activity/activity.info\n')
            for name_file in list_git_files:
                manifest_file.write(name_file)
            for name_file in list_data_files:
                if not os.path.isdir(name_file):
                    manifest_file.write(name_file + '\n')
            manifest_file.close()

            prepare_ok = True
        else:
            print "MD5 check error"


if sys.argv[1] == 'fix_manifest':
        print "Don't use fix_manifest. The MANIFEST is build in prepare stage"
        prepare_ok = False

if prepare_ok:
    bundlebuilder.start('Wikipedia')
