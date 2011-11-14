#!/bin/bash

#
# couchdb-replication-manager-wrappper.sh
#
# Developed by lhost 
# Copyright (c) 2011 lhost
# Licensed under terms of GNU General Public License.
# All rights reserved.
#
# Changelog:
# 2011-11-09 - created
#


LOG="/var/log/couchdb/replication-manager.log";

which realpath > /dev/null || exit 1

SCRIPT_NAME="` basename \"$0\" `";
SCRIPT_PATH=$( ([ "x${0##/}" = "x$0" ] && realpath -s "`pwd`/$0") || realpath -s "$0")

PERL_SCRIPT_PATH="${SCRIPT_PATH%-wrapper.sh}.pl"

[ -x $PERL_SCRIPT_PATH ] || exit 2
while `true`; do
	echo "# `date '+%Y-%m-%d %X'` Starting $PERL_SCRIPT_PATH $@" >> $LOG
	$PERL_SCRIPT_PATH $@ 2>&1 >> $LOG
	sleep 10
done

