#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

ctrl_c () {
    echo ""
    exit
}

trap ctrl_c SIGINT

while [ true ] ; do
    putobject -d '{"foo": "bar"}' foo bar ; getobject -s foo bar
done
