#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
#

set -o xtrace
export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:node_modules/moray/bin:$PATH
alias npm='node `which npm`'
set +o xtrace
