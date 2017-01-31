#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
#

#
# This environment file is intended for use only in development environments.
# This should set up any environment variables or aliases that are helpful for
# running Moray directly out of this workspace.  See the README for details.
#

set -o xtrace
export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:node_modules/moray/bin:$PATH
alias npm='node `which npm`'
set +o xtrace
