#!/usr/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

#
# coal-test-env.sh: Creates PG DB moray_test and boots a new moray-test instance
# listening to port 2222 for testing.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

MANATEE=$(/opt/smartdc/bin/sdc-vmname manatee)

zlogin $MANATEE "/opt/local/bin/createdb -U postgres -O moray moray_test"

zlogin $MANATEE "/opt/local/bin/psql -U postgres moray_test --command='
    CREATE TABLE buckets_config (
        name text PRIMARY KEY,
        index text NOT NULL,
        pre text NOT NULL,
        post text NOT NULL,
        options text,
        mtime timestamp without time zone DEFAULT now() NOT NULL
    );'"

zlogin $MANATEE "/opt/local/bin/psql -U postgres -c 'alter table buckets_config owner to moray' moray_test > /dev/null"

MORAY=$(/opt/smartdc/bin/sdc-vmname moray)

MORAY_TEST_SMF=/opt/smartdc/moray/smf/manifests/moray-test.xml

zlogin $MORAY "cp /opt/smartdc/moray/etc/config.json /opt/smartdc/moray/etc/config.test.json"

zlogin $MORAY "cp /opt/smartdc/moray/smf/manifests/moray-2021.xml /opt/smartdc/moray/smf/manifests/moray-test.xml"

zlogin $MORAY "/opt/local/bin/gsed -i -e \"s|smartdc-moray|smartdc-moray-test|\" $MORAY_TEST_SMF"

zlogin $MORAY "/opt/local/bin/gsed -i -e \"s|smartdc/application/moray|smartdc/application/moray-test|\" $MORAY_TEST_SMF"

zlogin $MORAY "/opt/local/bin/gsed -i -e \"s|2021|2222|\" $MORAY_TEST_SMF"

zlogin $MORAY "/opt/local/bin/gsed -i -e \"s|value=\\\"/usr/lib/extendedFILE.so.1\\\" />|value=\\\"/usr/lib/extendedFILE.so.1\\\" /><envvar name=\\\"MORAY_DB_NAME\\\" value=\\\"moray_test\\\" />|\" $MORAY_TEST_SMF"

zlogin $MORAY "/usr/sbin/svccfg import /opt/smartdc/moray/smf/manifests/moray-test.xml; /usr/sbin/svcadm enable moray-test"

