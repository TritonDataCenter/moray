#!/usr/bin/bash
#
# Copyright (c) 2012 Joyent Inc., All rights reserved.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

PATH=/opt/local/bin:/opt/local/sbin:/usr/bin:/usr/sbin
role=moray

# Local manifests
CONFIG_AGENT_LOCAL_MANIFESTS_DIRS=/opt/smartdc/$role/sdc

# Include common utility functions (then run the boilerplate)
source /opt/smartdc/sdc-boot/lib/util.sh
sdc_common_setup

# Cookie to identify this as a SmartDC zone and its role
mkdir -p /var/smartdc/$role
mkdir -p /opt/smartdc/$role/ssl

mkdir -p /opt/smartdc/$role/etc
cd -
/usr/bin/chown -R root:root /opt/smartdc

echo "Generating SSL Certificate"
/opt/local/bin/openssl req -x509 -nodes -subj '/CN=*' -newkey rsa:2048 \
    -keyout /opt/smartdc/$role/ssl/key.pem \
    -out /opt/smartdc/$role/ssl/cert.pem -days 3650


# Add build/node/bin and node_modules/.bin to PATH
echo "" >>/root/.profile
echo "export PATH=\$PATH:/opt/smartdc/$role/build/node/bin:/opt/smartdc/$role/node_modules/.bin:/opt/smartdc/$role/node_modules/$role/bin" >>/root/.profile

logadm -w moray -C 48 -c -p 1h \
    /var/svc/log/smartdc-application-moray:default.log

MANATEE_MAIN_ADMIN_IP=$(json -f /var/tmp/metadata.json manatee_admin_ips)

# Try to connect to manatee PostgreSQL instance. If we can connect, create the
# moray db when required. If we cannot connect after 10 retries, fail and exit.
#
# FIXME: Actually, we're manually overriding manatee's PG Password and setting
# the PG user to the default one. These should be configuration values:
POSTGRES_HOST=$MANATEE_MAIN_ADMIN_IP
POSTGRES_PW='PgresPass123'
for i in 0 1 2 3 4 5 6 7 8 9
do
    if ! PGPASSWORD=PgresPass123 /opt/local/bin/psql -U postgres \
        -h ${POSTGRES_HOST} -c "\l" 2>/dev/null; then
        sleep 5
        if [[ "$i" == "9" ]]; then
            echo "Connecting to manatee PostgreSQL failed. Exiting."
            exit 1
        else
            continue
        fi
    else
      if [[ -z $(PGPASSWORD=PgresPass123 /opt/local/bin/psql \
          -U postgres \
          -h ${POSTGRES_HOST} -c "\l"|grep $role) ]]; then
            echo "Creating $role database"
            PGPASSWORD=PgresPass123 /opt/local/bin/createdb \
            -U postgres \
            -h ${POSTGRES_HOST} $role
          if [[ -z $(PGPASSWORD=PgresPass123 /opt/local/bin/psql \
                -U postgres \
                -h ${POSTGRES_HOST} ${role} -c "\dt"|grep buckets_config) ]]; then
              echo "Creating table buckets_config"
              PGPASSWORD=PgresPass123 /opt/local/bin/psql \
                -U postgres \
                -h ${POSTGRES_HOST} $role -c "CREATE TABLE buckets_config (
                    name text PRIMARY KEY,
                    index text NOT NULL,
                    pre text NOT NULL,
                    post text NOT NULL,
                    options text,
                    mtime timestamp without time zone DEFAULT now() NOT NULL
                );"
              break
          else
              break
          fi
      else
          break
      fi
    fi
done

# All done, run boilerplate end-of-setup
sdc_setup_complete

exit 0
