#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# Copyright (c) 2013 Joyent Inc., All rights reserved.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

role=moray
SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
PG_USER=moray
SVC_ROOT=/opt/smartdc/moray
ZONE_UUID=`/usr/bin/zonename`

export PATH=$SVC_ROOT/bin:$SVC_ROOT/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH

#
# XXX in the future this should come from SAPI and we should be pulling out
# the "application" that's the parent of this instance. (see: SAPI-173)
#
if [[ -n $(mdata-get sdc:tags.manta_role) ]]; then
    export FLAVOR="manta"
else
    export FLAVOR="sdc"
fi

function setup_moray_rsyslogd {
    #rsyslog was already set up by common setup- this will overwrite the
    # config and restart since we want moray to log locally.
    local domain_name=$(json -f ${METADATA} domain_name)
    [[ $? -eq 0 ]] || fatal "Unable to domain name from metadata"

    mkdir -p /var/tmp/rsyslog/work
    chmod 777 /var/tmp/rsyslog/work

    echo "Updating /etc/rsyslog.conf"
    mkdir -p /var/tmp/rsyslog/work
    chmod 777 /var/tmp/rsyslog/work

    cat > /etc/rsyslog.conf <<"HERE"
$MaxMessageSize 64k

$ModLoad immark
$ModLoad imsolaris
$ModLoad imudp

$template bunyan,"%msg:R,ERE,1,FIELD:(\{.*\})--end%\n"

*.err;kern.notice;auth.notice                   /dev/sysmsg
*.err;kern.debug;daemon.notice;mail.crit        /var/adm/messages

*.alert;kern.err;daemon.err                     operator
*.alert                                         root

*.emerg                                         *

mail.debug                                      /var/log/syslog

auth.info                                       /var/log/auth.log
mail.info                                       /var/log/postfix.log

$WorkDirectory /var/tmp/rsyslog/work
$ActionQueueType LinkedList
$ActionQueueFileName mantafwd
$ActionResumeRetryCount -1
$ActionQueueSaveOnShutdown on

# Support node bunyan logs going to local0 and forwarding
# only as logs are already captured via SMF
# Uncomment the following line to get local logs via syslog
local0.* /var/log/moray.log;bunyan

HERE

    if [[ ${FLAVOR} == "manta" ]]; then
        echo "local0.* @@ops.$domain_name:10514" >> /etc/rsyslog.conf
    fi

    cat >> /etc/rsyslog.conf <<"HERE"
$UDPServerAddress 127.0.0.1
$UDPServerRun 514
HERE

    svcadm restart system-log
    [[ $? -eq 0 ]] || fatal "Unable to restart rsyslog"

    if [[ ${FLAVOR} == "manta" ]]; then
        #log pulling
        manta_add_logadm_entry "moray" "/var/log" "exact"
    fi
}

# setup haproxy
function setup_moray {
    local moray_instances=4

    if [[ ${FLAVOR} == "manta" ]]; then
        local size=`json -f ${METADATA} SIZE`
        if [[ ${size} != "lab" && ${size} != "production" ]]; then
            moray_instances=1
        fi
    fi

    #Build the list of ports.  That'll be used for everything else.
    local ports
    for (( i=1; i<=$moray_instances; i++ )); do
        ports[$i]=`expr 2020 + $i`
    done

    #To preserve whitespace in echo commands...
    IFS='%'

    #haproxy
    for port in "${ports[@]}"; do
        hainstances="$hainstances        server moray-$port 127.0.0.1:$port check inter 10s slowstart 10s error-limit 3 on-error mark-down\n"
    done

    sed -e "s#@@MORAY_INSTANCES@@#$hainstances#g" \
        $SVC_ROOT/etc/haproxy.cfg.in > $SVC_ROOT/etc/haproxy.cfg || \
        fatal "could not process $src to $dest"

    svccfg import $SVC_ROOT/smf/manifests/haproxy.xml || \
        fatal "unable to import haproxy"
    svcadm enable "manta/haproxy" || fatal "unable to start haproxy"

    #moray instances
    local moray_xml_in=$SVC_ROOT/smf/manifests/moray.xml.in
    for port in "${ports[@]}"; do
        local moray_instance="moray-$port"
        local moray_xml_out=$SVC_ROOT/smf/manifests/moray-$port.xml
        sed -e "s#@@MORAY_PORT@@#$port#g" \
            -e "s#@@MORAY_INSTANCE_NAME@@#$moray_instance#g" \
            $moray_xml_in  > $moray_xml_out || \
            fatal "could not process $moray_xml_in to $moray_xml_out"

        svccfg import $moray_xml_out || \
            fatal "unable to import $moray_instance: $moray_xml_out"
        svcadm enable "$moray_instance" || \
            fatal "unable to start $moray_instance"
    done

    unset IFS
}

function sdc_moray_setup {
    # Cookie to identify this as a SmartDC zone and its role
    mkdir -p /var/smartdc/$role
    mkdir -p /opt/smartdc/$role/ssl
    mkdir -p /opt/smartdc/$role/etc
    /usr/bin/chown -R root:root /opt/smartdc

    # SSL Cert
    echo "Generating SSL Certificate"
    /opt/local/bin/openssl req -x509 -nodes -subj '/CN=*' -newkey rsa:2048 \
        -keyout /opt/smartdc/$role/ssl/key.pem \
        -out /opt/smartdc/$role/ssl/cert.pem -days 3650

    # Add build/node/bin and node_modules/.bin to PATH
    echo "" >>/root/.profile
    echo "export PATH=\$PATH:/opt/smartdc/$role/build/node/bin:/opt/smartdc/$role/node_modules/.bin:/opt/smartdc/$role/node_modules/$role/bin" >>/root/.profile

    # Log Rotation FTW
    # What about manta/haproxy?
    sdc_log_rotation_add amon-agent /var/svc/log/*amon-agent*.log 1g
    sdc_log_rotation_add config-agent /var/svc/log/*config-agent*.log 1g
    sdc_log_rotation_add registrar /var/svc/log/*registrar*.log 1g
    sdc_log_rotation_add moray /var/log/moray.log 1g
    sdc_log_rotation_setup_end

}

function manta_setup_moray_config {
    #.bashrc
    echo 'function req() { grep "$@" `svcs -L moray` | bunyan ;}' >> $PROFILE
    echo 'export PATH=/opt/smartdc/moray/bin:$PATH' >> $PROFILE

    local moray_cfg=$SVC_ROOT/etc/config.json
    local svc_name=$(json -f ${METADATA} SERVICE_NAME)
    [[ $? -eq 0 ]] || fatal "Unable to retrieve service name"

    # create the moray user which isn't a super user but can create tables and
    # can't create rolse. Creating the user will fail if the user alredy
    # exists, so we don't check error, subsequent pg requests will fail with
    # this user if it dne.
    createuser -U postgres -h pg.$svc_name -p 5432 -d -S -R $PG_USER

    # Postgres sucks at return codes, so we basically have no choice but to
    # ignore the error code here since we can't conditionally create the DB
    createdb -h pg.$svc_name -p 5432 -U $PG_USER moray
    psql -U $PG_USER -h pg.$svc_name -p 5432 \
        -c 'CREATE TABLE IF NOT EXISTS buckets_config (name text PRIMARY KEY, index text NOT NULL, pre text NOT NULL, post text NOT NULL, options text, mtime timestamp without time zone DEFAULT now() NOT NULL);' \
        moray
    [[ $? -eq 0 ]] || fatal "Unable to create moray database"

    echo "alias manatee-stat='/opt/smartdc/moray/node_modules/.bin/manatee-stat -s $svc_name -p $zk'" >> $PROFILE
    echo "alias psql='/opt/local/bin/psql -h pg.$svc_name -U $PG_USER moray'" >> $PROFILE
}

function sdc_moray_createdb {
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
    # MORAY-156: Create "moray" user and grant all privileges into "moray"
    # database. This way we can upgrade existing setups and switch to
    # "moray" user instead of "postgres"
    if [[ -z $(PGPASSWORD=PgresPass123 /opt/local/bin/psql \
      -U postgres \
      -h ${POSTGRES_HOST} ${role} -c "\du"|grep ${role}) ]]; then
      echo "User ${role} does not exist. Creating it"
      /opt/local/bin/createuser -U postgres -h ${POSTGRES_HOST} -d -S -R $PG_USER
    else
      echo "User ${role} already exists."
    fi

    # We can safely execute this as many times as we want to:
    PGPASSWORD=PgresPass123 /opt/local/bin/psql \
      -U postgres \
      -h ${POSTGRES_HOST} ${role} \
      -c "GRANT ALL PRIVILEGES ON DATABASE $role to $role;">/dev/null
    PGPASSWORD=PgresPass123 /opt/local/bin/psql \
      -U postgres \
      -h ${POSTGRES_HOST} ${role} \
      -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $role;">/dev/null
    PGPASSWORD=PgresPass123 /opt/local/bin/psql \
      -U postgres \
      -h ${POSTGRES_HOST} ${role} \
      -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $role;">/dev/null
    # The following is needed in order to be able to update buckets:
    PGPASSWORD=PgresPass123 /opt/local/bin/psql \
      -U postgres \
      -h ${POSTGRES_HOST} \
      -c 'alter database moray owner to moray;' moray

    for tbl in `PGPASSWORD=PgresPass123 /opt/local/bin/psql -h ${POSTGRES_HOST} -U postgres -qAt -c "select tablename from pg_tables where schemaname = 'public';" moray`; do
      PGPASSWORD=PgresPass123 /opt/local/bin/psql \
        -h ${POSTGRES_HOST} \
        -U postgres \
        -c "alter table $tbl owner to moray" moray;>/dev/null
    done

    for tbl in `PGPASSWORD=PgresPass123 /opt/local/bin/psql -h ${POSTGRES_HOST} -U postgres -qAt -c "select sequence_name from information_schema.sequences where sequence_schema = 'public';" moray`; do
      PGPASSWORD=PgresPass123 /opt/local/bin/psql \
        -U postgres \
        -h ${POSTGRES_HOST} \
        -c "alter table $tbl owner to moray" moray;>/dev/null
    done

}


# Will likely move these into sdc util.sh once I can test them:
function sdc_ensure_manatee {
    local attempt=0
    local isok=0
    local pgok
    local zkok

    local zonename=$(zonename)

    local svc_name=$(json -f ${METADATA} SERVICE_NAME)
    local zk_ips=$(json -f ${METADATA} ZK_HA_SERVERS | json -a host)

    if [[ -z ${zk_ips} ]]; then
        zk_ips=$(json -f ${METADATA} ZK_SERVERS | json -a host)
    fi

    if [[ $? -ne 0 ]] ; then
        zk_ips=127.0.0.1
    fi

    while [[ $attempt -lt 90 ]]
    do
        for ip in $zk_ips
        do
            zkok=$(echo "ruok" | nc -w 1 $ip 2181)
            if [[ $? -eq 0 ]] && [[ "$zkok" == "imok" ]]
            then
                pgok=$(/opt/smartdc/moray/node_modules/.bin/manatee-stat -s $svc_name $ip 2>/dev/null | json sdc.registrar.database.primary)
                if [[ $? -eq 0 ]] && [[ $pgok == tcp* ]]
                then
                    isok=1
                    break
                fi
            fi
        done

        if [[ $isok -eq 1 ]]
        then
            break
        fi

        let attempt=attempt+1
        sleep 1
    done
    [[ $isok -eq 1 ]] || fatal "manatee is not up"
}


function sdc_ensure_zk {
    local attempt=0
    local isok=0
    local zkok

    local zonename=$(zonename)

    local zk_ips=$(json -f ${METADATA} ZK_SERVERS | json -a host)
    if [[ $? -ne 0 ]] ; then
        zk_ips=127.0.0.1
    fi

    while [[ $attempt -lt 60 ]]
    do
        for ip in $zk_ips
        do
            zkok=$(echo "ruok" | nc -w 1 $ip 2181)
            if [[ $? -eq 0 ]] && [[ "$zkok" == "imok" ]]
            then
                isok=1
                break
            fi
        done

        if [[ $isok -eq 1 ]]
        then
            break
        fi

        let attempt=attempt+1
        sleep 1
    done
    [[ $isok -eq 1 ]] || fatal "ZooKeeper is not running"
}


if [[ ${FLAVOR} == "manta" ]]; then

    source ${DIR}/scripts/util.sh
    source ${DIR}/scripts/services.sh

    # XXX See MANTA-1615.  These manifests are shipped for SDC but aren't relevant
    # for the manta image, so remove them until the situation with SDC/manta
    # manifests is resolved.
    rm -rf $SVC_ROOT/sdc/sapi_manifests

    echo "Running common setup scripts"
    manta_common_presetup

    echo "Adding local manifest directories"
    manta_add_manifest_dir "/opt/smartdc/moray"

    manta_common_setup "moray" 0

    manta_ensure_zk
    manta_ensure_manatee

    echo "Setting up Moray"
    manta_setup_moray_config

    # common bits (shared w/ SDC version)
    setup_moray
    setup_moray_rsyslogd

    manta_common_setup_end

else # ${FLAVOR} == "sdc"

    # Local manifests
    CONFIG_AGENT_LOCAL_MANIFESTS_DIRS=/opt/smartdc/$role/sdc

    # Include common utility functions (then run the boilerplate)
    source /opt/smartdc/boot/lib/util.sh
    sdc_common_setup

    # Run the common moray setup
    setup_moray
    setup_moray_rsyslogd

    # SDC-specific moray setup
    sdc_moray_setup

    sdc_ensure_zk
    sdc_ensure_manatee

    # Create the DB for moray
    sdc_moray_createdb

    # All done, run boilerplate end-of-setup
    sdc_setup_complete
fi

exit 0
