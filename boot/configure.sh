#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-

set -o xtrace

SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
SVC_ROOT=/opt/smartdc/moray

source ${DIR}/scripts/util.sh
source ${DIR}/scripts/services.sh


export PATH=$SVC_ROOT/bin:$SVC_ROOT/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH


function manta_setup_moray_config {
    #.bashrc
    echo 'function req() { grep "$@" `svcs -L moray` | bunyan ;}' >> $PROFILE
    echo 'export PATH=/opt/smartdc/moray/bin:$PATH' >> $PROFILE

    local moray_cfg=$SVC_ROOT/etc/config.json
    local svc_name=$(json -f ${METADATA} SERVICE_NAME)
    [[ $? -eq 0 ]] || fatal "Unable to retrieve service name"

    # Postgres sucks at return codes, so we basically have no choice but to
    # ignore the error code here since we can't conditionally create the DB
    createdb -h pg.$svc_name -p 5432 -U postgres moray
    psql -U postgres -h pg.$svc_name -p 5432 \
        -c 'CREATE TABLE IF NOT EXISTS buckets_config (name text PRIMARY KEY, index text NOT NULL, pre text NOT NULL, post text NOT NULL, options text, mtime timestamp without time zone DEFAULT now() NOT NULL);' \
        moray
    [[ $? -eq 0 ]] || fatal "Unable to create moray database"

    echo "alias manatee_stat='manatee_stat -s $svc_name -p $zk'" >> $PROFILE
    echo "alias psql='/opt/local/bin/psql -h pg.$svc_name -U postgres moray'" >> $PROFILE
}


function manta_setup_moray {
    local moray_instances=1
    local size=`json -f ${METADATA} SIZE`
    if [ "$size" = "lab" ] || [ "$size" = "production" ]; then
        moray_instances=4
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


function manta_setup_moray_rsyslogd {
    #rsyslog was already set up by common setup- this will overwrite the
    # config and restart since we want moray to log locally.
    local domain_name=$(json -f ${METADATA} domain_name)
    [[ $? -eq 0 ]] || fatal "Unable to domain name from metadata"

    mkdir -p /var/tmp/rsyslog/work
    chmod 777 /var/tmp/rsyslog/work

    cat > /etc/rsyslog.conf <<"HERE"
$MaxMessageSize 64k

$ModLoad immark
$ModLoad imsolaris
$ModLoad imudp


$template bunyan,"%msg:R,ERE,1,FIELD:(\{.*\})--end%\n"

*.err;kern.notice;auth.notice			/dev/sysmsg
*.err;kern.debug;daemon.notice;mail.crit	/var/adm/messages

*.alert;kern.err;daemon.err			operator
*.alert						root

*.emerg						*

mail.debug					/var/log/syslog

auth.info					/var/log/auth.log
mail.info					/var/log/postfix.log

$WorkDirectory /var/tmp/rsyslog/work
$ActionQueueType LinkedList
$ActionQueueFileName mantafwd
$ActionResumeRetryCount -1
$ActionQueueSaveOnShutdown on

HERE

        cat >> /etc/rsyslog.conf <<HERE

# Support node bunyan logs going to local0 and forwarding
# only as logs are already captured via SMF
# Uncomment the following line to get local logs via syslog
local0.* /var/log/moray.log;bunyan
local0.* @@ops.$domain_name:10514

HERE

        cat >> /etc/rsyslog.conf <<"HERE"
$UDPServerAddress 127.0.0.1
$UDPServerRun 514

HERE

    svcadm restart system-log
    [[ $? -eq 0 ]] || fatal "Unable to restart rsyslog"

    #log pulling
    manta_add_logadm_entry "moray" "/var/log" "exact"
}


# Mainline

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/moray"

manta_common_setup "moray" 0

manta_ensure_zk
manta_ensure_manatee

echo "Setting up Moray"
manta_setup_moray_config
manta_setup_moray
manta_setup_moray_rsyslogd

manta_common_setup_end

exit 0
