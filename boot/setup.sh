#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
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
        kangs[$i]=`expr 3020 + $i`
    done

    #Regenerate the registrar config with the real ports included
    #(the bootstrap one just includes 2020 alone)
    IFS=','
    local portlist=$(echo "${ports[*]}" | sed 's/^,//')
    if [[ ${FLAVOR} == "manta" ]]; then
        local RTPL=$SVC_ROOT/sapi_manifests/registrar/template
    else
        local RTPL=$SVC_ROOT/sdc/sapi_manifests/registrar/template
    fi
    sed -e "s/@@PORTS@@/${portlist}/g" ${RTPL}.in > ${RTPL}

    # Wait until config-agent updates registrar's config before restarting
    # registrar.
    svcadm disable -s config-agent
    svcadm enable -s config-agent
    svcadm restart registrar

    #To preserve whitespace in echo commands...
    IFS='%'

    #haproxy
    for port in "${ports[@]}"; do
        hainstances="$hainstances        server moray-$port 127.0.0.1:$port check inter 10s slowstart 10s error-limit 3 on-error mark-down\n"
    done

    sed -e "s#@@MORAY_INSTANCES@@#$hainstances#g" \
        "$SVC_ROOT/etc/haproxy.cfg.in" > "$SVC_ROOT/etc/haproxy.cfg" || \
        fatal "could not process $src to $dest"

    svccfg import "$SVC_ROOT/smf/manifests/haproxy.xml" || \
        fatal "unable to import haproxy"
    svcadm enable "manta/haproxy" || fatal "unable to start haproxy"

    sed -e "s#@@MORAY_FLAVOR@@#$FLAVOR#g" \
        "$SVC_ROOT/smf/manifests/moray-pg-setup.xml.in" > \
        "$SVC_ROOT/smf/manifests/moray-pg-setup.xml" || \
        fatal "could not process moray-pg-setup.xml.in with sed"

    svccfg import "$SVC_ROOT/smf/manifests/moray-pg-setup.xml" || \
        fatal "unable to import moray-pg-setup"
    svcadm enable "smartdc/moray-pg-setup" || fatal "unable to start moray-pg-setup"

    #moray instances
    local moray_xml_in="$SVC_ROOT/smf/manifests/moray.xml.in"
    for (( i=1; i<=$moray_instances; i++ )); do
        local port=${ports[$i]}
        local kang=${kangs[$i]}
        local moray_instance="moray-$port"
        local moray_xml_out="$SVC_ROOT/smf/manifests/moray-$port.xml"
        sed -e "s#@@MORAY_PORT@@#$port#g" \
            -e "s#@@KANG_PORT@@#$kang#g" \
            -e "s#@@MORAY_INSTANCE_NAME@@#$moray_instance#g" \
            "$moray_xml_in" > "$moray_xml_out" || \
            fatal "could not process $moray_xml_in to $moray_xml_out"

        svccfg import "$moray_xml_out" || \
            fatal "unable to import $moray_instance: $moray_xml_out"
        svcadm enable "$moray_instance" || \
            fatal "unable to start $moray_instance"
    done

    #
    # We join the metric ports in a comma-separated list, then add this list as
    # metricPorts mdata to allow scraping by cmon-agent.
    #
    # The metricPorts values are derived from the moray service's "SIZE"
    # SAPI metadata. We don't need to worry about keeping the metricPorts
    # updated if this variable changes, because such a change does not affect
    # already-provisioned zones. This is because moray zones pull the "SIZE"
    # variable from /var/tmp/metadata.json, which is only written once, when the
    # zone is provisioned -- it is not managed by config-agent.
    #
    mdata-put metricPorts $(IFS=','; echo "${kangs[*]}")

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

    # Add node and CLI tools to PATH, and manual pages to MANPATH
    echo "" >>/root/.profile
    echo "export PATH=\$PATH:/opt/smartdc/$role/build/node/bin:/opt/smartdc/$role/node_modules/.bin:/opt/smartdc/$role/node_modules/$role/bin" >>/root/.profile
    echo "export MANPATH=/opt/smartdc/$role/node_modules/moray/man:\$MANPATH" >> /root/.profile


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

    # Add manual pages to MANPATH
    echo "export MANPATH=/opt/smartdc/$role/node_modules/moray/man:\$MANPATH" >> /root/.profile

    local moray_cfg=$SVC_ROOT/etc/config.json
    local svc_name=$(json -f ${METADATA} SERVICE_NAME)
    [[ $? -eq 0 ]] || fatal "Unable to retrieve service name"
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

    manta_common2_setup 'moray'

    manta_ensure_zk

    echo "Setting up Moray"
    manta_setup_moray_config

    # common bits (shared w/ SDC version)
    setup_moray
    manta_common2_setup_log_rotation 'moray'

    manta_common_setup_end

else # ${FLAVOR} == "sdc"

    # Local manifests
    CONFIG_AGENT_LOCAL_MANIFESTS_DIRS=/opt/smartdc/$role/sdc

    # Include common utility functions (then run the boilerplate)
    source /opt/smartdc/boot/lib/util.sh
    sdc_common_setup

    # Run the common moray setup
    setup_moray
    manta_common2_setup_log_rotation 'moray'

    # SDC-specific moray setup
    sdc_moray_setup

    # All done, run boilerplate end-of-setup
    sdc_setup_complete
fi

exit 0
