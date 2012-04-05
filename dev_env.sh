export MORAY_URL=http://localhost:8080
export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:$PATH

alias newdb='dropdb moray; createdb moray'
alias test_db='newdb && nodeunit ./test/db/*.test.js 3>&1 1>&2 2>&3 | bunyan'
alias server='node main.js -f ./etc/moray.development.config.json 2>&1 -d | bunyan'

function mcurl() {
    /usr/bin/curl -isk \
        -H 'Accept-Version: ~1.0' \
        -H 'Accept: application/json' \
        -H 'Content-Type: application/json' \
        --url ${MORAY_URL}$@ ;
    echo "";
}
