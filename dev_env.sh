export LOG_LEVEL=trace
export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:$PATH
alias test_db='dropdb test; createdb test && nodeunit ./test/db.test.js 3>&1 1>&2 2>&3 | bunyan'
alias server='node main.js -f ./etc/moray.development.config.json 2>&1 -d | bunyan'

function mcurl() {
    /usr/bin/curl -is -H 'Accept-Version: >=1.0' -H 'Accept: application/json' -H 'Content-Type: application/json' --url http://localhost:8080$@ | json;
    echo "";
}
