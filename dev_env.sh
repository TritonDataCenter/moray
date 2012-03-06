export LOG_LEVEL=trace
export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:$PATH
alias test_db='dropdb test; createdb test && nodeunit ./test/db.test.js 3>&1 1>&2 2>&3 | bunyan'
alias server='node main.js -f ./etc/moray.development.config.json 2>&1 | bunyan'
