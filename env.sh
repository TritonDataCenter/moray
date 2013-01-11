export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:node_modules/moray/bin:$PATH

alias server='node main.js -f ./etc/config.coal.json -s 2>&1 | bunyan'
alias npm='node `which npm`'
