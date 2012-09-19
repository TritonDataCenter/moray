export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:node_modules/moray/bin:$PATH

alias moray='node main.js -f ./etc/config.coal.json -v -s 2>&1 | bunyan'
