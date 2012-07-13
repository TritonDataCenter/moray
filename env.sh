export PATH=$PWD/build/node/bin:$PWD/node_modules/.bin:$PATH

alias server='node main.js -f ./etc/config.laptop.json -vvv -s 2>&1 | bunyan'
