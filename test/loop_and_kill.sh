ctrl_c () {
    echo ""
    exit
}

trap ctrl_c SIGINT

while [ true ] ; do
    putobject -d '{"foo": "bar"}' foo bar ; getobject -s foo bar
done
