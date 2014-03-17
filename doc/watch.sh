#!/bin/bash

cleanup() {
    kill_http_server
    exit
}

kill_http_server() {
    if [ -n "$HTTP_SERVER_PID" ]
    then
        kill $HTTP_SERVER_PID
    fi
}

run() {
    kill_http_server
    make html
    pushd _build/html
    python -m SimpleHTTPServer &
    HTTP_SERVER_PID=$!
    popd
}

trap cleanup SIGHUP SIGINT SIGTERM

run

while true
do
    inotifywait -e modify -e move -e create -e delete *.rst
    run
done
