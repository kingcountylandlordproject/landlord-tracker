#!/bin/bash

set -e

DIR=$PWD

load () {
    cd $DIR/landlord_tracker/engles

    echo "(Re)creating and loading database tables"
    python3 ./database_builder.py
}

transform () {
    cd $DIR/landlord_tracker/engles

    echo "Running transforms"
    python3 ./cleaning.py
}

if [ "$1" = 'load' ]; then
    load

elif [ "$1" = 'transform' ]; then
    transform

elif [ "$1" = 'build-all' ]; then
    load
    transform
    echo "Finished build"
elif [ "$1" = 'noop' ]; then
    echo "Doing nothing."
else
    exec "$@"
fi
