#!/bin/bash

set -e

DIR=$PWD

HOME_DBT_DIR=~/.dbt

PROFILES_YML="${HOME_DBT_DIR}/profiles.yml"

if [ ! -f $PROFILES_YML ]; then
    echo "creating $PROFILES_YML"
    mkdir -p $HOME_DBT_DIR
    cat > $PROFILES_YML << EOF
landlord_tracker:
  outputs:
    dev:
      type: postgres
      threads: 2
      host: $DB_HOSTNAME
      port: 5432
      user: $DB_USER
      pass: $DB_PASSWORD
      dbname: $DB_USER
      schema: public

  target: dev
EOF
fi

preprocess() {
    echo "Preprocessing files"
    python3 -m engels.preprocess.pre_apartment_complex_address
    python3 -m engels.preprocess.pre_corporations
    python3 -m engels.preprocess.pre_real_property_account
}

load () {
    echo "Loading database tables"
    load_sources.py
}

transform () {
    echo "Running transforms"
    cd $DIR/dbt
    dbt run
    dbt test
}

docs () {
    echo "Creating docs"
    cd $DIR/dbt
    dbt docs generate
}

if [ "$1" = 'preprocess' ]; then
    preprocess

elif [ "$1" = 'load' ]; then
    load

elif [ "$1" = 'transform' ]; then
    transform
    docs

elif [ "$1" = 'build-all' ]; then
    preprocess
    load
    transform
    docs
    echo "Finished build"

elif [ "$1" = 'serve-docs' ]; then
    mkdir -p $DIR/dbt/target
    cd $DIR/dbt/target
    python3 -m http.server

elif [ "$1" = 'noop' ]; then
    echo "Doing nothing."
else
    exec "$@"
fi
