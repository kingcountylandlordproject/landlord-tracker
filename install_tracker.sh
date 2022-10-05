#!/bin/bash

sudo apt install libpq-dev

pip install -r ./requirements.txt
cd ./landlord_tracker/engles/scripts
./purge_postgres.sh
./install_postgres.sh
./start_postgres.sh
./setup_postgres.sh
cd ..
python3 ./database_builder.py
python3 ./cleaning.py
