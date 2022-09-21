#!/bin/bash

sudo apt install libpq-dev

pip install -r ./requirements.txt
cd ./landlord-tracker/engles
./scripts/install_postgres.sh
./scripts/start_postgres.sh
./scripts/setup_postgres.sh
python3 ./database_builder.py
