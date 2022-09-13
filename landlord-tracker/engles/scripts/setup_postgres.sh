#!/bin/bash

./install_postgres.sh
./start_postgres.sh


sudo -u postgres psql -c "ALTER USER postgres ENCRYPTED PASSWORD 'v5UtGtw06T%nBqW';"
sudo -u postgres psql -c "CREATE ROLE king_county_land CREATEDB ENCRYPTED PASSWORD '15EiidZabIx3SFi';"
sudo -u postgres psql -c "CREATE DATABASE king_county_parcels WITH OWNER = king_county_land;"
sudo -u postgres psql -c "CREATE USER connect_1 ENCRYPTED PASSWORD '^4cm0Kkt4Kgs3RG';"
sudo -u postgres psql -c "GRANT king_county_land TO connect_1;"
