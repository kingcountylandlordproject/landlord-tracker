
# Aleph

This is work in progress.

```sh
# make sure you're in this directory
cd ~/landlord-tracker/aleph_lt

# Aleph docs say to run this to allow ElasticSearch to map its memory
sudo sysctl -w vm.max_map_count=262144

# bring up all the services
docker compose up --build -d

# need to do this once to initialize
docker compose run shell aleph upgrade

# create a user
docker compose run shell aleph createuser --name="test user" --admin --password=admin admin@example.com

# load data into Aleph
docker compose run shell /landlord-tracker/aleph_lt/load_mappings.sh
```

Log into the web UI as the user you created.
