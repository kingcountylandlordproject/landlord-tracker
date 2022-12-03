# landlord-tracker

## Running this

* install docker

* unzip the datafile into a ./data/raw directory

* start services required for the build (right now, just the database):

```sh
# docker build needs buildkit enabled
export DOCKER_BUILDKIT=1

# copy sample .env file
cp .env-sample .env

# starts the database running in the background (takes a minute)
docker compose up --build -d
```

* to do a full run of the data build (loading and transforming the data):

```sh
# run the entire build
docker compose run elt build-all
```

You'll see a "Finished build" message when it's finished. Output files are created
in `./data/clean`. You can connect to the postgres database on localhost:5433 using
a SQL client (like DBeaver) or use psql:

```docker exec -it landlord-tracker-db-1 psql -U postgres```

* to (re)run only the load or transforms as you are doing development:

```sh
# runs only the initial load of database tables
docker compose run elt load

# runs only the transforms
docker compose run elt transform
```

* to stop the stack, run `docker compose down`. This stops the postgres
database, but the data should be preserved and available agaion the next time
you run `compose up`.

However, this may not work on the first try so if you enconter any bugs during the process, please message thomas.da.paine on slack or in the signal to fix the problems you encounter. Thanks!


------------ Now some Notes! ----------------

- to log into postgres, run: landlord-tracker/landlord_tracker/engles/scripts/login_to_postgres.sh

- note, if you reinstall this, cleaned data .csv will be wiped out --- you have been warned!