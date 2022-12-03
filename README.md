# landlord-tracker

## Running this

* install docker

* unzip the datafile into a ./data/raw directory

* to do a full run of the data build (loading and transforming the data):

```sh
export DOCKER_BUILDKIT=1
cp .env-sample .env
docker compose up --build
```

You'll see a "Finished build" message when it's finished. Output files are created
in `./data/clean`. You can connect to the postgres database on localhost:5433 using
a SQL client (like DBeaver).

* to (re)run only the load or transforms, after "compose up" has run
(run this in another window):

```sh
# runs only the initial load of database tables
docker compose run elt load

# runs only the transforms
docker compose run elt transform

# runs everything
docker compose run elt build-all
```

* to stop the stack, run `docker compose down`

However, this may not work on the first try so if you enconter any bugs during the process, please message thomas.da.paine on slack or in the signal to fix the problems you encounter. Thanks!


------------ Now some Notes! ----------------

- to log into postgres, run: landlord-tracker/landlord_tracker/engles/scripts/login_to_postgres.sh

- note, if you reinstall this, cleaned data .csv will be wiped out --- you have been warned!