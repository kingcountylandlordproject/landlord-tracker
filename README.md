# landlord-tracker

## Running this

This should work on Linux, Mac, and Windows.

* install docker

* if you're on Windows, run `git config --global core.autocrlf false`

* clone this repo

* unzip the data package zipfile into the `./data` directory

```sh
cd data
unzip /path/to/data.zip
```

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
# on windows, add "winpty" to the front
docker compose run elt build-all
```

You'll see a "Finished build" message when it's finished. You can connect to
the postgres database on localhost:5433 using a SQL client (like DBeaver) or use psql:

```docker exec -it landlord-tracker-db-1 psql -U postgres```

* to run the address report (replace 123 MAIN ST with a real address of an apartment
complex):

```sh
docker compose run elt python3 -m engels.reports.report_address "123 MAIN ST"
```

* to stop the stack, run `docker compose down`. This stops the postgres
database, but the data should be preserved and available agaion the next time
you run `compose up`.

## Doing Development

note: on windows, add "winpty" to the front of docker commands
to get interactivity and avoid output buffering

### Dev Process

To (re)run only the load or transforms as you are doing development:

```sh
# re-runs only the initial load of database tables
docker compose run elt load

# re-runs only the transforms
docker compose run elt transform

# you can also run bash in the container and run other things
docker compose run elt bash
```

### Documentation

After running the transform step, you can view the updated dbt-generated
HTML documentation at http://localhost:8000 to get a thousand foot view of
the pipelines.

### Naming Conventions

For tables:

`pre_` - data preprocessed from raw files

`raw_` - raw aka source tables, imported into the database directly from extract files

`int_` - intermediate tables; these only exist to help create other tables.

`stg_` - staged data: these are cleaned versions of raw tables, suitable for querying.

`ent_` - derived entity tables built from staging 

`vld_` - for validation
