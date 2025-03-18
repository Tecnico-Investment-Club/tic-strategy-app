# paper-engine-orders

Strategy to Broker interaction loader.

# Purpose
...

**Entity Relationship Model**

![ERM](docs/entity_relationship_model.png)

See extra data remarks and initial EDA in documentation [here](docs/value_proposition.md) and [here](notebooks/eda.ipynb).


**Database diagram**

![DB](docs/table_diagram.png)

See extra data specification documentation [here](docs/data_specification.md) and [user guide](docs/user_guide.pdf).

# Why do we need this datasource?
...

**Historical Data:**
...

## Delivery mechanism and frequency
...

# EDA Main Conclusions
1. TODO
2. TODO
3. TODO
4. TODO

# How to use this on my own repo?

## Take care of the requirements (you only do this once)
1. Load ``paper-engine.drawio`` into [here](https://app.diagrams.net);
2. Create ``Entity Relationship Model`` and replace ``/docs/entity_relationship_model.png`` file;
3. Do datasource initial exploratory analysis in ``/notebooks/eda.ipynb``;
4. Complete ``value_proposition.md``;
5. Model loader database tables and replace ``/docs/table_diagram.png`` file;
6. Complete ``data_specification.md``;
7. Implement ``db/*.sql`` files;
8. Implement ``model/*.py`` files;
9. Implement ``queries/*.py`` files;
10. Implement source in ``/persistance/source.py``;
11. Implement target in ``/persistance/target.py``;
12. Implement class Loader in ``__main__.py`` file;
13. Implement prometheus metrics in ``__main__.py`` file;
14. Implement event messages in ``/messaging/publisher.py`` file.

# Getting started
1. Install poetry in your system:
```shell
$ python3 -m pip install poetry --user
```

2. Install nox:
```shell
$ python3 -m pip install nox --user
```

3. Install your project:
```shell
$ poetry install --with dev
```

## Other tasks
### Adding dependencies
1. Add/remove your project dependencies:
```shell
$ poetry add/remove <package_name>
```

2. Compile dependencies for docker:
```shell
$ poetry export -f requirements.txt --output requirements.txt --without-hashes 
```

3. Publish package to nexus:
```shell
$ poetry config repositories.nexus-pub https://registry.analytics.indexconstellation.com/repository/pypi-2iq-hosted/
$ poetry config http-basic.nexus-pub <username> <password>
$ poetry publish --build --repository nexus-pub
```


### Updating dependencies
1. Update:
```shell
$ poetry update
```

2. Test that nothing broke:
```shell
$ nox
```

3. Update project version:
```shell
$ poetry version <desired_version>
```

### Release

Use the release tab on the repository GitHub page and tag using `v<major>.<minor>.<patch>` format, e.g. `v0.1.0`.


## Run process

The project is Dockerized and ready to run with all the dependencies in place - this is the preferred way of executing the project because it provides a consistent execution environment.
```shell
$ docker run -d --name paper-engine-orders-1 --restart unless-stopped --env-file paper-engine-orders.env docker.analytics.indexconstellation.com/loaders/paper-engine-orders:x.y.z
```

This will run a container with the project, grab the variables from the env-file (see `config.env.sample`) and start the application.
The project has two main modes of operation (depending on the value of the environment variable `RUN_AS_SERVICE`):
- run only once and exit;
- run continuously as a service.

Upon starting, the application connects to both data sources (source: s3 `SOURCE` bucket, and, target: pgsql `TARGET` db) and will iteratively apply the following logic:
- Check last process file from target loader_deliveries table.
- Go to bucket and get next file after this one
- Process next file

If the application is running as a service (`RUN_AS_SERVICE=True`) there will be a delay (15s-30s) between each iteration to limit the load of polling for new data on the origin server.

Expected output:
```shell
2022-09-22 09:58:53 INFO [__main__.py:370]: Delivery 24: process_dbo data/dbo/entity_3/LOAD00000001.csv.gz.
2022-09-22 09:58:53 INFO [__main__.py:696]: Delivery 24: persisted to postgres.
2022-09-22 09:58:53 INFO [__main__.py:698]: Delivery 24: persisted {}.
2022-09-22 09:58:53 INFO [__main__.py:887]: Delivery 24: messages published.
2022-09-22 09:58:53 INFO [__main__.py:922]: Delivery 24: prometheus counters incremented.
2022-09-22 09:58:53 INFO [__main__.py:353]: Delivery 24: processed (0:00:00.046061 seconds).
2022-09-22 09:58:54 INFO [__main__.py:370]: Delivery 25: process_dbo data/dbo/entity_2/LOAD00000001.csv.gz.
2022-09-22 09:58:54 INFO [__main__.py:696]: Delivery 25: persisted to postgres.
2022-09-22 09:58:54 INFO [__main__.py:698]: Delivery 25: persisted {}.
2022-09-22 09:58:54 INFO [__main__.py:887]: Delivery 25: messages published.
2022-09-22 09:58:54 INFO [__main__.py:922]: Delivery 25: prometheus counters incremented.
2022-09-22 09:58:54 INFO [__main__.py:353]: Delivery 25: processed (0:00:00.046207 seconds).
2022-09-22 09:58:54 INFO [__main__.py:370]: Delivery 26: process_dbo data/dbo/entity_1/LOAD00000001.csv.gz.
2022-09-22 09:58:57 INFO [__main__.py:533]: Delivery 26: processed 0/159967...
2022-09-22 09:59:00 INFO [__main__.py:533]: Delivery 26: processed 10000/159967...
2022-09-22 09:59:03 INFO [__main__.py:533]: Delivery 26: processed 20000/159967...
2022-09-22 09:59:06 INFO [__main__.py:533]: Delivery 26: processed 30000/159967...
2022-09-22 09:59:10 INFO [__main__.py:533]: Delivery 26: processed 40000/159967...
2022-09-22 09:59:13 INFO [__main__.py:533]: Delivery 26: processed 50000/159967...
2022-09-22 09:59:16 INFO [__main__.py:533]: Delivery 26: processed 60000/159967...
2022-09-22 09:59:20 INFO [__main__.py:533]: Delivery 26: processed 70000/159967...
2022-09-22 09:59:23 INFO [__main__.py:533]: Delivery 26: processed 80000/159967...
2022-09-22 09:59:27 INFO [__main__.py:533]: Delivery 26: processed 90000/159967...
2022-09-22 09:59:30 INFO [__main__.py:533]: Delivery 26: processed 100000/159967...
2022-09-22 09:59:33 INFO [__main__.py:533]: Delivery 26: processed 110000/159967...
2022-09-22 09:59:36 INFO [__main__.py:533]: Delivery 26: processed 120000/159967...
2022-09-22 09:59:40 INFO [__main__.py:533]: Delivery 26: processed 130000/159967...
2022-09-22 09:59:43 INFO [__main__.py:533]: Delivery 26: processed 140000/159967...
2022-09-22 09:59:45 INFO [__main__.py:533]: Delivery 26: processed 150000/159967...
2022-09-22 10:00:19 INFO [__main__.py:662]: Delivery 26: entity_1 (create: 159967, amend: 0, remove: 0).
```

## Developing

In order for a developer to get started with the project an initial development environment is required:
```shell
$ git clone https://github.com/docker.analytics.indexconstellation.com/loaders/paper-engine-orders.git
$ cd paper-engine-orders/
$ pip install -r requirements.txt
```
The project has 4 key components to be aware of:
  - `Loader` the main class and the entry point for the project;
  - `Source` the class that abstracts the "source" data source (S3 Bucket);
  - `Target` the class that abstracts the "target" data source (PostgresSQL);
  - `Notifier` the class that abstracts the publishing of notifications.

### Building

In order for you to build the Docker container and export it as a standalone image use the following commands:
```shell
$ docker build -t docker.analytics.indexconstellation.com/loaders/paper-engine-orders:x.y.z .
$ docker save -o paper-engine-orders.zip docker.analytics.indexconstellation.com/loaders/paper-engine-orders:x.y.z
```

Once the build finishes, a new container image and tag with a version will be present in your `docker image` list.
You can then export that version as a `zip` file and import it elsewhere.


### Deploying
```shell
$ docker push docker.analytics.indexconstellation.com/loaders/paper-engine-orders:x.y.z
```


## Features

- Run continuously as a service: controlled by the environment variable `RUN_AS_SERVICE=True`;
- Run only once and then exit: controlled by the environment variable `RUN_AS_SERVICE=False`;
- Publish notifications when entities change: controlled by `NOTIFICATIONS=True`;
- Dry run: controlled by `DRY_RUN=True`.


## Configuration

This project has the following features that are controlled by environment variables:

#### RUN_AS_SERVICE
Type: `Boolean`
Default: `True`

Enables the service mode.
This will make the application run until an exception is found or until a stop signal SIGINT is sent to the running PID.

#### MIN_SLEEP
Type: `Number`
Default: `15`

The minimum random delay that the process will wait until it runs the polling process again.
The application will sleep for a random amount of seconds between [`MIN_SLEEP`..`MAX_SLEEP`]

#### MAX_SLEEP
Type: `Number`
Default: `30`

The maximum random delay that the process will wait until it runs the polling process again.
The application will sleep for a random amount of seconds between [`MIN_SLEEP`..`MAX_SLEEP`]

#### DRY_RUN
Type: `Boolean`
Default: `False`

Enables or disables the Dry Run mode of the application. In this mode no data is persisted to any of the target databases.

#### NOTIFICATIONS
Type: `Boolean`
Default: `True`

If the process should publish messages to the message broker or not.

#### NATS
Type: `String`
Default: `(empty)`

A NATS2 URL with the following format:
```
nats://user:password@hostname-or-ip-address:4222
```

#### SOURCE
Type: `String`
Default: `(empty)`

AWS credentials to connect with a bucket with the following format:
```
bucket_endpoint=bucket_endpoint bucket_id=bucket_id bucket_key=bucket_key bucket_name=bucket_name bucket_path=bucket_path
```

#### TARGET
Type: `String`
Default: `(empty)`

A Postgres connection string with the following format:
```
user=username password=password host=localhost port=5432 dbname=paper_engine
```


## Notifications

You can find below the catalog of notifications that this project publishes (prioritised by order of publication).
The name of this data source is `paper-engine` and all the topics are scoped to that namespace. In addition to this,
the `meta` component is used as an umbrella topic for events that relate to the component itself.

Examples:
  - `paper-engine.meta.orders.change` refers to `changes` inside the process `orders`.
This specific pipeline happens to only have one process, therefore, some redundancy is exists.

#### Summary of all notification topics
  - `paper-engine.meta.orders.change`

#### Updates to entities
Topic: `paper-engine.meta.orders.change`
Encoding: `JSON`

Published when the process updates `entity_1`, `entity_2` and `entity_3` tables.

Example:
```json
{
  "v": 1,
  "ds": "paper-engine",
  "id": "orders",
  "host": "hostname",
  "utcts": "2021-12-09T08:31:04.824890",
  "stats": {
    "entity_1": {
      "AMEND": 0,
      "CREATE": 159967,
      "REMOVE": 0
    },
    "entity_2": {
      "AMEND": 0,
      "CREATE": 1494975,
      "REMOVE": 0
    },
    "entity_3": {
      "AMEND": 0,
      "CREATE": 90,
      "REMOVE": 0
    }
  },
  "max_event_id": 1020487673
}
```
