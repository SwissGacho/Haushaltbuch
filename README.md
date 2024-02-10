# Haushaltbuch

## Requirements
The following Python modules have to be installed:
* `websocket`
* `aiomysql` (if MySQL or MariaDB is used)
* `aiosqlite` (if SQLite is used)


## DB Configuration

At startup the application is looking for a configuration file called `configuration.json` in the directory from which the application is started.
The configuration file is then searched for an `"db_cfg"` object.
This object should have exactly one of the following contents.

### SQLite

```json
{
    "db_cfg": 
    {
        "file": "db_filename"
    }
}
```
`db_filename` is the filename of the database file. It can be an absolute path or a path relative to the starting directory.

### MySQL / MariaDB

```json
{
    "db_cfg": 
    {
        "host": "hostname",
        "db": "hhb",
        "user": "appusername",
        "password": "secret"
    }
}
```
`hostname` is the hostname or IP-address of the DB server, `hhb` is the schema name, `appusername` is the DB username of the app and `secret` is the password for `appusername`.