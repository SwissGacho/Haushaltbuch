# Haushaltbuch

## Requirements
The following Python modules have to be installed:
* `websocket`
* `aiomysql`


## DB Configuration

In the directory from which the frontend app is called, put a file called `configuration.json` having the following content:
```json
{
    "host": "hostname",
    "db": "hhb",
    "user": "appusername",
    "password": "secret"
}
```
`hostname` is the hostname or IP-address of the DB server, `hhb` is the schema name, `appusername` is the DB username of the app and `secret` is the password for `appusername`.