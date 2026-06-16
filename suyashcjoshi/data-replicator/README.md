# InfluxDB 3 Custom Data Replication Plugin

This plugin replicates **any/all data** written to an InfluxDB 3 Core/Enterprise instance to a remote InfluxDB 3 bucket (e.g Cloud Serverless Instance). It features durable queuing, compression, validation, downsampling, and table filtering, and is source-agnostic, working with Telegraf, custom scripts, or any client.

## Software requirements

- **InfluxDB 3 Core / Enterprise**
- **InfluxDB Cloud Account**: InfluxDB 3 account information where you want to replicate data e.g. Cloud Serverless URL, API Token with write access for the given bucket/database where data needs to be replicated.
- **Telegraf**: For collecting system metrics (optional). Anytime, however data gets written to InfluxDB 3 Core/Enterprise, this plugin will come into action.
- **Python**: 3.7+

## Files

- `data-replicator.py`: Data replication plugin code
- `telegraf.conf`: Example Telegraf config for collecting and wrting system metrics.

## Features

- Custom Data Replication: Replicate all or optionally downsampled data to another InfluxDB 3 instance
- Compressed Queue: Stores compressed data in edr_queue.jsonl.gz locally to handle connection interruptions etc.
- Table Filtering: Replicate all or optionally specific tables.

## Setup, run, and test the plugin

1. **Install and and set up InfluxDB 3 Core/Enterprise**:

 - [Download and install InfluxDB 3 Core](https://docs.influxdata.com/influxdb3/core/install/).
 - Make sure the "plugins" directory exists, otherwise create one `mkdir -p ~/.plugins`
 - Place [data-replicator.py](https://github.com/suyashcjoshi/influxdb3_plugins/blob/main/suyashcjoshi/data-replicator/data-replicator.py) in `~/.plugins/`. The plugin dynamically uses its own directory for queuing (edr_queue.jsonl.gz) which it creates in the same folder.

2. **Start InfluxDB 3 with the Processing Engine enabled** (`--plugin-dir /path/to/plugins`):

 ```bash
 influxdb3 serve \
 --node-id host01 \
 --object-store file \
 --data-dir ~/.influxdb3 \
 --plugin-dir ~/.plugins
 ```

3. **Download and install the InfluxDB 3 python module used by the plugin**:

```bash
   influxdb3 install package influxdb3-python
```

1. *Optional*: Download and install [Telegraf](https://docs.influxdata.com/telegraf/v1/install/) for your system

2. *Optional*: Configure and run Telegraf to write to InfluxDB 3 Core/Enterprise You can download the [example Telegraf config](https://github.com/suyashcjoshi/influxdb3_plugins/blob/main/suyashcjoshi/data-replicator/telegraf.config) for collecting system metrics and writing them to InfluxDB 3.

To start Telegraf with the configuration, run the following command:

```bash
   telegraf --config telegraf.conf
```

Alternatively, you can run the plugin without Telegraf as long as data is being written to InfluxDB 3 locally.

### Run the plugin

1. **Create a trigger**

Run the following command to create a [data-write trigger](https://docs.influxdata.com/influxdb3/core/plugins/) to run the plugin for all tables inside database `mydb`.

```bash
   influxdb3 create trigger \
     -d mydb \
     --plugin-filename data-replicator.py \
     --trigger-spec "all_tables" \
     --trigger-arguments "host=YOUR_HOST_URL,token=YOUR_TOKEN,database=mydb" \
     data_replicator_trigger
```

**Parameters**:

- tables: Comma-separated tables to replicate (e.g., cpu,mem). Omit for all.
- database: name of your database/bucket in your InfluxDB 3 instance where you want to replicate data (e.g. Cloud serverless URL)
- host: provide host URL for your InfluxDB 3 instance where you want to replicate (e.g. Cloud Serverless URL)
- token: provide authentication token for your InfluxDB 3 instance where you want to replicate the data (e.g Cloud Serverless API token)
- aggregate_interval: This is used to down sample data at given interval (e.g., 1m for 1-minute averages). Omit this for no downsampling.

1. **Enable the trigger**:

```bash
   influxdb3 enable trigger --database mydb data_replicator_trigger
```

### Test data replication using the plugin

1. **Clear the queue**

```bash
   rm ~/.plugins/edr_queue.jsonl.gz
```

1. **Run Telegraf** (Restart if already running)

```bash
   telegraf --config telegraf.conf
```

1. **Run Telegraf for at least 1 minute**.

To stop Telegraf, enter `Ctrl + C` in the terminal where it is running.

1. **Query the local InfluxDB 3 instance** using the CLI:

```bash
   influxdb3 query --database mydb "SELECT * FROM cpu WHERE cpu = 'cpu-total' AND time >= now() - interval '5 minutes' LIMIT 2"
```

1. **Query the Cloud Serverless instance** using the same SQL query as for the remote instance--enter the following into the Data Explorer UI tool within InfluxDB 3 Cloud Serverless:

```sql
   SELECT * FROM cpu WHERE cpu = 'cpu-total' AND time >= now() - interval '5 minutes' LIMIT 2
```

### Test data replication with downsampling

1. **Clear local queue**:  

```bash
   rm ~/.plugins/edr_queue.jsonl.gz
```

1. **Create a trigger**: Enable downsampling by providing the `aggregate_interval` argument--for example:

```bash
   influxdb3 create trigger \
     -d mydb \
     --plugin-filename data-replicator.py \
     --trigger-spec "all_tables" \
     --trigger-arguments "host=YOUR_HOST_URL,token=YOUR_TOKEN,database=mydb,tables=cpu,aggregate_interval=1m" \
     --error-behavior retry \
     data_replicator_trigger
```

1. **Enable the trigger**:

```bash
   influxdb3 enable trigger --database mydb data_replicator_trigger
```

1. **Restart Telegraf**:

```bash
   telegraf --config telegraf.conf
```

*Run Telegraf for at least 2 minutes to collect enough data.* To stop Telegraf, you can use `Ctrl + C`.

1. **Query the local instance (not downsampled)**:

```bash
   influxdb3 query --database mydb "SELECT * FROM cpu WHERE cpu = 'cpu-total' AND time >= now() - interval '5 minutes' LIMIT 2"
```

1. **Query the remote InfluxDB 3 instance (downsampled)**:

```sql
   SELECT * FROM cpu WHERE cpu = 'cpu-total' AND time >= now() - interval '5 minutes' LIMIT 2
```

## Support

For additional support with InfluxData plugins, visit:

- [Discord](https://discord.com/invite/influxdata) - #influxdb3_core channel
- [Community Forums](https://community.influxdata.com/)
- [GitHub Issues](https://github.com/influxdata/influxdb3_plugins/issues)
