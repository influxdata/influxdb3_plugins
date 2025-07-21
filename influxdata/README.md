# influxdata
Official Python plugins for InfluxDB 3, developed by InfluxData.

## Test the plugins

To test the plugins, run the following command in the root directory of this repository:

```bash
# Test all influxdata plugins
./test-plugins.sh influxdata
```

### More test examples

```bash
# Test specific plugin
./test-plugins.sh influxdata/basic_transformation

# List available organizations and plugins
./test-plugins.sh --list

# Show help
./test-plugins.sh --help
```

## Support

For additional support with InfluxData plugins, visit:
- [Discord](https://discord.com/invite/influxdata) - #influxdb3_core channel
- [Community Forums](https://community.influxdata.com/)
- [GitHub Issues](https://github.com/influxdata/influxdb3_plugins/issues)