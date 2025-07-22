# influxdata
Official Python plugins for InfluxDB 3, developed by InfluxData.

## Test the plugins

From the root directory, use Docker Compose to test plugins:

```bash
# Test all influxdata plugins
docker compose --profile test run --rm test-core-all

# Test specific plugin
PLUGIN_PATH="influxdata/basic_transformation" \
docker compose --profile test run --rm test-core-specific

# Test with TOML configuration
PLUGIN_PATH="influxdata/basic_transformation" \
PLUGIN_FILE="basic_transformation.py" \
TOML_CONFIG="basic_transformation_config_scheduler.toml" \
docker compose --profile test run --rm test-core-toml
```

## Support

For additional support with InfluxData plugins, visit:
- [Discord](https://discord.com/invite/influxdata) - #influxdb3_core channel
- [Community Forums](https://community.influxdata.com/)
- [GitHub Issues](https://github.com/influxdata/influxdb3_plugins/issues)