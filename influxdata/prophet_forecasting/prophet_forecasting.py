"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {
            "name": "source_measurement",
            "example": "temperature",
            "description": "Measurement to read. Required with field unless query is set.",
            "required": false
        },
        {
            "name": "field",
            "example": "value",
            "description": "Numeric field to forecast. Required with source_measurement unless query is set.",
            "required": false
        },
        {
            "name": "query",
            "example": "SELECT date_bin(INTERVAL '1 hour', time) AS time, room, avg(value) AS value FROM temperature WHERE time > now() - INTERVAL '30 days' GROUP BY 1, room",
            "description": "SQL whose result is forecast instead of source_measurement and field. The timestamp column must be named time; text columns are series keys and each numeric column is forecast on its own. The whole result is read on every run, so bound its time range in the query. The CLI splits --trigger-arguments at commas, so set a query with commas in the TOML config.",
            "required": false
        },
        {
            "name": "source_database",
            "example": "demo",
            "description": "Database to read from. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "tag_filter",
            "example": "room=Kitchen site=eu",
            "description": "Space-separated tag=value pairs the rows must match, one value per tag; several values per tag only through the TOML config. Not with query.",
            "required": false
        },
        {
            "name": "sql_filter",
            "example": "value > 0",
            "description": "SQL condition without WHERE, AND-ed with tag_filter and inserted into the query as is. Not with query. The CLI splits --trigger-arguments at commas, so set a condition with commas in the TOML config.",
            "required": false
        },
        {
            "name": "group_by",
            "example": "room",
            "description": "Space-separated tags to split by; each distinct combination is its own series with its own model, otherwise all rows form one. Without sampling_interval, a series mixing tag sets that write at the same time is refused. A tag named like a stored row column (field, model, forecast, step, ...) is refused. Not with query.",
            "required": false
        },
        {
            "name": "series_limit",
            "example": "5",
            "description": "Forecast only the first N forecasts (series x fields), ordered by the series' group_by tags or query key columns as text, a missing value last, then by field name; the rest are skipped. An integer >= 1, at most max_series. Unset: every series.",
            "required": false
        },
        {
            "name": "window",
            "example": "30d",
            "description": "History as a time window ending now, including validation_window: a whole number and us, ms, s, min, h, d, w, m (a month, 30.42 d), q (91.25 d) or y (365 d). With point_limit, the last point_limit points of the window. Window or point_limit is required unless query is set; with point_limit alone the window is point_limit buckets of sampling_interval (at most 365d) or 30d of raw points.",
            "required": false
        },
        {
            "name": "point_limit",
            "example": "2000",
            "description": "History as the most recent points of each series (buckets with sampling_interval), an integer >= 3. Window or point_limit is required unless query is set. Unset: every point of the window, however many; training time grows with them.",
            "required": false
        },
        {
            "name": "sampling_interval",
            "example": "1h",
            "description": "Resample history to the mean per bucket of this duration, which is also the forecast step; the bucket still filling is left out. Not with forecast_freq. Default: the raw points.",
            "required": false
        },
        {
            "name": "forecast_freq",
            "example": "MS",
            "description": "Step of the forecast points as a pandas frequency, fixed (30min, 1h) or calendar (D, W-SUN, MS, QS, B), over the raw points. Not with sampling_interval. Default: the frequency pandas infers from the data, or else their median spacing.",
            "required": false
        },
        {
            "name": "horizon_steps",
            "example": "24",
            "description": "Integer >= 1: forecast length in points. Exactly one of horizon_steps and horizon_duration.",
            "required": false
        },
        {
            "name": "horizon_duration",
            "example": "2d",
            "description": "Forecast length as a duration (us, ms, s, min, h, d, w, m, q, y) from the last history point, rounded up to whole steps. Exactly one of horizon_steps and horizon_duration.",
            "required": false
        },
        {
            "name": "levels",
            "example": "0.5 0.8",
            "description": "Space-separated interval levels, each between 0 and 1, written as lower_<percent> and upper_<percent>. Default: 0.5 0.8.",
            "required": false
        },
        {
            "name": "model",
            "example": "prophet",
            "description": "The only model is prophet, the default; any other value is refused.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "_forecasts.temperature",
            "description": "Measurement the forecast is written to. Default: _forecasts.<source_measurement>; required when query reads several measurements.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecasts",
            "description": "Database the forecast is written to. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "forecast_id",
            "example": "ops-1h",
            "description": "Written to every row as the tag forecast_id, so forecasts of one series with different settings do not overwrite each other. Unset: no tag.",
            "required": false
        },
        {
            "name": "model_mode",
            "example": "train",
            "description": "train fits a new in-memory model on every run. predict loads the model saved for unique_suffix and the series, or trains and saves it when there is no file. Default: train.",
            "required": false
        },
        {
            "name": "unique_suffix",
            "example": "20250619_v1",
            "description": "Model version, part of the saved model's file name: up to 64 characters from letters, digits, '.', '_' and '-'. Required with model_mode predict.",
            "required": false
        },
        {
            "name": "seasonality_mode",
            "example": "additive",
            "description": "Prophet seasonality mode: additive or multiplicative. Default: additive.",
            "required": false
        },
        {
            "name": "changepoint_prior_scale",
            "example": "0.05",
            "description": "Flexibility of trend changepoints, a number above 0. Default: 0.05.",
            "required": false
        },
        {
            "name": "changepoints",
            "example": "2025-01-01 2025-06-01",
            "description": "Space-separated ISO dates of trend changepoints, in UTC unless they name a timezone; each must fall within every series' training history. A TOML list is also accepted.",
            "required": false
        },
        {
            "name": "holiday_date_list",
            "example": "2025-01-01 2025-12-25",
            "description": "Space-separated ISO dates of custom holidays, in UTC unless they name a timezone, set together with holiday_names and as many. A TOML list is also accepted.",
            "required": false
        },
        {
            "name": "holiday_names",
            "example": "NewYear Christmas",
            "description": "Space-separated names matching holiday_date_list; a name with spaces only through a TOML list. A name Prophet reserves, such as trend, is refused.",
            "required": false
        },
        {
            "name": "holiday_country_names",
            "example": "US",
            "description": "Country code for built-in holidays, refused if Prophet does not know it. Prophet supports one country, so of several only the first is used, with a warning.",
            "required": false
        },
        {
            "name": "validation_window",
            "example": "3d",
            "description": "Duration held back from training before now. The forecast is compared with the measured points there, and a series that fails is not written. With sampling_interval it is rounded out to whole buckets, at least one. Unset: no validation.",
            "required": false
        },
        {
            "name": "validation_alignment",
            "example": "nearest",
            "description": "How measured and forecast points are paired: position pairs them in time order, nearest pairs each measured point with the closest forecast point within half a step. Default: position.",
            "required": false
        },
        {
            "name": "msre_threshold",
            "example": "0.05",
            "description": "Highest Mean Squared Relative Error a validated series may have, a number >= 0. Default: no limit.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Integer >= 1: most forecasts one run may produce (series x fields); more is refused unless series_limit keeps fewer. Default: 100.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "10000",
            "description": "Integer >= 1: most forecast points per series, counting the validation window; a longer horizon is refused. Default: 10000.",
            "required": false
        },
        {
            "name": "is_sending_alert",
            "example": "true",
            "description": "Whether to send one alert per run listing the series that failed validation: true or false. Default: false.",
            "required": false
        },
        {
            "name": "notification_text",
            "example": "Validation failed on table:$measurement, field:$field, series:$series",
            "description": "Template of the alert. Variables: $version, $measurement, $field, $series, $start_time, $end_time, $output_measurement.",
            "required": false
        },
        {
            "name": "senders",
            "example": "slack sms",
            "description": "Space-separated sender types: slack, discord, http, sms, whatsapp. Required when is_sending_alert is true.",
            "required": false
        },
        {
            "name": "notification_path",
            "example": "notify",
            "description": "URL path of the notification sender plugin. Default: notify.",
            "required": false
        },
        {
            "name": "port_override",
            "example": "8182",
            "description": "Port for notification dispatch, 1-65535. Default: 8181.",
            "required": false
        },
        {
            "name": "slack_webhook_url",
            "example": "https://hooks.slack.com/...",
            "description": "Webhook URL for Slack notifications. Required with the slack sender.",
            "required": false
        },
        {
            "name": "slack_headers",
            "example": "eyJDb250ZW50LVR5cGUiOiAiYXBwbGljYXRpb24vanNvbiJ9",
            "description": "Optional base64-encoded headers for the Slack webhook.",
            "required": false
        },
        {
            "name": "discord_webhook_url",
            "example": "https://discord.com/api/webhooks/...",
            "description": "Webhook URL for Discord notifications. Required with the discord sender.",
            "required": false
        },
        {
            "name": "discord_headers",
            "example": "eyJDb250ZW50LVR5cGUiOiAiYXBwbGljYXRpb24vanNvbiJ9",
            "description": "Optional base64-encoded headers for the Discord webhook.",
            "required": false
        },
        {
            "name": "http_webhook_url",
            "example": "https://example.com/webhook",
            "description": "Webhook URL for HTTP POST notifications. Required with the http sender.",
            "required": false
        },
        {
            "name": "http_headers",
            "example": "eyJDb250ZW50LVR5cGUiOiAiYXBwbGljYXRpb24vanNvbiJ9",
            "description": "Optional base64-encoded headers for the HTTP webhook.",
            "required": false
        },
        {
            "name": "twilio_sid",
            "example": "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "description": "Twilio Account SID. Required with the sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_to_number",
            "example": "+1234567890",
            "description": "Recipient phone number. Required with the sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "twilio_from_number",
            "example": "+19876543210",
            "description": "Twilio sender phone number. Required with the sms or whatsapp sender.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "prophet_forecasting_scheduler.toml",
            "description": "TOML file, relative to the plugin directory or absolute; its keys override these arguments.",
            "required": false
        }
    ],
    "http_args_config": [
        {
            "name": "source_measurement",
            "example": "temperature",
            "description": "Default measurement to read. A request sets it, or query, if this does not.",
            "required": false
        },
        {
            "name": "field",
            "example": "value",
            "description": "Default numeric field to forecast. A request sets it, or query, if this does not.",
            "required": false
        },
        {
            "name": "query",
            "example": "SELECT time, room, value FROM temperature WHERE time > now() - INTERVAL '30 days'",
            "description": "Default SQL whose result is forecast. A request's source_measurement and field replace it. The CLI splits --trigger-arguments at commas, so set a query with commas in the TOML config.",
            "required": false
        },
        {
            "name": "source_database",
            "example": "demo",
            "description": "Default database to read from. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "tag_filter",
            "example": "room=Kitchen site=eu",
            "description": "Default space-separated tag=value pairs the rows must match. Not with query.",
            "required": false
        },
        {
            "name": "sql_filter",
            "example": "value > 0",
            "description": "Default SQL condition without WHERE, AND-ed with tag_filter and inserted into the query as is. Not with query.",
            "required": false
        },
        {
            "name": "group_by",
            "example": "room",
            "description": "Default space-separated tags to split by; each distinct combination is its own series with its own model. Not with query.",
            "required": false
        },
        {
            "name": "series_limit",
            "example": "5",
            "description": "Default number of forecasts to keep, an integer >= 1, at most max_series. Unset: every series.",
            "required": false
        },
        {
            "name": "window",
            "example": "30d",
            "description": "Default history window (us, ms, s, min, h, d, w, m, q, y). A request's start_time and end_time replace window and point_limit.",
            "required": false
        },
        {
            "name": "point_limit",
            "example": "2000",
            "description": "Default count of the most recent points per series, an integer >= 3.",
            "required": false
        },
        {
            "name": "sampling_interval",
            "example": "1h",
            "description": "Default bucket for resampling history, also the forecast step. A request's forecast_freq replaces it.",
            "required": false
        },
        {
            "name": "forecast_freq",
            "example": "MS",
            "description": "Default step of the forecast points as a pandas frequency. A request's sampling_interval replaces it.",
            "required": false
        },
        {
            "name": "horizon_steps",
            "example": "24",
            "description": "Default forecast length in points, an integer >= 1. A request's horizon_duration replaces it.",
            "required": false
        },
        {
            "name": "horizon_duration",
            "example": "2d",
            "description": "Default forecast length as a duration. A request's horizon_steps replaces it.",
            "required": false
        },
        {
            "name": "levels",
            "example": "0.5 0.8",
            "description": "Default space-separated interval levels, each between 0 and 1. Default: 0.5 0.8.",
            "required": false
        },
        {
            "name": "model",
            "example": "prophet",
            "description": "The only model is prophet, the default; any other value is refused.",
            "required": false
        },
        {
            "name": "write_results",
            "example": "true",
            "description": "Boolean: also write each forecast to target_measurement. Default: false.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "_forecasts.temperature",
            "description": "Measurement a written forecast goes to. Default: _forecasts.<source_measurement>.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecasts",
            "description": "Database a written forecast goes to. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "forecast_id",
            "example": "ops-1h",
            "description": "Written to every row of a written forecast as the tag forecast_id. Unset: no tag.",
            "required": false
        },
        {
            "name": "start_time",
            "example": "2025-05-20T00:00:00Z",
            "description": "Start of an absolute history, ISO 8601 with a timezone. Set with end_time, instead of window and point_limit. Not with query.",
            "required": false
        },
        {
            "name": "end_time",
            "example": "2025-06-19T00:00:00Z",
            "description": "End of an absolute history, ISO 8601 with a timezone; the forecast continues from the last point before it. Rows later than now are read too, unless sampling_interval ends the history at its last full bucket.",
            "required": false
        },
        {
            "name": "model_mode",
            "example": "predict",
            "description": "train fits a new in-memory model per request. predict loads the model saved for unique_suffix and the series, or trains and saves it. Default: train.",
            "required": false
        },
        {
            "name": "unique_suffix",
            "example": "20250619_v1",
            "description": "Model version, part of the saved model's file name: up to 64 characters from letters, digits, '.', '_' and '-'. Required with model_mode predict.",
            "required": false
        },
        {
            "name": "seasonality_mode",
            "example": "additive",
            "description": "Prophet seasonality mode: additive or multiplicative. Default: additive.",
            "required": false
        },
        {
            "name": "changepoint_prior_scale",
            "example": "0.05",
            "description": "Flexibility of trend changepoints, a number above 0. Default: 0.05.",
            "required": false
        },
        {
            "name": "changepoints",
            "example": "2025-01-01 2025-06-01",
            "description": "Space-separated ISO dates of trend changepoints, in UTC unless they name a timezone; each must fall within every series' training history.",
            "required": false
        },
        {
            "name": "holiday_date_list",
            "example": "2025-07-04",
            "description": "Space-separated ISO dates of custom holidays, in UTC unless they name a timezone, set together with holiday_names and as many.",
            "required": false
        },
        {
            "name": "holiday_names",
            "example": "IndependenceDay",
            "description": "Space-separated names matching holiday_date_list; a name with spaces only through a list in TOML or the JSON body. A name Prophet reserves, such as trend, is refused.",
            "required": false
        },
        {
            "name": "holiday_country_names",
            "example": "US",
            "description": "Country code for built-in holidays, refused if Prophet does not know it. Prophet supports one country, so of several only the first is used, with a warning.",
            "required": false
        },
        {
            "name": "validation_window",
            "example": "3d",
            "description": "Duration held back from training before now or end_time. A series that fails the check answers code other, without a forecast. With sampling_interval it is rounded out to whole buckets, at least one. Not with backtest_points. Unset: no validation.",
            "required": false
        },
        {
            "name": "validation_alignment",
            "example": "nearest",
            "description": "How measured and forecast points are paired: position or nearest. Default: position.",
            "required": false
        },
        {
            "name": "msre_threshold",
            "example": "0.05",
            "description": "Highest Mean Squared Relative Error a validated series may have, a number >= 0. Default: no limit.",
            "required": false
        },
        {
            "name": "backtest_points",
            "example": "12",
            "description": "Default count of the latest history points a backtest holds out, an integer from 1 to max_forecast_points. Not with validation_window. Unset: no backtest.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Integer >= 1: most forecasts one request may produce (series x fields); more is refused unless series_limit keeps fewer. Default: 100.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "10000",
            "description": "Integer >= 1: most forecast points per series, counting the validation window. Default: 10000.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "prophet_forecasting_http.toml",
            "description": "TOML file, relative to the plugin directory or absolute; its keys override these arguments, and a request overrides it. Never read from a request.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "source_measurement",
            "example": "temperature",
            "description": "Measurement to read. Required with field unless query is set here or on the trigger.",
            "required": false
        },
        {
            "name": "field",
            "example": "value",
            "description": "Numeric field to forecast. Required with source_measurement unless query is set here or on the trigger.",
            "required": false
        },
        {
            "name": "query",
            "example": "SELECT date_bin(INTERVAL '1 hour', time) AS time, room, avg(value) AS value FROM temperature WHERE time > now() - INTERVAL '30 days' GROUP BY 1, room",
            "description": "SQL whose result is forecast instead of source_measurement and field. The timestamp column must be named time; text columns are series keys and each numeric column is forecast on its own. The whole result is read, so bound its time range in the query.",
            "required": false
        },
        {
            "name": "source_database",
            "example": "demo",
            "description": "Database to read from. Default: the trigger's database. An unknown database is refused.",
            "required": false
        },
        {
            "name": "tag_filter",
            "example": "{'room': ['Kitchen', 'Lounge'], 'site': 'eu'}",
            "description": "JSON object of the tag values rows must match: one value, or an array of several per tag. Not with query.",
            "required": false
        },
        {
            "name": "sql_filter",
            "example": "value > 0",
            "description": "SQL condition without WHERE, AND-ed with tag_filter and inserted into the query as is. Not with query.",
            "required": false
        },
        {
            "name": "group_by",
            "example": "['room']",
            "description": "JSON array of tags to split by; each distinct combination is its own series with its own model, otherwise all rows form one. Without sampling_interval, a series mixing tag sets that write at the same time is refused. Not with query.",
            "required": false
        },
        {
            "name": "series_limit",
            "example": "5",
            "description": "Integer >= 1, at most max_series: forecast only the first N forecasts (series x fields), ordered by the series' group_by tags or query key columns as text, a missing value last, then by field name; series_total in the response counts them all. Unset: every series.",
            "required": false
        },
        {
            "name": "window",
            "example": "30d",
            "description": "History as a time window ending now, including validation_window: a whole number and us, ms, s, min, h, d, w, m (a month, 30.42 d), q (91.25 d) or y (365 d). With point_limit, the last point_limit points of the window. A measurement needs window, point_limit, or start_time and end_time; with point_limit alone the window is point_limit buckets of sampling_interval (at most 365d) or 30d of raw points.",
            "required": false
        },
        {
            "name": "point_limit",
            "example": "2000",
            "description": "Integer >= 3: history as the most recent points of each series (buckets with sampling_interval). Unset: every point of the window, however many; training time grows with them.",
            "required": false
        },
        {
            "name": "start_time",
            "example": "2025-05-20T00:00:00Z",
            "description": "Start of an absolute history, ISO 8601 with a timezone. Set with end_time, instead of window and point_limit. Not with query.",
            "required": false
        },
        {
            "name": "end_time",
            "example": "2025-06-19T00:00:00Z",
            "description": "End of an absolute history, ISO 8601 with a timezone; the forecast continues from the last point before it. Rows later than now are read too, unless sampling_interval ends the history at its last full bucket.",
            "required": false
        },
        {
            "name": "sampling_interval",
            "example": "1h",
            "description": "Resample history to the mean per bucket of this duration, which is also the forecast step; the bucket still filling is left out. Not with forecast_freq. Default: the raw points.",
            "required": false
        },
        {
            "name": "forecast_freq",
            "example": "MS",
            "description": "Step of the forecast points as a pandas frequency, fixed (30min, 1h) or calendar (D, W-SUN, MS, QS, B), over the raw points. Not with sampling_interval. Default: the frequency pandas infers from the data, or else their median spacing.",
            "required": false
        },
        {
            "name": "horizon_steps",
            "example": "24",
            "description": "Integer >= 1: forecast length in points. Exactly one of horizon_steps and horizon_duration, here or on the trigger; one sent here replaces the other set on the trigger.",
            "required": false
        },
        {
            "name": "horizon_duration",
            "example": "2d",
            "description": "Forecast length as a duration (us, ms, s, min, h, d, w, m, q, y) from the last history point, rounded up to whole steps. Exactly one of horizon_steps and horizon_duration.",
            "required": false
        },
        {
            "name": "levels",
            "example": "[0.5, 0.8]",
            "description": "JSON array of interval levels, each between 0 and 1; any level works, as bounds come from the model's samples. Default: [0.5, 0.8].",
            "required": false
        },
        {
            "name": "model",
            "example": "prophet",
            "description": "The only model is prophet, the default; any other value is refused.",
            "required": false
        },
        {
            "name": "write_results",
            "example": "true",
            "description": "Boolean: also write the forecast to target_measurement. A series that fails validation is not written. Default: false.",
            "required": false
        },
        {
            "name": "target_measurement",
            "example": "_forecasts.temperature",
            "description": "Measurement a written forecast goes to. Default: _forecasts.<source_measurement>; required when query reads several measurements.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "forecasts",
            "description": "Database a written forecast goes to. Default: the trigger's database.",
            "required": false
        },
        {
            "name": "forecast_id",
            "example": "ops-1h",
            "description": "Written to every row of a written forecast as the tag forecast_id, so forecasts of one series with different settings do not overwrite each other. Unset: no tag.",
            "required": false
        },
        {
            "name": "model_mode",
            "example": "predict",
            "description": "train fits a new in-memory model per request. predict loads the model saved for unique_suffix and the series, or trains and saves it when there is no file. Default: train.",
            "required": false
        },
        {
            "name": "unique_suffix",
            "example": "20250619_v1",
            "description": "Model version, part of the saved model's file name: up to 64 characters from letters, digits, '.', '_' and '-'. Required with model_mode predict.",
            "required": false
        },
        {
            "name": "seasonality_mode",
            "example": "additive",
            "description": "Prophet seasonality mode: additive or multiplicative. Default: additive.",
            "required": false
        },
        {
            "name": "changepoint_prior_scale",
            "example": "0.05",
            "description": "Flexibility of trend changepoints, a number above 0. Default: 0.05.",
            "required": false
        },
        {
            "name": "changepoints",
            "example": "['2025-01-01', '2025-06-01']",
            "description": "JSON array of ISO dates of trend changepoints, in UTC unless they name a timezone; each must fall within every series' training history.",
            "required": false
        },
        {
            "name": "holiday_date_list",
            "example": "['2025-07-04']",
            "description": "JSON array of ISO dates of custom holidays, in UTC unless they name a timezone, set together with holiday_names and as many.",
            "required": false
        },
        {
            "name": "holiday_names",
            "example": "['Independence Day']",
            "description": "JSON array of names matching holiday_date_list. A name Prophet reserves, such as trend, is refused.",
            "required": false
        },
        {
            "name": "holiday_country_names",
            "example": "['US']",
            "description": "JSON array with the country code for built-in holidays, refused if Prophet does not know it. Prophet supports one country, so of several only the first is used, with a warning.",
            "required": false
        },
        {
            "name": "validation_window",
            "example": "3d",
            "description": "Duration held back from training before now or end_time. A series that fails the check answers code other, without a forecast. With sampling_interval it is rounded out to whole buckets, at least one. Not with backtest_points. Unset: no validation.",
            "required": false
        },
        {
            "name": "validation_alignment",
            "example": "nearest",
            "description": "How measured and forecast points are paired: position pairs them in time order, nearest pairs each measured point with the closest forecast point within half a step. Default: position.",
            "required": false
        },
        {
            "name": "msre_threshold",
            "example": "0.05",
            "description": "Highest Mean Squared Relative Error a validated series may have, a number >= 0. Default: no limit.",
            "required": false
        },
        {
            "name": "backtest_points",
            "example": "12",
            "description": "Integer from 1 to max_forecast_points: hold out the latest N history points, train a second model on the points before them, and return each series' backtest (the share inside each interval and the mean absolute error). About doubles the training time. Not with validation_window. Unset: no backtest.",
            "required": false
        },
        {
            "name": "max_series",
            "example": "100",
            "description": "Integer >= 1: most forecasts the request may produce (series x fields); more is refused unless series_limit keeps fewer. Default: 100.",
            "required": false
        },
        {
            "name": "max_forecast_points",
            "example": "10000",
            "description": "Integer >= 1: most forecast points per series, counting the validation window; a longer horizon is refused. Default: 10000.",
            "required": false
        }
    ]
}
"""

import dataclasses
import hashlib
import json
import math
import os
import random
import re
import tempfile
import time as _time
import uuid
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from string import Template
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from influxdata_plugin_utils.config import load_config, resolve_plugin_dir
from influxdata_plugin_utils.introspection import NUMERIC_TYPES, TAG_DATA_TYPE, get_schema
from influxdata_plugin_utils.parsing import parse_bool, parse_int
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator
from influxdata_plugin_utils.write import build_line, write_data
from prophet import Prophet
from prophet.serialize import model_from_json, model_to_json

MODEL_ID = "prophet"
DEFAULT_LEVELS = (0.5, 0.8)
# Without window, point_limit reads its buckets of sampling_interval (at most a year)
# or 30 days of raw points, so a read never scans the whole table.
DEFAULT_WINDOW_NS = 30 * 24 * 3600 * 10**9
MAX_DEFAULT_WINDOW_NS = 365 * 24 * 3600 * 10**9
DEFAULT_MAX_SERIES = 100
DEFAULT_MAX_FORECAST_POINTS = 10000
# Fewest points a series needs; the manifest's min_points.
MIN_POINTS = 3

# Columns of a stored forecast row, besides lower_<level>/upper_<level>.
FORECAST_ROW_TAGS = ("field", "model", "source_measurement", "forecast_id")
FORECAST_ROW_FIELDS = ("forecast", "step", "points_read", "run_time")

HTTP_STATUS = {
    "bad_request": 400,
    "unauthorized": 401,
    "model_unavailable": 503,
    "timeout": 504,
    "internal": 500,
    "other": 400,
}

AVAILABLE_SENDERS = {
    "slack": ["slack_webhook_url", "slack_headers"],
    "discord": ["discord_webhook_url", "discord_headers"],
    "http": ["http_webhook_url", "http_headers"],
    "whatsapp": [
        "twilio_sid",
        "twilio_token",
        "twilio_to_number",
        "twilio_from_number",
    ],
    "sms": ["twilio_sid", "twilio_token", "twilio_to_number", "twilio_from_number"],
}

# Keywords to skip when validating sender args
EXCLUDED_KEYWORDS = ["headers", "token", "sid"]

MODEL_DIR_NAME = "prophet_models"
# unique_suffix becomes part of a file name, so its character set is restricted
SAFE_SUFFIX_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")

SEASONALITY_MODES = ("additive", "multiplicative")
MODEL_MODES = ("train", "predict")
VALIDATION_ALIGNMENTS = ("position", "nearest")

DEFAULT_NOTIFICATION_TEXT = (
    "Validation failed for prophet on table:$measurement, field:$field "
    "for period from $start_time to $end_time, forecast not written to "
    "table:$output_measurement. Series: $series"
)


class ForecastError(Exception):
    """A failure answered with one of the contract's error codes."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

CONTRACT_KEYS = (
    "source_measurement",
    "field",
    "query",
    "source_database",
    "tag_filter",
    "sql_filter",
    "group_by",
    "series_limit",
    "window",
    "point_limit",
    "sampling_interval",
    "horizon_steps",
    "horizon_duration",
    "levels",
    "model",
    "write_results",
    "target_measurement",
    "target_database",
    "forecast_id",
    "backtest_points",
)
PLUGIN_KEYS = (
    "forecast_freq",
    "model_mode",
    "unique_suffix",
    "seasonality_mode",
    "changepoint_prior_scale",
    "changepoints",
    "holiday_date_list",
    "holiday_names",
    "holiday_country_names",
    "validation_window",
    "validation_alignment",
    "msre_threshold",
    "max_series",
    "max_forecast_points",
)
NOTIFICATION_KEYS = (
    "is_sending_alert",
    "notification_text",
    "notification_path",
    "port_override",
    "senders",
    "influxdb3_auth_token",
    *dict.fromkeys(name for keys in AVAILABLE_SENDERS.values() for name in keys),
)
# Trigger arguments can be read from system.processing_engine_trigger_arguments, so
# these come only from the TOML file or the environment.
SECRET_KEYS = ("influxdb3_auth_token", "twilio_token")
# An absolute history is for one request; a schedule's history moves with each run.
REQUEST_NAMES = CONTRACT_KEYS + PLUGIN_KEYS + ("start_time", "end_time")
# A scheduled run always writes and returns no response: no write_results, no backtest.
SCHEDULED_NAMES = (
    tuple(name for name in CONTRACT_KEYS if name not in ("write_results", "backtest_points"))
    + PLUGIN_KEYS
    + NOTIFICATION_KEYS
)

ENV_PREFIX = "INFLUXDB3_PROPHET_FORECASTING_"
HEADER_PREFIX = "X-Influxdb3-Prophet-Forecasting-"

_DURATION_PATTERN = re.compile(r"(\d+)(us|ms|s|min|h|d|w|m|q|y)")
_UNIT_NS = {
    "us": 10**3,
    "ms": 10**6,
    "s": 10**9,
    "min": 60 * 10**9,
    "h": 3600 * 10**9,
    "d": 86400 * 10**9,
    "w": 604800 * 10**9,
    # Calendar units have no fixed length, so they are approximated.
    "m": 2628288 * 10**9,  # 30.42 d
    "q": 7884000 * 10**9,  # 91.25 d
    "y": 31536000 * 10**9,  # 365 d
}


def _text(raw) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"expected a non-empty string, got {raw!r}")
    return raw.strip()


def _items(raw) -> list[str]:
    """A list of strings, from a list or a space-separated string."""
    if isinstance(raw, str):
        items = raw.split()
    elif isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        items = [item.strip() for item in raw]
    else:
        raise ValueError(f"expected a list of names or a space-separated string, got {raw!r}")
    if not all(items):
        raise ValueError(f"names must not be empty, got {raw!r}")
    return items


def _names(raw) -> list[str]:
    return list(dict.fromkeys(_items(raw)))


def _duration_ns(raw) -> int:
    match = _DURATION_PATTERN.fullmatch(raw.strip()) if isinstance(raw, str) else None
    if not match or int(match.group(1)) == 0:
        raise ValueError(
            f"expected a whole number above 0 and a unit (us, ms, s, min, h, d, w, m, q, y), got {raw!r}"
        )
    return int(match.group(1)) * _UNIT_NS[match.group(2)]


def _duration_text(ns: int) -> str:
    """A duration in the largest unit that divides it: 86400 s -> '1d'."""
    for unit in reversed(_UNIT_NS):
        if ns % _UNIT_NS[unit] == 0:
            return f"{ns // _UNIT_NS[unit]}{unit}"
    return f"{ns}ns"


def _levels(raw) -> list[float]:
    items = raw.split() if isinstance(raw, str) else raw
    if not isinstance(items, (list, tuple)):
        raise ValueError(f"expected a list of levels, got {raw!r}")
    levels = []
    for item in items:
        if isinstance(item, bool):
            raise ValueError(f"expected a number between 0 and 1, got {item!r}")
        try:
            level = float(item)
        except (TypeError, ValueError):
            raise ValueError(f"expected a number between 0 and 1, got {item!r}") from None
        if not 0 < level < 1:
            raise ValueError(f"each level must be between 0 and 1, got {item!r}")
        levels.append(level)
    return sorted(set(levels))


def _tag_value(raw) -> str:
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)) or raw == "":
        raise ValueError(f"expected a tag value, got {raw!r}")
    return str(raw)


def _tag_filter(raw) -> dict[str, list[str]]:
    """{tag: [values]} from 'host=a region=b' or from a JSON/TOML object."""
    result: dict[str, list[str]] = {}
    if isinstance(raw, str):
        for pair in raw.split():
            tag, sep, value = pair.partition("=")
            if not sep or not tag or not value:
                raise ValueError(f"expected space-separated tag=value pairs, got {pair!r}")
            if tag in result:
                raise ValueError(
                    f"tag {tag!r} is set twice; several values per tag need the TOML "
                    f"config or the JSON body"
                )
            result[tag] = [value]
        return result
    if isinstance(raw, dict):
        for tag, value in raw.items():
            values = value if isinstance(value, list) else [value]
            if not tag or not values:
                raise ValueError(f"expected a tag and at least one value, got {tag!r}")
            result[str(tag)] = list(dict.fromkeys(_tag_value(v) for v in values))
        return result
    raise ValueError(f"expected tag=value pairs or an object, got {raw!r}")


def _model(raw) -> str:
    if raw != MODEL_ID:
        raise ValueError(f"the only model is {MODEL_ID!r}, got {raw!r}")
    return raw


def _choice(allowed: tuple[str, ...]):
    """A cast keeping a lower-cased value from a fixed set of options."""

    def cast(raw) -> str:
        value = str(raw).strip().lower()
        if value not in allowed:
            raise ValueError(f"expected one of {', '.join(allowed)}, got {raw!r}")
        return value

    return cast


def _unique_suffix(raw) -> str:
    """The model version, which becomes part of a file name."""
    suffix = str(raw).strip()
    if not SAFE_SUFFIX_PATTERN.fullmatch(suffix) or ".." in suffix:
        raise ValueError(
            f"expected up to 64 characters from letters, digits, '.', '_' and '-', got {raw!r}"
        )
    return suffix


def _number(raw) -> float:
    if isinstance(raw, bool):
        raise ValueError(f"expected a number, got {raw!r}")
    return float(raw)


def _prior_scale(raw) -> float:
    value = _number(raw)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"expected a number above 0, got {raw!r}")
    return value


def _msre_threshold(raw) -> float:
    value = _number(raw)
    if math.isnan(value) or value < 0:
        raise ValueError(f"expected a number >= 0, got {raw!r}")
    return value


def _freq(raw) -> str:
    """A pandas frequency alias that moves time forward."""
    freq = _text(raw)
    if pd.tseries.frequencies.to_offset(freq).n <= 0:
        raise ValueError(f"frequency {raw!r} does not move time forward")
    return freq


def _dates(raw) -> list[pd.Timestamp]:
    """ISO dates, from a list or a space-separated string, as naive UTC like the history."""
    dates = []
    for item in _items(raw):
        moment = datetime.fromisoformat(item)
        if moment.tzinfo is not None:
            moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
        dates.append(pd.Timestamp(moment))
    return dates


def _moment_ns(raw) -> int:
    """A timezone-aware ISO 8601 time as nanoseconds."""
    moment = datetime.fromisoformat(_text(raw))
    if moment.tzinfo is None:
        raise ValueError(f"expected a timezone, as in 2025-06-19T00:00:00Z, got {raw!r}")
    return _to_ns(moment)


SETTING_VALIDATORS = [
    Validator(
        "source_measurement",
        "field",
        "query",
        "source_database",
        "sql_filter",
        "target_measurement",
        "target_database",
        "forecast_id",
        cast=_text,
    ),
    Validator("tag_filter", cast=_tag_filter),
    Validator("group_by", default=[], cast=_names),
    Validator(
        "window", "sampling_interval", "horizon_duration", "validation_window", cast=_duration_ns
    ),
    # A series needs MIN_POINTS points, so a shorter history is refused up front.
    Validator("point_limit", cast=lambda raw: parse_int(raw, minimum=MIN_POINTS)),
    Validator(
        "horizon_steps", "series_limit", "backtest_points", cast=lambda raw: parse_int(raw, minimum=1)
    ),
    Validator(
        "max_series", default=DEFAULT_MAX_SERIES, cast=lambda raw: parse_int(raw, minimum=1)
    ),
    Validator(
        "max_forecast_points",
        default=DEFAULT_MAX_FORECAST_POINTS,
        cast=lambda raw: parse_int(raw, minimum=1),
    ),
    Validator("levels", default=list(DEFAULT_LEVELS), cast=_levels),
    Validator("model", default=MODEL_ID, cast=_model),
    Validator("write_results", "is_sending_alert", default=False, cast=parse_bool),
    Validator("forecast_freq", cast=_freq),
    Validator("model_mode", default="train", cast=_choice(MODEL_MODES)),
    Validator("unique_suffix", cast=_unique_suffix),
    Validator("seasonality_mode", default="additive", cast=_choice(SEASONALITY_MODES)),
    Validator("changepoint_prior_scale", default=0.05, cast=_prior_scale),
    Validator("changepoints", "holiday_date_list", default=[], cast=_dates),
    # Paired with holiday_date_list one to one, so a holiday may repeat.
    Validator("holiday_names", default=[], cast=_items),
    Validator("holiday_country_names", default=[], cast=_names),
    Validator(
        "validation_alignment", default="position", cast=_choice(VALIDATION_ALIGNMENTS)
    ),
    Validator("msre_threshold", default=math.inf, cast=_msre_threshold),
    Validator("start_time", "end_time", cast=_moment_ns),
    Validator("notification_text", default=DEFAULT_NOTIFICATION_TEXT, cast=_text),
    Validator("notification_path", default="notify", cast=_text),
    Validator(
        "port_override", default=8181, cast=lambda raw: parse_int(raw, minimum=1, maximum=65535)
    ),
]


def _env_spec(names) -> KeySpec:
    """Read settings from INFLUXDB3_PROPHET_FORECASTING_<KEY>."""
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


# The variable is spelled in upper case; the config key it becomes is not.
AUTH_TOKEN_ENV = KeySpec(
    allowlist=["INFLUXDB3_AUTH_TOKEN"],
    rename={"INFLUXDB3_AUTH_TOKEN": "influxdb3_auth_token"},
)


def _header_name(name: str) -> str:
    return HEADER_PREFIX + name.replace("_", "-")


HEADER_KEYS = KeySpec(
    allowlist=tuple(_header_name(name) for name in REQUEST_NAMES),
    rename={_header_name(name): name for name in REQUEST_NAMES},
)
REQUEST_KEYS = KeySpec(allowlist=REQUEST_NAMES, unknown="reject")


def _refuse_unknown_headers(request_headers) -> None:
    """A header with the plugin's prefix must name a setting, or a typo would be lost."""
    known = {_header_name(name).lower() for name in REQUEST_NAMES}
    prefix = HEADER_PREFIX.lower()
    for name in request_headers or {}:
        if str(name).lower().startswith(prefix) and str(name).lower() not in known:
            raise ForecastError(
                "bad_request",
                f"unknown header {name!r}; a setting goes in {HEADER_PREFIX}<key>, "
                f"with hyphens for underscores",
            )


# Choices between keys: the side the highest source sets wins, and the other
# side from lower sources is dropped, so a request overrides a trigger default.
EXCLUSIVE_KEYS = (
    (
        ("query",),
        ("source_measurement", "field", "tag_filter", "sql_filter", "group_by"),
    ),
    (("horizon_steps",), ("horizon_duration",)),
    (("start_time", "end_time"), ("window", "point_limit")),
    (("forecast_freq",), ("sampling_interval",)),
)


def _given(value) -> bool:
    """Whether a raw value counts as set; empty text, lists and objects do not."""
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict)):
        return bool(value)
    return value is not None


def _drop_overridden(layers: list) -> list:
    """Keep, for each choice in EXCLUSIVE_KEYS, only the side set highest."""
    layers = [dict(layer) for layer in layers]
    for first, second in EXCLUSIVE_KEYS:
        tops = [
            max(
                (i for i, layer in enumerate(layers) if any(_given(layer.get(k)) for k in side)),
                default=-1,
            )
            for side in (first, second)
        ]
        if tops[0] == tops[1]:
            continue  # neither side, or both in one source: _resolve decides
        loser = second if tops[0] > tops[1] else first
        for layer in layers:
            for key in loser:
                layer.pop(key, None)
    return layers


def _load_settings(args, names, request_layers=()) -> dict:
    """Merge env, trigger args, TOML and, on demand, the request; later wins."""
    args = args or {}
    for key in SECRET_KEYS:
        if key in names and key in args:
            raise ValueError(
                f"{key} cannot be a trigger argument, as those can be read from a system "
                f"table; set it in the TOML file or in {ENV_PREFIX}{key.upper()}"
            )
    config_file_path = args.get("config_file_path") or parse_env(
        _env_spec(("config_file_path",))
    ).get("config_file_path")
    layers = [
        parse_env(_env_spec(names)),
        parse_trigger_args(
            args, KeySpec(allowlist=names + ("config_file_path",), unknown="reject")
        ),
        parse_toml(config_file_path, KeySpec(allowlist=names, unknown="reject")),
        *request_layers,
    ]
    if "influxdb3_auth_token" in names:
        layers.insert(0, parse_env(AUTH_TOKEN_ENV))
    return load_config(*_drop_overridden(layers), validators=SETTING_VALIDATORS)


@dataclasses.dataclass
class ForecastRequest:
    """A validated request: the keys after every cross-key check."""

    source_measurement: str | None
    field: str | None
    query: str | None
    source_database: str | None
    tag_filter: dict
    sql_filter: str | None
    group_by: list
    series_limit: int | None
    window_ns: int | None
    default_window: bool
    point_limit: int | None
    start_ns: int | None
    end_ns: int | None
    sampling_ns: int | None
    forecast_freq: str | None
    horizon_steps: int | None
    horizon_ns: int | None
    backtest_points: int | None
    levels: list
    model: str
    write_results: bool
    target_measurement: str | None
    target_database: str | None
    forecast_id: str | None
    max_series: int
    max_forecast_points: int
    model_mode: str
    unique_suffix: str | None
    seasonality_mode: str
    changepoint_prior_scale: float
    changepoints: list
    holiday_dates: list
    holiday_names: list
    holiday_country_names: list
    validation_ns: int | None
    validation_alignment: str
    msre_threshold: float
    now_ns: int
    # None only for a query without window.
    history_start_ns: int | None
    history_end_ns: int
    # Where training stops and validation starts; None without validation.
    cut_ns: int | None
    # The measurement the forecast comes from, once a query's plan is read.
    source: str | None = None


def _forecast_row_columns(levels: list) -> set:
    """Tag and field names of a stored forecast row."""
    names = {*FORECAST_ROW_TAGS, *FORECAST_ROW_FIELDS}
    for level in levels:
        key = _level_key(level)
        names.update((f"lower_{key}", f"upper_{key}"))
    return names


def _refuse_clashing_keys(keys: list, levels: list, kind: str) -> None:
    """Series keys are written as tags, so one named like a row column is refused."""
    columns = _forecast_row_columns(levels)
    for key in keys:
        if key in columns:
            raise ForecastError(
                "bad_request",
                f"{kind} {key!r} clashes with a column of the stored forecast rows; "
                f"rename it with an alias in a query",
            )


def _check_horizon(req: ForecastRequest, points: int) -> None:
    if points > req.max_forecast_points:
        raise ForecastError(
            "bad_request",
            f"the horizon is {points} points, more than max_forecast_points="
            f"{req.max_forecast_points}; shorten it or raise max_forecast_points",
        )


def _resolve(cfg: dict, scheduled: bool, now_ns: int) -> ForecastRequest:
    """Check the keys against each other and build the request."""
    query_mode = "query" in cfg
    if query_mode:
        # These first: set with query in one source, they keep lower sources'
        # source_measurement and field, which would be named instead.
        for key in ("tag_filter", "sql_filter", "group_by", "start_time", "end_time"):
            if cfg.get(key):
                raise ForecastError("bad_request", f"{key} does not apply to query")
        if "source_measurement" in cfg or "field" in cfg:
            raise ForecastError(
                "bad_request", "set either query or source_measurement and field, not both"
            )
    elif "source_measurement" not in cfg or "field" not in cfg:
        raise ForecastError(
            "bad_request", "source_measurement and field are required, or query"
        )

    if ("horizon_steps" in cfg) == ("horizon_duration" in cfg):
        raise ForecastError(
            "bad_request", "set exactly one of horizon_steps and horizon_duration"
        )
    if "sampling_interval" in cfg and "forecast_freq" in cfg:
        raise ForecastError(
            "bad_request", "set one of sampling_interval and forecast_freq, not both"
        )
    if cfg["model_mode"] == "predict" and "unique_suffix" not in cfg:
        raise ForecastError("bad_request", "model_mode predict requires unique_suffix")
    if len(cfg["holiday_date_list"]) != len(cfg["holiday_names"]):
        raise ForecastError(
            "bad_request",
            f"holiday_date_list and holiday_names must come together and be as many, got "
            f"{len(cfg['holiday_date_list'])} and {len(cfg['holiday_names'])}",
        )
    write_results = True if scheduled else cfg["write_results"]
    if write_results:
        _refuse_clashing_keys(cfg["group_by"], cfg["levels"], "group_by tag")

    window_ns = cfg.get("window")
    point_limit = cfg.get("point_limit")
    start_ns, end_ns = cfg.get("start_time"), cfg.get("end_time")
    absolute = start_ns is not None or end_ns is not None
    if absolute:
        if window_ns is not None or point_limit is not None:
            raise ForecastError(
                "bad_request", "set start_time and end_time, or window and point_limit, not both"
            )
        if start_ns is None or end_ns is None:
            raise ForecastError("bad_request", "set start_time and end_time together")
        if start_ns >= end_ns:
            raise ForecastError("bad_request", "start_time must be earlier than end_time")
    elif not query_mode and window_ns is None and point_limit is None:
        options = "window or point_limit" if scheduled else (
            "window, point_limit, or start_time and end_time"
        )
        raise ForecastError("bad_request", f"set the history to read: {options}")

    sampling_ns = cfg.get("sampling_interval")
    validation_ns = cfg.get("validation_window")
    backtest_points = cfg.get("backtest_points")
    if backtest_points and validation_ns:
        # Both hold back the latest points, and the windows would overlap.
        raise ForecastError(
            "bad_request", "set one of backtest_points and validation_window, not both"
        )
    # History ends now or at end_time; with sampling_interval at the edge of the last
    # full bucket, so a bucket still filling or cut short never reaches the model.
    history_end_ns = end_ns if absolute else now_ns
    if sampling_ns:
        history_end_ns = min(history_end_ns, now_ns)
        history_end_ns -= history_end_ns % sampling_ns
    cut_ns = None
    if validation_ns:
        cut_ns = history_end_ns - validation_ns
        if sampling_ns:
            # On a bucket's edge, so no bucket holds points from both sides of the cut.
            cut_ns -= cut_ns % sampling_ns
    default_window = not query_mode and not absolute and window_ns is None
    if default_window:
        # The backtest's held-out buckets come on top, and one more: the window counts
        # from now, and the history ends at the last full bucket.
        buckets = point_limit + (backtest_points or 0) + 1
        window_ns = (
            min(buckets * sampling_ns, MAX_DEFAULT_WINDOW_NS)
            if sampling_ns
            else DEFAULT_WINDOW_NS
        ) + (validation_ns or 0)
    history_start_ns = start_ns if absolute else now_ns - window_ns if window_ns else None
    # The server refuses a time range that holds nothing, so it must never get one.
    if window_ns and sampling_ns and window_ns < sampling_ns:
        raise ForecastError(
            "bad_request",
            f"the history window ({_duration_text(window_ns)}) is shorter than one "
            f"sampling_interval ({_duration_text(sampling_ns)})",
        )
    if absolute and start_ns >= history_end_ns:
        raise ForecastError(
            "bad_request",
            f"no full sampling_interval between start_time and the end of the history "
            f"({_iso(history_end_ns)})",
        )
    if cut_ns is not None and history_start_ns is not None and cut_ns <= history_start_ns:
        raise ForecastError("bad_request", "validation_window leaves no history to train on")

    req = ForecastRequest(
        source_measurement=cfg.get("source_measurement"),
        field=cfg.get("field"),
        query=cfg.get("query"),
        source_database=cfg.get("source_database"),
        tag_filter=cfg.get("tag_filter") or {},
        sql_filter=cfg.get("sql_filter"),
        group_by=cfg["group_by"],
        series_limit=cfg.get("series_limit"),
        window_ns=window_ns,
        default_window=default_window,
        point_limit=point_limit,
        start_ns=start_ns,
        end_ns=end_ns,
        sampling_ns=sampling_ns,
        forecast_freq=cfg.get("forecast_freq"),
        horizon_steps=cfg.get("horizon_steps"),
        horizon_ns=cfg.get("horizon_duration"),
        backtest_points=backtest_points,
        levels=cfg["levels"],
        model=cfg["model"],
        write_results=write_results,
        target_measurement=cfg.get("target_measurement"),
        target_database=cfg.get("target_database"),
        forecast_id=cfg.get("forecast_id"),
        max_series=cfg["max_series"],
        max_forecast_points=cfg["max_forecast_points"],
        model_mode=cfg["model_mode"],
        unique_suffix=cfg.get("unique_suffix"),
        seasonality_mode=cfg["seasonality_mode"],
        changepoint_prior_scale=cfg["changepoint_prior_scale"],
        changepoints=cfg["changepoints"],
        holiday_dates=cfg["holiday_date_list"],
        holiday_names=cfg["holiday_names"],
        holiday_country_names=cfg["holiday_country_names"],
        validation_ns=validation_ns,
        validation_alignment=cfg["validation_alignment"],
        msre_threshold=cfg["msre_threshold"],
        now_ns=now_ns,
        history_start_ns=history_start_ns,
        history_end_ns=history_end_ns,
        cut_ns=cut_ns,
    )
    # The horizon in points is known now unless the spacing comes from the data.
    if req.horizon_steps:
        _check_horizon(req, req.horizon_steps)
    elif req.sampling_ns:
        _check_horizon(req, -(-req.horizon_ns // req.sampling_ns))
    if backtest_points and backtest_points > req.max_forecast_points:
        raise ForecastError(
            "bad_request",
            f"backtest_points is {backtest_points}, more than max_forecast_points="
            f"{req.max_forecast_points}; lower it or raise max_forecast_points",
        )
    # Within the cap, the limit always holds, so the cap never refuses a limited request.
    if req.series_limit and req.series_limit > req.max_series:
        raise ForecastError(
            "bad_request",
            f"series_limit is {req.series_limit}, more than max_series={req.max_series}; "
            f"lower it or raise max_series",
        )
    if req.holiday_dates or req.holiday_country_names:
        # A reserved name or an unknown country would otherwise fail every series.
        try:
            _new_model(req)
        except Exception as exc:
            raise ForecastError("bad_request", f"invalid holidays: {exc}") from exc
    return req


# ---------------------------------------------------------------------------
# Reading history
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Series:
    """One forecast input: a field of one series, oldest point first."""

    field: str
    tags: dict
    times: list = dataclasses.field(default_factory=list)  # int ns, the training history
    values: list = dataclasses.field(default_factory=list)  # float
    actual_times: list = dataclasses.field(default_factory=list)  # the validation window's
    actual_values: list = dataclasses.field(default_factory=list)
    key: dict = dataclasses.field(default_factory=dict)  # group_by tags or query key columns
    error: tuple | None = None  # (code, message)


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _run_query(influxdb3_local, sql: str, params: dict | None, database: str | None):
    kwargs = {"database": database} if database else {}
    if params:
        return influxdb3_local.query(sql, params, **kwargs)
    return influxdb3_local.query(sql, **kwargs)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _to_ns(value) -> int | None:
    """A query timestamp as integer nanoseconds."""
    if value is None or isinstance(value, int):
        return value
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    # Integer arithmetic: a float timestamp is off by up to a few hundred ns.
    return (value - _EPOCH) // timedelta(microseconds=1) * 1000


def _iso(ns: int) -> str:
    """Nanoseconds as an ISO 8601 UTC time, for a query parameter or a message."""
    return (_EPOCH + timedelta(microseconds=ns // 1000)).isoformat()


def _interval_sql(ns: int) -> str:
    """A duration as a SQL interval: in seconds when whole, as most are, else in nanoseconds."""
    if ns % 10**9 == 0:
        return f"INTERVAL '{ns // 10**9} seconds'"
    return f"INTERVAL '{ns} nanoseconds'"


def _numeric(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _too_many(cap: int) -> ForecastError:
    return ForecastError(
        "bad_request",
        f"the request produces more forecasts than max_series={cap}; "
        f"narrow it, set series_limit, or raise max_series",
    )


def _read_limit(req: ForecastRequest) -> int | None:
    """Points a series keeps: point_limit and the backtest's held-out ones; None for all."""
    return req.point_limit + (req.backtest_points or 0) if req.point_limit else None


def _measurement_tags(influxdb3_local, req: ForecastRequest) -> list[str]:
    """Check the named columns against the schema and return the tag names."""
    try:
        schema = get_schema(
            influxdb3_local,
            req.source_measurement,
            database=req.source_database,
            use_cache=False,
        )
    except Exception as exc:
        raise ForecastError(
            "bad_request", f"cannot read measurement {req.source_measurement!r}: {exc}"
        ) from exc
    tags = [name for name, data_type in schema.items() if data_type == TAG_DATA_TYPE]
    numeric = {name for name, data_type in schema.items() if data_type in NUMERIC_TYPES}
    if req.field not in numeric:
        raise ForecastError(
            "bad_request",
            f"{req.field!r} is not a numeric field of {req.source_measurement!r}",
        )
    for name in [*req.group_by, *req.tag_filter]:
        if name not in tags:
            raise ForecastError(
                "bad_request", f"{name!r} is not a tag of {req.source_measurement!r}"
            )
    return tags


def _where(req: ForecastRequest, params: dict) -> str:
    """The WHERE clause shared by the series and the data queries."""
    clauses = [f"{_quote_ident(req.field)} IS NOT NULL"]
    params["history_start"] = _iso(req.history_start_ns)
    params["history_end"] = _iso(req.history_end_ns)
    clauses.append("time >= $history_start AND time < $history_end")
    for i, (tag, values) in enumerate(req.tag_filter.items()):
        placeholders = []
        for j, value in enumerate(values):
            params[f"tf{i}_{j}"] = value
            placeholders.append(f"$tf{i}_{j}")
        if len(placeholders) == 1:
            clauses.append(f"{_quote_ident(tag)} = {placeholders[0]}")
        else:
            clauses.append(f"{_quote_ident(tag)} IN ({', '.join(placeholders)})")
    if req.sql_filter:
        # Inserted as is, by design: the README states what it can read.
        clauses.append(f"({req.sql_filter})")
    return " AND ".join(clauses)


def _series_keys(influxdb3_local, req, tags, where, params) -> tuple[list, int]:
    """Find the series, their single-valued tags and row counts, and enforce the cap.

    Returns:
        [(group values, tags, rows)] in the contract's order (group_by values, a missing
        one last), at most series_limit of them, and how many series there are in all.
        Without group_by there is always one series, empty when no row matches; with
        it, no row means no series.
    """
    others = [tag for tag in tags if tag not in req.group_by]
    columns = [f"{_quote_ident(g)} AS __g{i}" for i, g in enumerate(req.group_by)]
    for i, tag in enumerate(others):
        text = f"CAST({_quote_ident(tag)} AS VARCHAR)"
        columns.append(f"count({text}) AS __c{i}")
        columns.append(f"count(DISTINCT {text}) AS __n{i}")
        columns.append(f"min({text}) AS __v{i}")
    columns.append("count(*) AS __rows")
    limited = bool(req.group_by and req.series_limit)
    if limited:
        # Counts every group before LIMIT, so only series_limit rows come back.
        columns.append("count(*) OVER () AS __total")
    sql = f"SELECT {', '.join(columns)} FROM {_quote_ident(req.source_measurement)} WHERE {where}"
    if req.group_by:
        positions = ", ".join(str(i + 1) for i in range(len(req.group_by)))
        order = ", ".join(f"{i + 1} ASC NULLS LAST" for i in range(len(req.group_by)))
        limit = req.series_limit or req.max_series + 1
        sql += f" GROUP BY {positions} ORDER BY {order} LIMIT {limit}"
    try:
        rows = _run_query(influxdb3_local, sql, params, req.source_database)
    except Exception as exc:
        # The first query to run the filters, so a bad sql_filter fails here.
        raise ForecastError("bad_request", f"cannot read the series: {exc}") from exc
    rows = [row for row in rows if row.get("__rows")]
    if not req.group_by and not rows:
        return [((), {}, 0)], 1
    if len(rows) > req.max_series:
        raise _too_many(req.max_series)
    total = rows[0]["__total"] if limited and rows else len(rows)

    keys = []
    for row in rows:
        group = tuple(row.get(f"__g{i}") for i in range(len(req.group_by)))
        series_tags = {
            g: str(value) for g, value in zip(req.group_by, group) if value not in (None, "")
        }
        for i, tag in enumerate(others):
            # A series tag is on every row with one value; partial tags are left out.
            if (
                row.get(f"__c{i}") == row.get("__rows")
                and row.get(f"__n{i}") == 1
                and row.get(f"__v{i}") not in (None, "")
            ):
                series_tags[tag] = str(row[f"__v{i}"])
        keys.append((group, series_tags, row["__rows"]))
    return keys, total


def _chosen_series(req: ForecastRequest, keys: list, params: dict) -> str:
    """A condition matching only the rows of these series, the ones series_limit keeps."""
    alternatives = []
    for i, (group, _, _) in enumerate(keys):
        parts = []
        for j, (tag, value) in enumerate(zip(req.group_by, group)):
            if value is None:
                parts.append(f"{_quote_ident(tag)} IS NULL")
            else:
                params[f"sl{i}_{j}"] = value
                parts.append(f"{_quote_ident(tag)} = $sl{i}_{j}")
        alternatives.append(f"({' AND '.join(parts)})")
    return f"({' OR '.join(alternatives)})"


# Tag sets read to check one request for mixed series; more than this counts as mixed.
_MAX_TAG_SETS = 10_000


def _refuse_mixed_series(influxdb3_local, req, tags, where, params) -> None:
    """Refuse a series whose rows come from tag sets writing at the same time.

    Interleaved, they form a saw-tooth; a tag set that follows another in time
    (a version after an upgrade) is fine. Only the history the model trains on counts:
    the last point_limit timestamps of each series before the cut; with window alone,
    all of it.
    """
    others = [tag for tag in tags if tag not in req.group_by]
    if not others:
        return
    groups = [f"__g{i}" for i in range(len(req.group_by))]
    keys = ", ".join(groups + [f"__o{i}" for i in range(len(others))])
    columns = [f"{_quote_ident(g)} AS __g{i}" for i, g in enumerate(req.group_by)]
    columns += [f"{_quote_ident(tag)} AS __o{i}" for i, tag in enumerate(others)]
    rows_sql = (
        f"SELECT {', '.join(columns)}, time AS __time "
        f"FROM {_quote_ident(req.source_measurement)} WHERE {where}"
    )
    if req.point_limit:
        partition = f"PARTITION BY {', '.join(groups)} " if groups else ""
        rows_sql = (
            f"SELECT * FROM (SELECT *, DENSE_RANK() OVER ({partition}ORDER BY __time DESC) "
            f"AS __rank FROM ({rows_sql}) AS __rows) AS __ranked "
            f"WHERE __rank <= {_read_limit(req)}"
        )
    sql = (
        f"SELECT {keys}, min(__time) AS __first, max(__time) AS __last "
        f"FROM ({rows_sql}) AS __recent GROUP BY {keys} LIMIT {_MAX_TAG_SETS + 1}"
    )
    try:
        rows = _run_query(influxdb3_local, sql, params, req.source_database)
    except Exception as exc:
        raise ForecastError("bad_request", f"cannot read the series: {exc}") from exc
    if len(rows) > _MAX_TAG_SETS:
        raise ForecastError(
            "bad_request",
            f"the rows come from more than {_MAX_TAG_SETS} tag sets; narrow them with "
            f"group_by or tag_filter, or set sampling_interval",
        )

    spans: dict[tuple, list] = {}  # group -> [(first, last, tag values)]
    for row in rows:
        group = tuple(row.get(f"__g{i}") for i in range(len(req.group_by)))
        tag_values = tuple(row.get(f"__o{i}") for i in range(len(others)))
        spans.setdefault(group, []).append(
            (_to_ns(row["__first"]) or 0, _to_ns(row["__last"]) or 0, tag_values)
        )
    for group, tag_sets in spans.items():
        latest = None  # the tag set that ends last so far
        for span in sorted(tag_sets, key=lambda span: span[0]):
            if latest and span[0] < latest[1]:
                differ = [tag for tag, a, b in zip(others, span[2], latest[2]) if a != b]
                series = f"the series {dict(zip(req.group_by, group))}" if req.group_by else "the series"
                raise ForecastError(
                    "bad_request",
                    f"rows of {series} come from several tag sets writing at the same time "
                    f"(they differ in {', '.join(differ)}); add those tags to group_by, pin "
                    f"them with tag_filter, or set sampling_interval to forecast their mean",
                )
            if latest is None or span[1] > latest[1]:
                latest = span


def _data_sql(req: ForecastRequest, where: str, limit: int | None) -> tuple[str, bool]:
    """The history query, at most limit points per series; the flag says rows come newest first.

    Rows of a series sharing a timestamp, or a sampling bucket, are averaged.
    """
    groups = [f"{_quote_ident(g)} AS __g{i}" for i, g in enumerate(req.group_by)]
    table = _quote_ident(req.source_measurement)
    moment = "time"
    if req.sampling_ns:
        moment = f"date_bin({_interval_sql(req.sampling_ns)}, time)"
    select = [f"{moment} AS __t", *groups, f"avg({_quote_ident(req.field)}) AS __f0"]
    positions = ", ".join(str(i + 1) for i in range(1 + len(groups)))
    sql = f"SELECT {', '.join(select)} FROM {table} WHERE {where} GROUP BY {positions}"

    if limit is None:
        return f"{sql} ORDER BY __t", False
    if not req.group_by:
        return f"{sql} ORDER BY __t DESC LIMIT {limit}", True
    partition = ", ".join(f"__g{i}" for i in range(len(req.group_by)))
    ranked = (
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY __t DESC) "
        f"AS __rn FROM ({sql}) AS __history"
    )
    return (
        f"SELECT * FROM ({ranked}) AS __ranked WHERE __rn <= {limit} ORDER BY __t",
        False,
    )


def _measurement_plan(influxdb3_local, req: ForecastRequest) -> tuple:
    """Mode A checks: the schema, the filters, the series cap, mixed series.

    Returns:
        (WHERE clause, its parameters, series keys from _series_keys, series in all).
    """
    tags = _measurement_tags(influxdb3_local, req)
    params: dict = {}
    where = _where(req, params)
    keys, total = _series_keys(influxdb3_local, req, tags, where, params)
    if len(keys) < total:
        # series_limit left series out: they are neither checked nor read.
        where = f"{where} AND {_chosen_series(req, keys, params)}"
    if not req.sampling_ns and any(rows for _, _, rows in keys):
        # With sampling_interval, a mixed series is the mean per bucket the request asks for.
        _refuse_mixed_series(influxdb3_local, req, tags, *_before_cut(req, where, params))
    return where, params, keys, total


def _before_cut(req: ForecastRequest, where: str, params: dict) -> tuple[str, dict]:
    """The condition and parameters of the history the model trains on."""
    if req.cut_ns is None:
        return where, params
    return f"{where} AND time < $cut", {**params, "cut": _iso(req.cut_ns)}


def _read_points(influxdb3_local, req, where, params, limit) -> dict:
    """{group values: [(time, value)]}, oldest first."""
    sql, newest_first = _data_sql(req, where, limit)
    try:
        rows = _run_query(influxdb3_local, sql, params, req.source_database)
    except Exception as exc:
        raise ForecastError("internal", f"history query failed: {exc}") from exc
    if newest_first:
        rows = list(reversed(rows))
    points: dict = {}
    for row in rows:
        value = row.get("__f0")
        moment = _to_ns(row.get("__t"))
        if value is None or moment is None:
            continue
        group = tuple(row.get(f"__g{i}") for i in range(len(req.group_by)))
        points.setdefault(group, []).append((moment, float(value)))
    return points


def _read_measurement(influxdb3_local, req: ForecastRequest, plan: tuple) -> list[Series]:
    """Mode A: one series per group_by combination, or one in all."""
    where, params, keys, _ = plan
    history_where, history_params = _before_cut(req, where, params)
    actual: dict = {}
    if req.cut_ns is not None:
        actual = _read_points(
            influxdb3_local, req, f"{where} AND time >= $cut", history_params, None
        )
    history = _read_points(influxdb3_local, req, history_where, history_params, _read_limit(req))

    series_list = []
    for group, series_tags, _ in keys:
        series = Series(field=req.field, tags=series_tags, key=dict(zip(req.group_by, group)))
        series.times = [moment for moment, _ in history.get(group, [])]
        series.values = [value for _, value in history.get(group, [])]
        series.actual_times = [moment for moment, _ in actual.get(group, [])]
        series.actual_values = [value for _, value in actual.get(group, [])]
        series_list.append(series)
    return series_list


# EXPLAIN names each scanned table unquoted, then its attributes:
# "TableScan: cpu load projection=[...], full_filters=[...]".
_TABLE_SCAN = re.compile(
    r"TableScan: (.+?)(?= projection=\[|, (?:partial_filters|full_filters|unsupported_filters|fetch)=|$)",
    re.MULTILINE,
)


def _query_measurements(influxdb3_local, req: ForecastRequest) -> list[str]:
    """The measurements a query reads, from its logical plan."""
    try:
        rows = _run_query(influxdb3_local, f"EXPLAIN {req.query}", None, req.source_database)
    except Exception as exc:
        raise ForecastError("bad_request", f"the query does not plan: {exc}") from exc
    names = set()
    for row in rows:
        if row.get("plan_type") != "logical_plan":
            continue
        for name in _TABLE_SCAN.findall(str(row.get("plan", ""))):
            for prefix in ("public.iox.", "iox."):
                if name.startswith(prefix):
                    name = name[len(prefix):]
                    break
            names.add(name)
    return sorted(names)


def _windowed_query(query: str, window_ns: int) -> str:
    """The query wrapped in a window filter the server applies.

    UTC compares a time zone aware time (tz()) by instant; the line breaks keep a
    trailing -- comment from swallowing the closing parenthesis.
    """
    body = query.strip().rstrip(";").rstrip()
    return (
        f"SELECT * FROM (\n{body}\n) AS __history WHERE time > "
        f"(now() AT TIME ZONE 'UTC') - {_interval_sql(window_ns)}"
    )


def _query_rows(influxdb3_local, req: ForecastRequest, task_id: str) -> list:
    """The query's rows; with window, filtered by the server when the query can be wrapped."""
    if req.window_ns:
        try:
            return _run_query(
                influxdb3_local,
                _windowed_query(req.query, req.window_ns),
                None,
                req.source_database,
            )
        except Exception as exc:
            if "Resources exhausted" in str(exc):
                # Unwrapped, the query would read more and fail the same way.
                raise ForecastError("internal", f"the query failed: {exc}") from exc
            # For example an integer time column; _read_query applies the window then.
            influxdb3_local.warn(
                f"[{task_id}] Window applied in the plugin, the wrapped query failed: {exc}"
            )
    try:
        return _run_query(influxdb3_local, req.query, None, req.source_database)
    except Exception as exc:
        raise ForecastError("internal", f"the query failed: {exc}") from exc


def _read_query(influxdb3_local, req: ForecastRequest, task_id: str) -> tuple[list[Series], int]:
    """Mode B: each key combination × each numeric column is a series.

    Returns:
        The series in the contract's order, at most series_limit of them, and how many
        there are in all.
    """
    rows = _query_rows(influxdb3_local, req, task_id)
    if rows and not any("time" in row for row in rows):
        raise ForecastError(
            "bad_request", "no time column in query result; alias the timestamp AS time"
        )
    if req.window_ns:
        # Repeats the server's filter, or applies it when the query could not be wrapped;
        # before the split, so a series with no rows in the window does not exist.
        rows = [row for row in rows if (_to_ns(row.get("time")) or 0) >= req.history_start_ns]
    if req.sampling_ns:
        # The query defines its history, so only the buckets of sampling_interval end it.
        rows = [row for row in rows if (_to_ns(row.get("time")) or 0) < req.history_end_ns]
    if not rows:
        return [], 0
    columns = list(dict.fromkeys(column for row in rows for column in row))

    keys, values = [], []
    for column in columns:
        if column == "time":
            continue
        present = [row.get(column) for row in rows if row.get(column) is not None]
        if not present:
            continue
        (values if all(_numeric(v) for v in present) else keys).append(column)
    if not values:
        raise ForecastError("bad_request", "no numeric column to forecast in query result")
    if req.write_results:
        _refuse_clashing_keys(keys, req.levels, "query column")

    groups: dict[tuple, list] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(k) for k in keys), []).append(row)
    # The contract's order: key values as text, a missing value last, then field name.
    forecasts = sorted(
        ((group, column) for group in groups for column in values),
        key=lambda pair: (tuple((v is None, str(v)) for v in pair[0]), pair[1]),
    )
    total = len(forecasts)
    if req.series_limit:
        forecasts = forecasts[: req.series_limit]
    elif total > req.max_series:
        raise _too_many(req.max_series)

    series_list = []
    for group, column in forecasts:
        series_tags = {k: str(v) for k, v in zip(keys, group) if v not in (None, "")}
        series = Series(field=column, tags=series_tags, key=dict(zip(keys, group)))
        _mean_per_moment(
            series,
            (
                (moment, float(row[column]))
                for row in groups[group]
                if row.get(column) is not None
                and (moment := _to_ns(row.get("time"))) is not None
            ),
            req.sampling_ns,
        )
        if req.cut_ns is not None:
            split = bisect_left(series.times, req.cut_ns)
            series.actual_times, series.actual_values = series.times[split:], series.values[split:]
            series.times, series.values = series.times[:split], series.values[:split]
        limit = _read_limit(req)
        if limit:
            series.times, series.values = series.times[-limit:], series.values[-limit:]
        series_list.append(series)
    return series_list, total


def _mean_per_moment(series: Series, points, step_ns: int | None) -> None:
    """The series, sorted: the mean per timestamp, or per bucket aligned to the epoch like date_bin."""
    sums: dict[int, list] = {}
    for moment, value in points:
        total = sums.setdefault(moment - moment % step_ns if step_ns else moment, [0.0, 0])
        total[0] += value
        total[1] += 1
    series.times = sorted(sums)
    series.values = [sums[t][0] / sums[t][1] for t in series.times]


# ---------------------------------------------------------------------------
# The forecast grid
# ---------------------------------------------------------------------------


def _median_step_ns(times: list) -> int:
    """The median gap between consecutive timestamps; a series has at least MIN_POINTS."""
    gaps = sorted(b - a for a, b in zip(times, times[1:]))
    middle = len(gaps) // 2
    return gaps[middle] if len(gaps) % 2 else (gaps[middle - 1] + gaps[middle]) // 2


# Round units for a raw-point grid: its step is rounded to one about 1% of it,
# its phase to one about 10% (10 ms and 1 s for a 10 s step, 1 h for a week).
_ROUND_UNITS_NS = tuple(
    n * 10**6 for n in (1, 10, 100, 1_000, 10_000, 60_000, 600_000, 3_600_000)
)


def _round_unit(step: int, share: int) -> int:
    """The largest round unit at most step / share, or 1 ns."""
    return max((unit for unit in _ROUND_UNITS_NS if share * unit <= step), default=1)


def _round_step(gap_ns: int) -> int:
    """The median gap rounded to a unit about 1% of it, so jitter does not change it."""
    unit = _round_unit(gap_ns, 100)
    return (gap_ns + unit // 2) // unit * unit


def _grid_anchor(times: list, step: int) -> int:
    """A cell of the raw-point grid, on the rhythm most points keep.

    The last point moved by the median offset of all points, so a stray timestamp
    does not shift the grid; its phase is rounded to about a tenth of the step, so
    jitter does not move the grid between runs.
    """
    last = times[-1]
    half = step // 2
    offsets = sorted((moment - last + half) % step - half for moment in times)
    anchor = last + offsets[len(offsets) // 2]
    unit = _round_unit(step, 10)
    phase = anchor % step
    return anchor - phase + (phase + unit // 2) // unit * unit


def _timestamps(times: list) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(np.array(times, dtype="datetime64[ns]"))


def _forecast_step(series: Series, req: ForecastRequest) -> tuple:
    """The forecast's step as a pandas frequency, and its first point after the history."""
    last = series.times[-1]
    if req.sampling_ns:
        return pd.Timedelta(req.sampling_ns, "ns"), pd.Timestamp(last + req.sampling_ns, unit="ns")
    freq = req.forecast_freq or pd.infer_freq(_timestamps(series.times))
    if freq:
        offset = pd.tseries.frequencies.to_offset(freq)
        return offset, pd.Timestamp(last, unit="ns") + offset
    # Irregular or jittered points: the grid they keep, so a later run hits the same times.
    step = _round_step(_median_step_ns(series.times))
    anchor = _grid_anchor(series.times, step)
    cell = anchor - (anchor - last + step // 2) // step * step
    return pd.Timedelta(step, "ns"), pd.Timestamp(cell + step, unit="ns")


def _fixed_ns(offset) -> int | None:
    """One step in nanoseconds, or None for a calendar frequency (month, business day, ...)."""
    try:
        return pd.Timedelta(offset).as_unit("ns").value
    except (TypeError, ValueError):
        return None


def _grid_lengths(series: Series, req: ForecastRequest, offset, first: pd.Timestamp) -> tuple:
    """(grid points up to the last measured point, grid points in all)."""
    after = series.actual_times[-1] if series.actual_times else series.times[-1]
    first_ns = first.as_unit("ns").value
    step = _fixed_ns(offset)
    if step:
        checked = max(0, (after - first_ns) // step + 1)
    elif after >= first_ns:
        checked = len(pd.date_range(first, pd.Timestamp(after, unit="ns"), freq=offset))
    else:
        checked = 0

    if req.horizon_steps:
        return checked, checked + req.horizon_steps
    end = after + req.horizon_ns
    if step:
        # A horizon ending between two points runs to the later one.
        needed = max(0, -(-(end - first_ns) // step)) + 1
    else:
        reached = pd.date_range(first, pd.Timestamp(end, unit="ns"), freq=offset)
        needed = len(reached) + (0 if len(reached) and reached[-1].as_unit("ns").value == end else 1)
    return checked, needed


@dataclasses.dataclass
class Grid:
    """Where a series is predicted: through the validation window, then the horizon."""

    times: pd.DatetimeIndex
    first_new: int  # the first point after the data
    spacing: pd.Timedelta


def _forecast_grid(series: Series, req: ForecastRequest) -> Grid:
    offset, first = _forecast_step(series, req)
    try:
        checked, length = _grid_lengths(series, req, offset, first)
        _check_horizon(req, length)
        times = pd.date_range(start=first, periods=length, freq=offset).as_unit("ns")
    except (OverflowError, pd.errors.OutOfBoundsDatetime) as exc:
        raise ForecastError(
            "bad_request", f"the horizon runs past the timestamps InfluxDB can hold: {exc}"
        ) from exc
    spacing = pd.Series(times).diff().median() if len(times) > 1 else (times[0] + offset) - times[0]
    return Grid(times=times, first_new=checked, spacing=spacing)


# ---------------------------------------------------------------------------
# The model
# ---------------------------------------------------------------------------


def _level_key(level: float) -> str:
    """0.8 -> '80', 0.975 -> '97.5'."""
    return format((Decimal(str(level)) * 100).normalize(), "f")


def _model_path(req: ForecastRequest, series: Series) -> Path:
    """The saved model of a series: one per version and per data it trains on.

    History bounds stay out, as they move with every run, and so do the model's settings:
    changing those is a new unique_suffix. Values may hold anything, so the name holds a digest.
    """
    key = json.dumps(
        {
            "database": req.source_database,
            "measurement": req.source_measurement,
            "query": req.query,
            "field": series.field,
            "series": series.key,
            "tag_filter": {tag: sorted(values) for tag, values in req.tag_filter.items()},
            "sql_filter": req.sql_filter,
            "sampling_ns": req.sampling_ns,
        },
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    directory = resolve_plugin_dir() / MODEL_DIR_NAME
    return directory / f"prophet_model_{req.unique_suffix}_{digest}.json"


def _save_model(model: Prophet, path: Path) -> None:
    """Serialize a model through a temporary file so readers never see partial JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(model_to_json(model))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _new_model(req: ForecastRequest) -> Prophet:
    """An unfitted model; building it runs Prophet's own checks of the settings."""
    holidays = None
    if req.holiday_dates:
        holidays = pd.DataFrame(
            {"ds": pd.to_datetime(req.holiday_dates), "holiday": req.holiday_names}
        )
    model = Prophet(
        seasonality_mode=req.seasonality_mode,
        changepoint_prior_scale=req.changepoint_prior_scale,
        changepoints=req.changepoints or None,
        holidays=holidays,
    )
    if req.holiday_country_names:
        model.add_country_holidays(country_name=req.holiday_country_names[0])
    return model


def _train(req: ForecastRequest, series: Series) -> Prophet:
    model = _new_model(req)
    frame = pd.DataFrame({"ds": _timestamps(series.times), "y": series.values})
    # By default cmdstanpy keeps a run's files until the process ends.
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as output_dir:
        model.fit(frame, output_dir=output_dir)
    return model


def _model_for(influxdb3_local, req: ForecastRequest, series: Series, task_id: str) -> Prophet:
    """A fitted model: trained now, or the one saved for this version and series."""
    if req.model_mode == "train":
        return _train(req, series)
    try:
        path = _model_path(req, series)
        if path.exists():
            return model_from_json(path.read_text())
    except Exception as exc:
        raise ForecastError(
            "model_unavailable", f"cannot read the saved model of {series.tags}: {exc}"
        ) from exc
    model = _train(req, series)
    try:
        _save_model(model, path)
        influxdb3_local.info(f"[{task_id}] Saved a new model to {path}")
    except OSError as exc:
        influxdb3_local.warn(f"[{task_id}] The model could not be saved to {path}: {exc}")
    return model


def _predict(model: Prophet, times: pd.DatetimeIndex, levels: list) -> tuple:
    """The central forecast and {level: (lower, upper)}, the bounds from the model's samples."""
    future = pd.DataFrame({"ds": times})
    # predict would draw its own samples for one interval; these are drawn once, below.
    samples_kept = model.uncertainty_samples
    model.uncertainty_samples = 0
    try:
        central = model.predict(future)["yhat"].to_numpy()
    finally:
        model.uncertainty_samples = samples_kept
    if not levels:
        return central, {}
    samples = model.predictive_samples(future)["yhat"]
    bounds = {}
    for level in levels:
        lower, upper = np.nanpercentile(
            samples, [(1 - level) / 2 * 100, (1 + level) / 2 * 100], axis=1
        )
        # Samples scatter around the central value, so a narrow interval could miss it.
        bounds[level] = (np.minimum(lower, central), np.maximum(upper, central))
    return central, bounds


def _finite(value) -> float | None:
    return float(value) if value is not None and math.isfinite(value) else None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validation_problem(
    influxdb3_local, series: Series, grid: Grid, central, req: ForecastRequest, task_id: str
) -> str | None:
    """Compare the forecast with the measured points after the cut.

    With alignment position the two are paired in time order; with nearest each
    measured point is paired with the closest forecast point within half a step.

    Returns:
        Why validation failed, or None when MSRE stays within the threshold.
    """
    actual = pd.DataFrame({"ds": _timestamps(series.actual_times), "y": series.actual_values})
    predicted = pd.DataFrame({"ds": grid.times, "yhat": central})

    if req.validation_alignment == "nearest":
        # Half a step keeps the mapping unambiguous: the windows of neighbouring
        # forecast points do not overlap.
        matched = pd.merge_asof(
            actual, predicted, on="ds", direction="nearest", tolerance=grid.spacing / 2
        ).dropna(subset=["yhat"])
        if matched.empty:
            return f"validation failed: no measured point is within {grid.spacing / 2} of the forecast"
        y_true, y_pred = matched["y"], matched["yhat"]
    else:
        length = min(len(actual), len(predicted))
        y_true, y_pred = actual["y"].iloc[:length], predicted["yhat"].iloc[:length]

    # Zero actuals would divide by zero in the relative error.
    nonzero = y_true != 0
    y_true, y_pred = y_true[nonzero], y_pred[nonzero]
    if y_true.empty:
        return "validation failed: every measured value is zero, so MSRE has nothing to compare"
    msre = float(((y_true - y_pred) ** 2 / y_true**2).mean())
    influxdb3_local.info(f"[{task_id}] {series.field} {series.tags}: MSRE {msre}")
    if math.isnan(msre):
        return "validation failed: MSRE is not a number"
    if msre > req.msre_threshold:
        return f"validation failed: MSRE {msre} exceeds msre_threshold {req.msre_threshold}"
    return None


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------


def _split_backtest(series: Series, req: ForecastRequest) -> tuple[Series, list, list]:
    """Hold out the last backtest_points points, and keep point_limit points for the forecast.

    Returns:
        The backtest's training history, as long as the forecast's, and the held-out
        points' times and values.
    """
    held_out, limit = req.backtest_points, req.point_limit
    check = Series(
        field=series.field,
        tags=series.tags,
        times=series.times[:-held_out],
        values=series.values[:-held_out],
    )
    actual_times, actual_values = series.times[-held_out:], series.values[-held_out:]
    if limit:
        check.times, check.values = check.times[-limit:], check.values[-limit:]
        series.times, series.values = series.times[-limit:], series.values[-limit:]
    return check, actual_times, actual_values


def _backtest_result(central, bounds: dict, actual: list, levels: list) -> dict:
    """The forecast against the held-out values: the share inside each level, and the MAE."""
    measured = np.array(actual, dtype=float)
    coverage = {
        _level_key(level): float(
            np.mean((bounds[level][0] <= measured) & (measured <= bounds[level][1]))
        )
        for level in levels
    }
    mae = float(np.mean(np.abs(central - measured)))
    return {"points": len(actual), "coverage": coverage, "mae": _finite(mae)}


def _backtest(influxdb3_local, req: ForecastRequest, split: tuple, task_id: str) -> dict | None:
    """Train on the history before the cut and score it, or None when that cannot be done.

    A saved model has seen the held-out points, so this one is always trained, and never
    saved. It must not cost the forecast: a failure here, such as a changepoint after the
    cut, leaves a null backtest.
    """
    history, actual_times, actual_values = split
    if len(history.times) < MIN_POINTS:
        return None
    try:
        model = _train(req, history)
        central, bounds = _predict(model, _timestamps(actual_times), req.levels)
    except Exception as exc:
        influxdb3_local.warn(
            f"[{task_id}] {history.field} {history.tags}: the backtest failed: {exc}"
        )
        return None
    return _backtest_result(central, bounds, actual_values, req.levels)


# ---------------------------------------------------------------------------
# A forecast run
# ---------------------------------------------------------------------------


def _forecast_lines(req, series, moments, points, run_time, points_read, row_columns):
    """The stored rows of one forecast, one per point."""
    target = req.target_measurement or f"_forecasts.{req.source}"
    tags = {
        "field": series.field,
        "model": req.model,
        "source_measurement": req.source or None,
        "forecast_id": req.forecast_id or None,
        # A series tag named like a row column is left out; such series keys were refused.
        **{tag: value for tag, value in series.tags.items() if tag not in row_columns and value},
    }
    lines = []
    for step, (moment, point) in enumerate(zip(moments, points), start=1):
        fields = {key: value for key, value in point.items() if key != "timestamp"}
        fields.update(step=step, points_read=points_read, run_time=run_time)
        lines.append(build_line(LineBuilder, target, tags=tags, fields=fields, time_ns=moment))
    return lines


def _write(influxdb3_local, req: ForecastRequest, lines: list, task_id: str) -> None:
    if not lines:
        return
    try:
        write_data(influxdb3_local, lines, no_sync=True, database=req.target_database, retries=0)
    except Exception as exc:
        raise ForecastError("internal", f"forecast computed but the write failed: {exc}") from exc
    influxdb3_local.info(f"[{task_id}] Wrote {len(lines)} forecast rows")


def _too_few_message(req: ForecastRequest, observed: int) -> str:
    message = f"need >= {MIN_POINTS} points, got {observed}"
    if req.start_ns is not None:
        message += f" from {_iso(req.start_ns)} to {_iso(req.end_ns)}"
    elif req.window_ns:
        message += f" in the last {_duration_text(req.window_ns)}"
        if req.default_window and req.sampling_ns:
            message += "; set window, raise point_limit or shorten sampling_interval"
        elif req.default_window:
            message += "; set window or sampling_interval"
    if req.cut_ns is not None:
        message += ", before the validation window"
    return message


def _forecast_points(grid: Grid, central, bounds: dict, levels: list) -> tuple[list, list]:
    """The grid's points after the data: their times in ns, and the response's points."""
    keys = [(level, f"lower_{_level_key(level)}", f"upper_{_level_key(level)}") for level in levels]
    grid_ns = grid.times.asi8
    moments, points = [], []
    for i in range(grid.first_new, len(grid_ns)):
        moment = int(grid_ns[i])
        point = {"timestamp": moment // 10**6, "forecast": _finite(central[i])}
        for level, lower, upper in keys:
            point[lower] = _finite(bounds[level][0][i])
            point[upper] = _finite(bounds[level][1][i])
        moments.append(moment)
        points.append(point)
    return moments, points


def _run_forecast(influxdb3_local, req: ForecastRequest, task_id: str) -> tuple[dict, list]:
    """Read and forecast: the response of §4, and what _store writes."""
    response = {
        "status": "ok",
        "model": req.model,
        "levels": req.levels,
        "series_total": 0,
        "results": [],
    }
    if len(req.holiday_country_names) > 1:
        influxdb3_local.warn(
            f"[{task_id}] Prophet supports built-in holidays for one country, "
            f"using {req.holiday_country_names[0]!r}"
        )
    if req.query:
        # EXPLAIN also without a write: a query that does not plan is a bad request,
        # while one that fails while running is an internal error.
        sources = _query_measurements(influxdb3_local, req)
        req.source = sources[0] if len(sources) == 1 else None
        if req.write_results and req.source is None and not req.target_measurement:
            raise ForecastError(
                "bad_request",
                f"the query reads {len(sources)} measurements {sources}; set "
                f"target_measurement",
            )
        series_list, total = _read_query(influxdb3_local, req, task_id)
    else:
        req.source = req.source_measurement
        plan = _measurement_plan(influxdb3_local, req)
        total = plan[3]
        series_list = _read_measurement(influxdb3_local, req, plan) if plan[2] else []
    response["series_total"] = total
    if not series_list:
        influxdb3_local.info(f"[{task_id}] No history matches; nothing to forecast")
        return response, []
    if len(series_list) < total:
        influxdb3_local.info(
            f"[{task_id}] series_limit={req.series_limit} keeps {len(series_list)} of {total} "
            f"forecasts"
        )

    # The grids come first, so a horizon over the cap refuses before any model trains.
    grids, splits = {}, {}
    for series in series_list:
        if req.backtest_points:
            splits[id(series)] = _split_backtest(series, req)
        if len(series.times) < MIN_POINTS:
            series.error = ("too_few_points", _too_few_message(req, len(series.times)))
        elif req.cut_ns is not None and not series.actual_times:
            series.error = (
                "other",
                f"validation failed: no data from {_iso(req.cut_ns)} on, so the forecast cannot "
                f"be checked",
            )
        else:
            grids[id(series)] = _forecast_grid(series, req)

    started = _time.time()
    to_store = []
    for series in series_list:
        result = {"field": series.field, "tags": series.tags}
        grid = grids.get(id(series))
        if grid is not None:
            try:
                model = _model_for(influxdb3_local, req, series, task_id)
                central, bounds = _predict(model, grid.times, req.levels)
                if req.cut_ns is not None:
                    problem = _validation_problem(
                        influxdb3_local, series, grid, central, req, task_id
                    )
                    if problem:
                        series.error = ("other", problem)
            except ForecastError as exc:
                series.error = (exc.code, exc.message)
            except Exception as exc:
                series.error = ("internal", f"forecast failed: {exc}")
        if series.error:
            result.update(
                {"status": "error", "code": series.error[0], "message": series.error[1]}
            )
            response["results"].append(result)
            continue

        moments, points = _forecast_points(grid, central, bounds, req.levels)
        # A saved model read the history it was trained on, not today's.
        points_read = len(model.history)
        if req.end_ns is None and moments[-1] < req.now_ns:
            influxdb3_local.warn(
                f"[{task_id}] {series.field} {series.tags}: the history ends at "
                f"{_iso(series.times[-1])}, so the whole forecast lies in the past"
            )
        result.update(
            {
                "points_read": points_read,
                "step_ms": int(grid.spacing / pd.Timedelta(1, "ms")),
                "forecast": points,
            }
        )
        if req.backtest_points:
            result["backtest"] = _backtest(influxdb3_local, req, splits[id(series)], task_id)
        response["results"].append(result)
        to_store.append((series, moments, points, points_read))
    influxdb3_local.info(
        f"[{task_id}] Forecast {len(grids)} series in {(_time.time() - started) * 1000:.0f}ms"
    )
    return response, to_store


def _store(influxdb3_local, req: ForecastRequest, to_store: list, task_id: str) -> None:
    """Write the forecasts of a run, all in one batch."""
    run_time = datetime.fromtimestamp(req.now_ns // 10**9, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    row_columns = _forecast_row_columns(req.levels)
    try:
        lines = [
            line
            for series, moments, points, points_read in to_store
            for line in _forecast_lines(
                req, series, moments, points, run_time, points_read, row_columns
            )
        ]
    except Exception as exc:
        # LineBuilder refuses some names, e.g. one with a space.
        raise ForecastError(
            "internal", f"forecast computed but its rows cannot be built: {exc}"
        ) from exc
    _write(influxdb3_local, req, lines, task_id)


def _error_body(exc: ForecastError) -> dict:
    return {"status": "error", "code": exc.code, "message": exc.message}


# ---------------------------------------------------------------------------
# Validation alerts
# ---------------------------------------------------------------------------


def parse_senders(influxdb3_local, config: dict, task_id: str) -> dict:
    """
    Parse and validate sender configurations from the loaded config.

    Args:
        influxdb3_local: InfluxDB client instance.
        config (dict): Loaded configuration containing "senders" (a space-separated
            string or a list) and the keys each sender requires (see AVAILABLE_SENDERS).
        task_id (str): Unique task identifier used for logging context.

    Returns:
        dict: A mapping `{sender_type: {key: value}}` for each valid sender.

    Raises:
        Exception: If no valid senders are found after parsing.
    """
    senders_config: defaultdict = defaultdict(dict)
    senders: list = _names(config.get("senders", ""))
    if not senders:
        raise Exception("No senders provided")

    for sender in senders:
        if sender not in AVAILABLE_SENDERS:
            influxdb3_local.warn(f"[{task_id}] Invalid sender type: {sender}")
            continue
        for key in AVAILABLE_SENDERS[sender]:
            if key not in config and not any(ex in key for ex in EXCLUDED_KEYWORDS):
                influxdb3_local.warn(
                    f"[{task_id}] Required key '{key}' missing for sender '{sender}'"
                )
                senders_config.pop(sender, None)
                break
            if "url" in key and not validate_webhook_url(
                influxdb3_local, sender, config[key], task_id
            ):
                senders_config.pop(sender, None)
                break

            if key not in config:
                continue
            senders_config[sender][key] = config[key]

    if not senders_config:
        raise Exception("No valid senders configured")
    return senders_config


def validate_webhook_url(influxdb3_local, service: str, url: str, task_id: str) -> bool:
    """
    Validate webhook URL format.

    Returns:
        bool: True if URL is valid, False otherwise.
    """
    try:
        result = urlparse(url)
        if result.scheme not in ("http", "https"):
            influxdb3_local.error(
                f"[{task_id}] {service} webhook URL must start with 'https://' or 'http://'"
            )
            return False
        return True
    except Exception as e:
        influxdb3_local.error(
            f"[{task_id}] Unable to parse {service} webhook URL: {str(e)}"
        )
        return False


def interpolate_notification_text(text: str, row_data: dict) -> str:
    """Replace $variables in the notification template with actual values."""
    return Template(text).safe_substitute(row_data)


def send_notification(
    influxdb3_local, port: int, path: str, token: str, payload: dict, task_id: str
) -> None:
    """
    Send a JSON POST to the given InfluxDB 3 webhook endpoint, with up to
    3 retry attempts and randomized backoff delays between attempts.

    Args:
        influxdb3_local: InfluxDB client instance.
        port (int): Port number on which the HTTP API is listening (e.g. 8181).
        path (str): Path to the webhook handler (e.g. "notify" or "custom/path").
        token (str): API v3 token string (without the "Bearer " prefix).
        payload (dict): Dict to serialize as JSON in the POST body.
        task_id (str): Unique task identifier.
    """
    url: str = f"http://localhost:{port}/api/v3/engine/{path}"
    headers: dict = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    data: str = json.dumps(payload)

    max_retries: int = 3
    timeout: float = 5.0

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, data=data, timeout=timeout)
            resp.raise_for_status()  # raises on 4xx/5xx
            influxdb3_local.info(
                f"[{task_id}] Alert sent to notification plugin with results: {resp.json()['results']}"
            )
            return
        except requests.RequestException as e:
            influxdb3_local.warn(
                f"[{task_id}] [Attempt {attempt}/{max_retries}] Error sending alert to notification plugin: {e}"
            )
            if attempt < max_retries:
                wait = random.uniform(1, 4)
                influxdb3_local.info(
                    f"[{task_id}] Retrying sending alert to notification plugin in {wait:.1f} seconds."
                )
                _time.sleep(wait)
            else:
                influxdb3_local.error(
                    f"[{task_id}] Failed to send alert to notification plugin after {max_retries} attempts: {e}"
                )


def _series_label(result: dict) -> str:
    tags = " ".join(f"{tag}={value}" for tag, value in result["tags"].items())
    return f"{result['field']} {tags}".strip()


def _send_validation_alert(
    influxdb3_local, cfg: dict, req: ForecastRequest, failed: list, task_id: str
) -> None:
    """One alert for a run, listing the series that failed validation."""
    try:
        senders_config: dict = parse_senders(influxdb3_local, cfg, task_id)
        token: str = cfg.get("influxdb3_auth_token", "")
        if not token:
            raise Exception("INFLUXDB3_AUTH_TOKEN not found")

        fields = sorted({result["field"] for result in failed})
        payload: dict = {
            "notification_text": interpolate_notification_text(
                cfg["notification_text"],
                {
                    "version": req.unique_suffix or "",
                    "measurement": req.source or "",
                    "field": ", ".join(fields),
                    "series": ", ".join(_series_label(result) for result in failed),
                    "start_time": _iso(req.cut_ns),
                    "end_time": _iso(req.now_ns),
                    "output_measurement": req.target_measurement or f"_forecasts.{req.source}",
                },
            ),
            "senders_config": senders_config,
        }
        send_notification(
            influxdb3_local,
            cfg["port_override"],
            cfg["notification_path"],
            token,
            payload,
            task_id,
        )
    except Exception as e:
        influxdb3_local.error(f"[{task_id}] Failed to send notification: {e}")


# ===========================================================================
# Entry point: HTTP trigger
# ===========================================================================


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """On-demand forecast of a measurement field or of a query result.

    Args:
        influxdb3_local: Processing engine API handle for queries, writes, and logging.
        query_parameters: URL query parameters; any request key may be set here.
        request_headers: HTTP headers; a key is also read from
            X-Influxdb3-Prophet-Forecasting-<key>.
        request_body: JSON object with the request keys.
        args: Trigger arguments, used as defaults for the request.

    Returns:
        The response body and its HTTP status: 200 with the forecast, or the
        status of the error code.
    """
    task_id = str(uuid.uuid4())[:8]
    try:
        _refuse_unknown_headers(request_headers)
        try:
            cfg = _load_settings(
                args,
                REQUEST_NAMES,
                (
                    parse_json_body(request_body, REQUEST_KEYS),
                    parse_request_headers(request_headers, HEADER_KEYS),
                    parse_query_parameters(query_parameters, REQUEST_KEYS),
                ),
            )
        except ValueError as exc:
            raise ForecastError("bad_request", str(exc)) from exc
        req = _resolve(cfg, scheduled=False, now_ns=_time.time_ns())
        influxdb3_local.info(
            f"[{task_id}] Forecast request: "
            f"{'query' if req.query else f'{req.source_measurement}.{req.field}'}"
        )
        response, to_store = _run_forecast(influxdb3_local, req, task_id)
        if req.write_results:
            _store(influxdb3_local, req, to_store, task_id)
        return response, 200
    except ForecastError as exc:
        log = influxdb3_local.warn if exc.code == "bad_request" else influxdb3_local.error
        log(f"[{task_id}] {exc.code}: {exc.message}")
        return _error_body(exc), HTTP_STATUS[exc.code]
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] internal: {exc}")
        return {"status": "error", "code": "internal", "message": str(exc)}, 500


# ===========================================================================
# Entry point: Scheduled trigger
# ===========================================================================


def process_scheduled_call(
    influxdb3_local, call_time: datetime, args: dict | None = None
):
    """Scheduled forecast, written to target_measurement on every run.

    A series that fails validation is not written, and one alert for the run lists
    them all when is_sending_alert is set.

    Args:
        influxdb3_local: Processing engine API handle for queries, writes, and logging.
        call_time: When the trigger fired.
        args: Trigger arguments; config_file_path points at a TOML file whose
            keys override them.

    Returns:
        None. Errors go to the plugin log with their code.
    """
    task_id = str(uuid.uuid4())[:8]
    try:
        try:
            cfg = _load_settings(args, SCHEDULED_NAMES)
        except ValueError as exc:
            raise ForecastError("bad_request", str(exc)) from exc
        req = _resolve(cfg, scheduled=True, now_ns=_to_ns(call_time))
        if cfg["is_sending_alert"] and not req.validation_ns:
            influxdb3_local.warn(
                f"[{task_id}] is_sending_alert has no effect without validation_window: "
                f"alerts are only sent when validation fails"
            )
        response, to_store = _run_forecast(influxdb3_local, req, task_id)
        failed = []
        for result in response["results"]:
            if result.get("status") != "error":
                continue
            log = influxdb3_local.warn if result["code"] == "too_few_points" else influxdb3_local.error
            log(
                f"[{task_id}] {result['code']} for {result['field']} "
                f"{result['tags']}: {result['message']}"
            )
            if result["code"] == "other":
                failed.append(result)
        if failed and cfg["is_sending_alert"]:
            _send_validation_alert(influxdb3_local, cfg, req, failed, task_id)
        # Last, so a failed write costs neither the errors above nor the alert.
        _store(influxdb3_local, req, to_store, task_id)
    except ForecastError as exc:
        influxdb3_local.error(f"[{task_id}] {exc.code}: {exc.message}")
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] internal: {exc}")
    return None
