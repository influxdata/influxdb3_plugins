"""
{
    "plugin_type": [
        "scheduled",
        "http"
    ],
    "scheduled_args_config": [
        {
            "name": "providers",
            "example": "anthropic openai",
            "description": "Providers to collect: a list (space-separated string in trigger arguments, list in TOML) of anthropic, openai, bedrock, azure and google (at least one). Each listed provider is collected on every run. A configuration error in a listed provider's settings, libraries or secrets refuses the whole run; a provider whose calls fail does not stop the others.",
            "required": true
        },
        {
            "name": "collect",
            "example": "usage",
            "description": "What each run reads for every provider: a list (space-separated string in trigger arguments, list in TOML) of usage and cost (at least one). Default: usage cost. Without usage, no usage endpoint is called; without cost, no cost endpoint is called and the time of the last cost fetch is left unchanged. usage alone suits credentials without cost permissions and avoids the paid Bedrock Cost Explorer calls.",
            "required": false
        },
        {
            "name": "usage_types",
            "example": "input_tokens output_tokens requests",
            "description": "Usage types to collect: a list (space-separated string in trigger arguments, list in TOML) of input_tokens, cache_read_tokens, cache_write_tokens, output_tokens, requests, images, audio_seconds, tts_characters, web_search_requests, file_search_requests, code_interpreter_sessions, vector_store_bytes (at least one). Default: all. A provider writes only the types it reports; the OpenAI endpoints for unselected types are not called.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "llm_usage",
            "description": "Database that receives both tables: letters, digits, _ and -, starting with a letter or digit. Default: the database of the trigger.",
            "required": false
        },
        {
            "name": "usage_table",
            "example": "llm_usage",
            "description": "Table that receives usage rows: any printable characters except spaces and backslashes, not starting with #; must differ from cost_table. Default: llm_usage.",
            "required": false
        },
        {
            "name": "cost_table",
            "example": "llm_cost",
            "description": "Table that receives cost rows: any printable characters except spaces and backslashes, not starting with #; must differ from usage_table. Default: llm_cost.",
            "required": false
        },
        {
            "name": "bucket_width",
            "example": "1h",
            "description": "Width of a usage bucket: 1h or 1d. Default: 1h. Cost is always daily. Changing it on a table that already has data mixes granularities; use a new table.",
            "required": false
        },
        {
            "name": "exclude_tags",
            "example": "user_id api_key_id",
            "description": "Tags left out of both tables: a list (space-separated string in trigger arguments, list in TOML) of account, project, api_key_id and user_id. Default: none. Rows that become identical are summed, so totals stay intact; api_key_name goes with api_key_id. Where the provider can skip a breakdown, the plugin doesn't request it. Changing it on a table that already has data mixes granularities; use a new table.",
            "required": false
        },
        {
            "name": "usage_lookback",
            "example": "48h",
            "description": "How far back each run re-reads usage: an integer and a unit (s, min, h, d, w), at least one bucket (1h, or 1d when bucket_width is 1d). Default: 48h. Only complete buckets are read, up to the start of the current bucket; rows are overwritten in place.",
            "required": false
        },
        {
            "name": "cost_lookback",
            "example": "40d",
            "description": "How far back a cost fetch re-reads daily cost: an integer and a unit (s, min, h, d, w), at least 1d, rounded up to whole days. Default: 40d, which covers the corrections providers make into the first week of the next month.",
            "required": false
        },
        {
            "name": "cost_interval",
            "example": "6h",
            "description": "Minimum time between cost fetches of one provider: an integer and a unit (s, min, h, d, w), greater than zero. Default: 6h. A run before that interval has passed fetches usage only. For AWS, a longer interval spaces out the Cost Explorer calls; CloudWatch usage is read on every run. STS is called with each cost fetch, and on every run unless bedrock_account is set or account is in exclude_tags.",
            "required": false
        },
        {
            "name": "max_run_time",
            "example": "15min",
            "description": "Longest time a run spends calling providers: an integer and a unit (s, min, h, d, w), greater than zero. Default: 15min. What is left of it is shared equally by the providers not started yet. When a provider's share is used up, a retry that would outlast it isn't made, no new call starts, and the rest of its work in the run is skipped with an error naming what was skipped. Choose a trigger interval longer than max_run_time: runs that outlast the interval are queued and run back to back.",
            "required": false
        },
        {
            "name": "anthropic_admin_key_env",
            "example": "ANTHROPIC_ADMIN_KEY",
            "description": "Name of the environment variable that holds the Anthropic admin key. Default: ANTHROPIC_ADMIN_KEY. When anthropic is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "anthropic_account",
            "example": "default",
            "description": "Free-text label written as the account tag of Anthropic rows; keeps the rows of two triggers apart. Default: default.",
            "required": false
        },
        {
            "name": "openai_admin_key_env",
            "example": "OPENAI_ADMIN_KEY",
            "description": "Name of the environment variable that holds the OpenAI admin key. Default: OPENAI_ADMIN_KEY. When openai is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "openai_account",
            "example": "default",
            "description": "Free-text label written as the account tag of OpenAI rows; keeps the rows of two triggers apart. Default: default.",
            "required": false
        },
        {
            "name": "bedrock_regions",
            "example": "us-east-1 eu-west-1",
            "description": "AWS regions to read Bedrock usage and cost from: a list (space-separated string in trigger arguments, list in TOML) of region codes such as us-east-1 (at least one). Required when bedrock is listed in providers; a missing value or a name that isn't a region code refuses the run with an error naming the setting or the name, and so does a missing boto3 package. Charges and usage in other regions are not collected.",
            "required": false
        },
        {
            "name": "bedrock_use_default_credentials",
            "example": "false",
            "description": "true or false. Default: false, which reads the access key from the variables named by the bedrock_*_env settings and ignores the standard AWS_* variables. true uses the standard AWS credential chain instead: AWS_* variables, then an instance or container role. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_access_key_id_env",
            "example": "BEDROCK_AWS_ACCESS_KEY_ID",
            "description": "Name of the environment variable that holds the AWS access key ID. Default: BEDROCK_AWS_ACCESS_KEY_ID. Not read when bedrock_use_default_credentials is true; otherwise, when bedrock is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_secret_access_key_env",
            "example": "BEDROCK_AWS_SECRET_ACCESS_KEY",
            "description": "Name of the environment variable that holds the AWS secret access key. Default: BEDROCK_AWS_SECRET_ACCESS_KEY. Not read when bedrock_use_default_credentials is true; otherwise, when bedrock is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_session_token_env",
            "example": "BEDROCK_AWS_SESSION_TOKEN",
            "description": "Name of the environment variable that holds an optional AWS session token. Default: BEDROCK_AWS_SESSION_TOKEN; the token is used only when that variable is set, and a value with a control character refuses the run. Not read when bedrock_use_default_credentials is true. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_account",
            "example": "aws-main",
            "description": "Free-text label written as the account tag of Bedrock rows. Default: the AWS account ID of the credentials in use.",
            "required": false
        },
        {
            "name": "azure_tenant_id",
            "example": "00000000-0000-0000-0000-000000000000",
            "description": "Microsoft Entra tenant ID (a GUID) used to request tokens. Required when azure is listed in providers; a missing value or one that isn't a GUID refuses the run with an error naming the setting.",
            "required": false
        },
        {
            "name": "azure_client_id",
            "example": "11111111-1111-1111-1111-111111111111",
            "description": "Application (client) ID (a GUID) of the service principal. Required when azure is listed in providers; a missing value or one that isn't a GUID refuses the run with an error naming the setting.",
            "required": false
        },
        {
            "name": "azure_client_secret_env",
            "example": "AZURE_CLIENT_SECRET",
            "description": "Name of the environment variable that holds the service principal client secret. Default: AZURE_CLIENT_SECRET. When azure is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "azure_subscription_ids",
            "example": "22222222-2222-2222-2222-222222222222",
            "description": "Azure subscriptions to collect: a list (space-separated string in trigger arguments, list in TOML) of subscription IDs (GUIDs), at least one. Required when azure is listed in providers; a missing value or an item that isn't a GUID refuses the run with an error naming the setting. Every Azure OpenAI and AI Services resource of each subscription is discovered.",
            "required": false
        },
        {
            "name": "azure_account",
            "example": "azure-main",
            "description": "Free-text label written as the account tag of Azure rows. Default: the subscription ID. One label for several subscriptions sums their rows.",
            "required": false
        },
        {
            "name": "google_credentials_env",
            "example": "GOOGLE_LLM_USAGE_CREDENTIALS",
            "description": "Name of the environment variable that holds the Google service account key: the key JSON on one line (a value starting with `{`; the server passes a value only up to its first line break), or a path to the key file (absolute or relative to the plugin directory). Default: GOOGLE_LLM_USAGE_CREDENTIALS. When google is listed in providers, a variable that is not set or does not hold a usable service account key refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "google_project_ids",
            "example": "my-project-1 my-project-2",
            "description": "Google Cloud project IDs to read Vertex AI and Gemini API usage and cost from: a list (space-separated string in trigger arguments, list in TOML) of project IDs such as my-project-1 (6 to 30 lower-case letters, digits and hyphens, starting with a letter and not ending with a hyphen; a domain-scoped ID such as example.com:my-project is also accepted), at least one. Required when google is listed in providers; a missing value or an ID of another form refuses the run with an error naming the setting, and so does a missing google-auth package.",
            "required": false
        },
        {
            "name": "google_account",
            "example": "gcp-main",
            "description": "Free-text label written as the account tag of Google rows; keeps the rows of two triggers apart. Default: default.",
            "required": false
        },
        {
            "name": "google_sources",
            "example": "vertex_metrics gemini_api_metrics",
            "description": "Sources to read in every project: a list (space-separated string in trigger arguments, list in TOML) of vertex_metrics (Vertex AI usage), gemini_api_metrics (Gemini API usage) and billing_export (cost from the Cloud Billing export in BigQuery), at least one. Default: vertex_metrics gemini_api_metrics. Google cost is collected only with billing_export, which needs google_billing_table.",
            "required": false
        },
        {
            "name": "google_billing_table",
            "example": "my-billing-project.billing_export.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX",
            "description": "BigQuery table of the standard usage cost export of the Cloud Billing account, as <project>.<dataset>.<table>: a project ID as in google_project_ids, a dataset name of 1 to 1024 letters, digits and _, a table name of 1 to 1024 letters, digits, _ and -. Required when billing_export is listed in google_sources; a missing value refuses the run, and so does a value in another form, whether or not billing_export is listed.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "llm_usage_scheduled_config.toml",
            "description": "TOML configuration file, absolute or relative to the plugin directory. Accepted only as a trigger argument. The file may hold every other setting of this trigger type; its keys override the same keys given inline.",
            "required": false
        }
    ],
    "http_args_config": [
        {
            "name": "providers",
            "example": "anthropic openai",
            "description": "Providers to collect: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of anthropic, openai, bedrock, azure and google (at least one). Each listed provider is collected on every run. A configuration error in a listed provider's settings, libraries or secrets refuses the whole request; a provider whose calls fail does not stop the others. A request may name only providers its trigger is configured with.",
            "required": true
        },
        {
            "name": "start",
            "example": "2026-01-01T00:00:00Z",
            "description": "Start of the period to collect: an ISO 8601 timestamp with Z or an offset; a value without a zone is UTC. Required for each request: usually sent in the request; set it on the trigger only to give every request the same start. A start before a provider's oldest available data is served from that moment, and the response says so.",
            "required": false
        },
        {
            "name": "end",
            "example": "2026-02-01T00:00:00Z",
            "description": "End of the period to collect, exclusive: an ISO 8601 timestamp with Z or an offset; a value without a zone is UTC. Must be after start. Default: the time of the request. Usage is read in whole buckets, complete ones only (up to the start of the current bucket); cost in whole days through the day of end, no later than today.",
            "required": false
        },
        {
            "name": "collect",
            "example": "usage",
            "description": "What the request reads for every provider: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of usage and cost (at least one). Default: usage cost. Without usage, no usage endpoint is called and usage_period is null; without cost, no cost endpoint is called and cost_period is null.",
            "required": false
        },
        {
            "name": "usage_types",
            "example": "input_tokens output_tokens requests",
            "description": "Usage types to collect: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of input_tokens, cache_read_tokens, cache_write_tokens, output_tokens, requests, images, audio_seconds, tts_characters, web_search_requests, file_search_requests, code_interpreter_sessions, vector_store_bytes (at least one). Default: all. A provider writes only the types it reports; the OpenAI endpoints for unselected types are not called.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "llm_usage",
            "description": "Database that receives both tables: letters, digits, _ and -, starting with a letter or digit. Default: the database of the trigger.",
            "required": false
        },
        {
            "name": "usage_table",
            "example": "llm_usage",
            "description": "Table that receives usage rows: any printable characters except spaces and backslashes, not starting with #; must differ from cost_table. Default: llm_usage.",
            "required": false
        },
        {
            "name": "cost_table",
            "example": "llm_cost",
            "description": "Table that receives cost rows: any printable characters except spaces and backslashes, not starting with #; must differ from usage_table. Default: llm_cost.",
            "required": false
        },
        {
            "name": "bucket_width",
            "example": "1h",
            "description": "Width of a usage bucket: 1h or 1d. Default: 1h. Cost is always daily. Changing it on a table that already has data mixes granularities; use a new table.",
            "required": false
        },
        {
            "name": "exclude_tags",
            "example": "user_id api_key_id",
            "description": "Tags left out of both tables: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of account, project, api_key_id and user_id. Default: none. Rows that become identical are summed, so totals stay intact; api_key_name goes with api_key_id. Where the provider can skip a breakdown, the plugin doesn't request it. Changing it on a table that already has data mixes granularities; use a new table.",
            "required": false
        },
        {
            "name": "max_run_time",
            "example": "1h",
            "description": "Longest time a request spends calling providers: an integer and a unit (s, min, h, d, w), greater than zero. Default: 15min. What is left of it is shared equally by the providers not started yet. When a provider's share is used up, a retry that would outlast it isn't made, no new call starts, and the rest of its work is skipped with an error naming what was skipped; repeating the request with a larger value is safe. A backfill of a long period may need more.",
            "required": false
        },
        {
            "name": "anthropic_admin_key_env",
            "example": "ANTHROPIC_ADMIN_KEY",
            "description": "Name of the environment variable that holds the Anthropic admin key. Default: ANTHROPIC_ADMIN_KEY. When anthropic is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "anthropic_account",
            "example": "default",
            "description": "Free-text label written as the account tag of Anthropic rows; keeps the rows of two triggers apart. Default: default.",
            "required": false
        },
        {
            "name": "openai_admin_key_env",
            "example": "OPENAI_ADMIN_KEY",
            "description": "Name of the environment variable that holds the OpenAI admin key. Default: OPENAI_ADMIN_KEY. When openai is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "openai_account",
            "example": "default",
            "description": "Free-text label written as the account tag of OpenAI rows; keeps the rows of two triggers apart. Default: default.",
            "required": false
        },
        {
            "name": "bedrock_regions",
            "example": "us-east-1 eu-west-1",
            "description": "AWS regions to read Bedrock usage and cost from: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of region codes such as us-east-1 (at least one). Required when bedrock is listed in providers; a missing value or a name that isn't a region code refuses the run with an error naming the setting or the name, and so does a missing boto3 package. Charges and usage in other regions are not collected.",
            "required": false
        },
        {
            "name": "bedrock_use_default_credentials",
            "example": "false",
            "description": "true or false. Default: false, which reads the access key from the variables named by the bedrock_*_env settings and ignores the standard AWS_* variables. true uses the standard AWS credential chain instead: AWS_* variables, then an instance or container role. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_access_key_id_env",
            "example": "BEDROCK_AWS_ACCESS_KEY_ID",
            "description": "Name of the environment variable that holds the AWS access key ID. Default: BEDROCK_AWS_ACCESS_KEY_ID. Not read when bedrock_use_default_credentials is true; otherwise, when bedrock is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_secret_access_key_env",
            "example": "BEDROCK_AWS_SECRET_ACCESS_KEY",
            "description": "Name of the environment variable that holds the AWS secret access key. Default: BEDROCK_AWS_SECRET_ACCESS_KEY. Not read when bedrock_use_default_credentials is true; otherwise, when bedrock is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_session_token_env",
            "example": "BEDROCK_AWS_SESSION_TOKEN",
            "description": "Name of the environment variable that holds an optional AWS session token. Default: BEDROCK_AWS_SESSION_TOKEN; the token is used only when that variable is set, and a value with a control character refuses the run. Not read when bedrock_use_default_credentials is true. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "bedrock_account",
            "example": "aws-main",
            "description": "Free-text label written as the account tag of Bedrock rows. Default: the AWS account ID of the credentials in use.",
            "required": false
        },
        {
            "name": "azure_tenant_id",
            "example": "00000000-0000-0000-0000-000000000000",
            "description": "Microsoft Entra tenant ID (a GUID) used to request tokens. Required when azure is listed in providers; a missing value or one that isn't a GUID refuses the run with an error naming the setting.",
            "required": false
        },
        {
            "name": "azure_client_id",
            "example": "11111111-1111-1111-1111-111111111111",
            "description": "Application (client) ID (a GUID) of the service principal. Required when azure is listed in providers; a missing value or one that isn't a GUID refuses the run with an error naming the setting.",
            "required": false
        },
        {
            "name": "azure_client_secret_env",
            "example": "AZURE_CLIENT_SECRET",
            "description": "Name of the environment variable that holds the service principal client secret. Default: AZURE_CLIENT_SECRET. When azure is listed in providers, a variable that is not set or holds a control character refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "azure_subscription_ids",
            "example": "22222222-2222-2222-2222-222222222222",
            "description": "Azure subscriptions to collect: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of subscription IDs (GUIDs), at least one. Required when azure is listed in providers; a missing value or an item that isn't a GUID refuses the run with an error naming the setting. Every Azure OpenAI and AI Services resource of each subscription is discovered.",
            "required": false
        },
        {
            "name": "azure_account",
            "example": "azure-main",
            "description": "Free-text label written as the account tag of Azure rows. Default: the subscription ID. One label for several subscriptions sums their rows.",
            "required": false
        },
        {
            "name": "google_credentials_env",
            "example": "GOOGLE_LLM_USAGE_CREDENTIALS",
            "description": "Name of the environment variable that holds the Google service account key: the key JSON on one line (a value starting with `{`; the server passes a value only up to its first line break), or a path to the key file (absolute or relative to the plugin directory). Default: GOOGLE_LLM_USAGE_CREDENTIALS. When google is listed in providers, a variable that is not set or does not hold a usable service account key refuses the run. Set in trigger arguments, the TOML file or the environment; not accepted from an HTTP request.",
            "required": false
        },
        {
            "name": "google_project_ids",
            "example": "my-project-1 my-project-2",
            "description": "Google Cloud project IDs to read Vertex AI and Gemini API usage and cost from: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of project IDs such as my-project-1 (6 to 30 lower-case letters, digits and hyphens, starting with a letter and not ending with a hyphen; a domain-scoped ID such as example.com:my-project is also accepted), at least one. Required when google is listed in providers; a missing value or an ID of another form refuses the run with an error naming the setting, and so does a missing google-auth package.",
            "required": false
        },
        {
            "name": "google_account",
            "example": "gcp-main",
            "description": "Free-text label written as the account tag of Google rows; keeps the rows of two triggers apart. Default: default.",
            "required": false
        },
        {
            "name": "google_sources",
            "example": "vertex_metrics gemini_api_metrics",
            "description": "Sources to read in every project: a list (space-separated string in trigger arguments and query parameters, list in TOML and the JSON body) of vertex_metrics (Vertex AI usage), gemini_api_metrics (Gemini API usage) and billing_export (cost from the Cloud Billing export in BigQuery), at least one. Default: vertex_metrics gemini_api_metrics. Google cost is collected only with billing_export, which needs google_billing_table.",
            "required": false
        },
        {
            "name": "google_billing_table",
            "example": "my-billing-project.billing_export.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX",
            "description": "BigQuery table of the standard usage cost export of the Cloud Billing account, as <project>.<dataset>.<table>: a project ID as in google_project_ids, a dataset name of 1 to 1024 letters, digits and _, a table name of 1 to 1024 letters, digits, _ and -. Required when billing_export is listed in google_sources; a missing value refuses the run, and so does a value in another form, whether or not billing_export is listed.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "llm_usage_http_config.toml",
            "description": "TOML configuration file, absolute or relative to the plugin directory. Accepted only as a trigger argument, never from a request.",
            "required": false
        }
    ],
    "http_body_config": [
        {
            "name": "providers",
            "example": "['openai']",
            "description": "Providers to collect in this request: a JSON array (a space-separated string also works) of anthropic, openai, bedrock, azure and google, at least one. Only providers the trigger is configured with; naming another refuses the request. Default: the trigger's providers.",
            "required": false
        },
        {
            "name": "start",
            "example": "2026-01-01T00:00:00Z",
            "description": "Start of the period to collect: an ISO 8601 timestamp with Z or an offset; a value without a zone is UTC. Required, here or in the trigger's settings. A start before a provider's oldest available data is served from that moment, and the response says so.",
            "required": false
        },
        {
            "name": "end",
            "example": "2026-02-01T00:00:00Z",
            "description": "End of the period to collect, exclusive: an ISO 8601 timestamp with Z or an offset; a value without a zone is UTC. Must be after start. Default: the time of the request. Usage is read in whole buckets, complete ones only (up to the start of the current bucket); cost in whole days through the day of end, no later than today.",
            "required": false
        },
        {
            "name": "collect",
            "example": "['usage']",
            "description": "What the request reads for every provider: a JSON array (a space-separated string also works) of usage and cost, at least one. Default: usage cost. Without usage, no usage endpoint is called and usage_period is null; without cost, no cost endpoint is called and cost_period is null.",
            "required": false
        },
        {
            "name": "usage_types",
            "example": "['input_tokens', 'output_tokens', 'requests']",
            "description": "Usage types to collect: a JSON array (a space-separated string also works) of input_tokens, cache_read_tokens, cache_write_tokens, output_tokens, requests, images, audio_seconds, tts_characters, web_search_requests, file_search_requests, code_interpreter_sessions, vector_store_bytes, at least one. Default: all. A provider writes only the types it reports; the OpenAI endpoints for unselected types are not called.",
            "required": false
        },
        {
            "name": "target_database",
            "example": "llm_usage",
            "description": "Database that receives both tables: letters, digits, _ and -, starting with a letter or digit. Default: the database of the trigger.",
            "required": false
        },
        {
            "name": "usage_table",
            "example": "llm_usage",
            "description": "Table that receives usage rows: any printable characters except spaces and backslashes, not starting with #; must differ from cost_table. Default: llm_usage.",
            "required": false
        },
        {
            "name": "cost_table",
            "example": "llm_cost",
            "description": "Table that receives cost rows: any printable characters except spaces and backslashes, not starting with #; must differ from usage_table. Default: llm_cost.",
            "required": false
        },
        {
            "name": "bucket_width",
            "example": "1d",
            "description": "Width of a usage bucket: 1h or 1d. Default: 1h. Cost is always daily. Changing it on a table that already has data mixes granularities; use a new table.",
            "required": false
        },
        {
            "name": "exclude_tags",
            "example": "['user_id', 'api_key_id']",
            "description": "Tags left out of both tables: a JSON array (a space-separated string also works) of account, project, api_key_id and user_id. An empty array clears a value set on the trigger; an empty string counts as unset. Default: none. Rows that become identical are summed, so totals stay intact; api_key_name goes with api_key_id. Changing it on a table that already has data mixes granularities; use a new table.",
            "required": false
        },
        {
            "name": "max_run_time",
            "example": "1h",
            "description": "Longest time the request spends calling providers: an integer and a unit (s, min, h, d, w), greater than zero. Default: the trigger's value, else 15min. What is left of it is shared equally by the providers not started yet. When a provider's share is used up, a retry that would outlast it isn't made, no new call starts, and the rest of its work is skipped with an error naming what was skipped; repeating the request with a larger value is safe.",
            "required": false
        },
        {
            "name": "anthropic_account",
            "example": "team-a",
            "description": "Free-text label written as the account tag of Anthropic rows. Default: default.",
            "required": false
        },
        {
            "name": "openai_account",
            "example": "team-b",
            "description": "Free-text label written as the account tag of OpenAI rows. Default: default.",
            "required": false
        },
        {
            "name": "bedrock_regions",
            "example": "['us-east-1', 'eu-west-1']",
            "description": "AWS regions to read Bedrock usage and cost from: a JSON array (a space-separated string also works) of region codes such as us-east-1, at least one. Required when bedrock is listed in providers, here or in the trigger's settings; a name that isn't a region code refuses the request. Charges and usage in other regions are not collected.",
            "required": false
        },
        {
            "name": "bedrock_account",
            "example": "aws-main",
            "description": "Free-text label written as the account tag of Bedrock rows. Default: the AWS account ID of the credentials in use.",
            "required": false
        },
        {
            "name": "azure_tenant_id",
            "example": "00000000-0000-0000-0000-000000000000",
            "description": "Microsoft Entra tenant ID (a GUID) used to request tokens. Required when azure is listed in providers, here or in the trigger's settings; a value that isn't a GUID refuses the request.",
            "required": false
        },
        {
            "name": "azure_client_id",
            "example": "11111111-1111-1111-1111-111111111111",
            "description": "Application (client) ID (a GUID) of the service principal. Required when azure is listed in providers, here or in the trigger's settings; a value that isn't a GUID refuses the request.",
            "required": false
        },
        {
            "name": "azure_subscription_ids",
            "example": "['22222222-2222-2222-2222-222222222222']",
            "description": "Azure subscriptions to collect: a JSON array (a space-separated string also works) of subscription IDs (GUIDs), at least one. Required when azure is listed in providers, here or in the trigger's settings; an item that isn't a GUID refuses the request. Every Azure OpenAI and AI Services resource of each subscription is discovered.",
            "required": false
        },
        {
            "name": "azure_account",
            "example": "azure-main",
            "description": "Free-text label written as the account tag of Azure rows. Default: the subscription ID. One label for several subscriptions sums their rows.",
            "required": false
        },
        {
            "name": "google_project_ids",
            "example": "['my-project-1', 'my-project-2']",
            "description": "Google Cloud project IDs to read Vertex AI and Gemini API usage and cost from: a JSON array (a space-separated string also works) of project IDs such as my-project-1 (6 to 30 lower-case letters, digits and hyphens, starting with a letter and not ending with a hyphen; a domain-scoped ID such as example.com:my-project is also accepted), at least one. Required when google is listed in providers, here or in the trigger's settings; an ID of another form refuses the request.",
            "required": false
        },
        {
            "name": "google_account",
            "example": "gcp-main",
            "description": "Free-text label written as the account tag of Google rows. Default: default.",
            "required": false
        },
        {
            "name": "google_sources",
            "example": "['vertex_metrics', 'billing_export']",
            "description": "Sources to read in every project: a JSON array (a space-separated string also works) of vertex_metrics (Vertex AI usage), gemini_api_metrics (Gemini API usage) and billing_export (cost from the Cloud Billing export in BigQuery), at least one. Default: vertex_metrics gemini_api_metrics. Google cost is collected only with billing_export, which needs google_billing_table.",
            "required": false
        },
        {
            "name": "google_billing_table",
            "example": "my-billing-project.billing_export.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX",
            "description": "BigQuery table of the standard usage cost export of the Cloud Billing account, as <project>.<dataset>.<table>: a project ID as in google_project_ids, a dataset name of 1 to 1024 letters, digits and _, a table name of 1 to 1024 letters, digits, _ and -. Required when billing_export is listed in google_sources; a value in another form refuses the request, whether or not billing_export is listed.",
            "required": false
        }
    ]
}
"""

import hashlib
import json
import math
import os
import re
import time
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import Any

import requests

from influxdata_plugin_utils.config import load_config, merge_config_layers
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_timedelta,
)
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_json_body,
    parse_query_parameters,
    parse_request_headers,
    parse_toml,
    parse_trigger_args,
    resolve_path,
)
from influxdata_plugin_utils.validation import Validator
from influxdata_plugin_utils.cache import cached
from influxdata_plugin_utils.write import build_line_typed, write_data

# --- settings ---

PROVIDERS = ("anthropic", "openai", "bedrock", "azure", "google")
GOOGLE_SOURCES = ("vertex_metrics", "gemini_api_metrics", "billing_export")
GOOGLE_DEFAULT_SOURCES = ("vertex_metrics", "gemini_api_metrics")
COLLECT_KINDS = ("usage", "cost")
IDENTIFIER_TAGS = ("account", "project", "api_key_id", "user_id")
USAGE_UNITS = {
    "input_tokens": "tokens",
    "cache_read_tokens": "tokens",
    "cache_write_tokens": "tokens",
    "output_tokens": "tokens",
    "requests": "requests",
    "images": "images",
    "audio_seconds": "seconds",
    "tts_characters": "characters",
    "web_search_requests": "requests",
    "file_search_requests": "requests",
    "code_interpreter_sessions": "sessions",
    "vector_store_bytes": "bytes",
}
BUCKET_WIDTHS = {"1h": timedelta(hours=1), "1d": timedelta(days=1)}
DATABASE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


def is_table_name(value: str) -> bool:
    """
    Printable, no whitespace (LineBuilder refuses spaces), no backslash (it escapes the next separator)
    and no leading # (the server reads such a line as a comment and drops it).
    """
    return (
        bool(value)
        and value.isprintable()
        and not value.startswith("#")
        and not any(char.isspace() or char == "\\" for char in value)
    )

ENV_PREFIX = "INFLUXDB3_LLM_USAGE_"
HEADER_PREFIX = "X-Influxdb3-Llm-Usage-"

# provider -> settings that must be present when that provider is listed
REQUIRED_PROVIDER_SETTINGS = {
    "bedrock": ("bedrock_regions",),
    "azure": ("azure_tenant_id", "azure_client_id", "azure_subscription_ids"),
    "google": ("google_project_ids",),
}


class ConfigError(ValueError):
    """A setting is unknown, missing or has an unusable value."""


def trimmed(value) -> str:
    """A setting as trimmed text; TOML may deliver it as a number."""
    return str(value).strip()


def list_of(allowed_values, what: str):
    """
    Cast a space-separated string or a real list into a list of allowed values.

    ``allowed_values`` is a callable, so the allowed set is read when the
    setting is validated, not when the validator is built.
    """

    def cast(value) -> list[str]:
        items = list(dict.fromkeys(parse_delimited_list(value, sep=" ")))
        unknown = [item for item in items if item not in allowed_values()]
        if unknown:
            allowed = " ".join(allowed_values())
            raise ValueError(f"unknown {what} {' '.join(unknown)}; allowed: {allowed}")
        return items

    return cast


def non_empty_list(value) -> list[str]:
    """A space-separated string or a real list, without blank or repeated items."""
    return list(dict.fromkeys(parse_delimited_list(value, sep=" ")))


def parse_instant(value) -> datetime:
    """An ISO 8601 timestamp with ``Z`` or an offset as aware UTC; naive input is UTC."""
    if isinstance(value, datetime):
        instant = value
    else:
        try:
            instant = datetime.fromisoformat(trimmed(value).replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"not an ISO 8601 timestamp: {value!r}") from None
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(timezone.utc)


COMMON_VALIDATORS = [
    Validator(
        "providers",
        required=True,
        cast=list_of(lambda: PROVIDERS, "provider"),
        len_min=1,
    ),
    Validator(
        "collect",
        default=list(COLLECT_KINDS),
        cast=list_of(lambda: COLLECT_KINDS, "value"),
        len_min=1,
    ),
    Validator(
        "usage_types",
        default=list(USAGE_UNITS),
        cast=list_of(lambda: USAGE_UNITS, "usage type"),
        len_min=1,
    ),
    Validator("target_database", cast=trimmed),
    Validator("usage_table", default="llm_usage", cast=trimmed),
    Validator("cost_table", default="llm_cost", cast=trimmed),
    Validator("bucket_width", default="1h", cast=trimmed, is_in=tuple(BUCKET_WIDTHS)),
    Validator(
        "exclude_tags", default=[], cast=list_of(lambda: IDENTIFIER_TAGS, "tag")
    ),
    Validator(
        "usage_lookback",
        default=timedelta(hours=48),
        cast=parse_timedelta,
        gt=timedelta(0),
    ),
    Validator(
        "cost_lookback",
        default=timedelta(days=40),
        cast=parse_timedelta,
        gte=timedelta(days=1),
    ),
    Validator(
        "cost_interval",
        default=timedelta(hours=6),
        cast=parse_timedelta,
        gt=timedelta(0),
    ),
    Validator(
        "max_run_time",
        default=timedelta(minutes=15),
        cast=parse_timedelta,
        gt=timedelta(0),
    ),
    Validator("start", cast=parse_instant),
    Validator("end", cast=parse_instant),
    Validator("config_file_path", cast=trimmed),
]

PROVIDER_VALIDATORS = [
    Validator("anthropic_admin_key_env", default="ANTHROPIC_ADMIN_KEY", cast=trimmed),
    Validator("anthropic_account", default="default", cast=trimmed),
    Validator("openai_admin_key_env", default="OPENAI_ADMIN_KEY", cast=trimmed),
    Validator("openai_account", default="default", cast=trimmed),
    Validator(
        "bedrock_access_key_id_env",
        default="BEDROCK_AWS_ACCESS_KEY_ID",
        cast=trimmed,
    ),
    Validator(
        "bedrock_secret_access_key_env",
        default="BEDROCK_AWS_SECRET_ACCESS_KEY",
        cast=trimmed,
    ),
    Validator(
        "bedrock_session_token_env",
        default="BEDROCK_AWS_SESSION_TOKEN",
        cast=trimmed,
    ),
    Validator("bedrock_regions", cast=non_empty_list),
    Validator("bedrock_use_default_credentials", default=False, cast=parse_bool),
    Validator("bedrock_account", cast=trimmed),
    Validator("azure_client_secret_env", default="AZURE_CLIENT_SECRET", cast=trimmed),
    Validator("azure_tenant_id", cast=trimmed),
    Validator("azure_client_id", cast=trimmed),
    Validator("azure_subscription_ids", cast=non_empty_list),
    Validator("azure_account", cast=trimmed),
    Validator(
        "google_credentials_env",
        default="GOOGLE_LLM_USAGE_CREDENTIALS",
        cast=trimmed,
    ),
    Validator("google_project_ids", cast=non_empty_list),
    Validator("google_account", default="default", cast=trimmed),
    Validator(
        "google_sources",
        default=list(GOOGLE_DEFAULT_SOURCES),
        cast=list_of(lambda: GOOGLE_SOURCES, "source"),
        len_min=1,
    ),
    Validator("google_billing_table", cast=trimmed),
]

CONFIG_VALIDATORS = COMMON_VALIDATORS + PROVIDER_VALIDATORS

COMMON_NAMES = [name for v in COMMON_VALIDATORS for name in v.names]
PROVIDER_NAMES = [name for v in PROVIDER_VALIDATORS for name in v.names]
SETTING_NAMES = COMMON_NAMES + PROVIDER_NAMES

# each trigger type reads only its own settings; the other type's are unknown keys
SCHEDULED_NAMES = [name for name in SETTING_NAMES if name not in ("start", "end")]
HTTP_NAMES = [
    name for name in SETTING_NAMES if name not in ("usage_lookback", "cost_lookback", "cost_interval")
]

# settings naming an environment variable, and the switch to the server's own AWS
# credentials, never come from a request; the config file path comes only from a
# trigger argument
SECRET_NAME_SETTINGS = [name for name in SETTING_NAMES if name.endswith("_env")]
REQUEST_FORBIDDEN_SETTINGS = (*SECRET_NAME_SETTINGS, "bedrock_use_default_credentials", "config_file_path")
REQUEST_NAMES = [name for name in HTTP_NAMES if name not in REQUEST_FORBIDDEN_SETTINGS]


def header_name(name: str) -> str:
    """The header a setting is spelled as, prefixed and hyphenated."""
    return HEADER_PREFIX + name.replace("_", "-").title()


def env_spec(names: Sequence[str]) -> KeySpec:
    """Settings read from ``INFLUXDB3_LLM_USAGE_<SETTING>``."""
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


def header_spec(names: Sequence[str]) -> KeySpec:
    """
    Settings read from ``X-Influxdb3-Llm-Usage-<Setting>`` headers.

    A header outside this list is dropped rather than refused: a client sends
    headers of its own on every request.
    """
    return KeySpec(
        allowlist=tuple(header_name(name) for name in names),
        rename={header_name(name): name for name in names},
    )


@dataclass
class Settings:
    """Validated settings of one trigger run."""

    providers: list[str]
    collect: list[str]
    usage_types: list[str]
    target_database: str | None
    usage_table: str
    cost_table: str
    bucket_width: str
    exclude_tags: list[str]
    usage_lookback: timedelta
    cost_lookback: timedelta
    cost_interval: timedelta
    max_run_time: timedelta
    start: datetime | None
    end: datetime | None
    provider: dict[str, Any]


def bucket_delta(bucket_width: str) -> timedelta:
    """The length of one usage bucket."""
    return BUCKET_WIDTHS[bucket_width]


def read_secret(env_name: str) -> str:
    """The secret held by an environment variable, never echoed in an error."""
    value = os.environ.get(env_name, "").strip()
    if not value:
        raise ConfigError(f"environment variable {env_name} is not set")
    if any(unicodedata.category(char) == "Cc" for char in value):
        raise ConfigError(
            f"environment variable {env_name} contains a control character"
        )
    return value


def check_request_providers(trigger_layers: list[dict], request_layers: list[dict]):
    """A request may name only providers the trigger itself is configured with."""
    requested = merge_config_layers(*request_layers).get("providers")
    if requested is None:
        return
    configured = parse_delimited_list(
        merge_config_layers(*trigger_layers).get("providers", []), sep=" "
    )
    outside = [
        provider
        for provider in parse_delimited_list(requested, sep=" ")
        if provider not in configured
    ]
    if outside:
        raise ConfigError(
            f"providers {' '.join(outside)} are not configured on this trigger"
        )


def check_relations(config: dict, http: bool, now: datetime) -> None:
    """Table and database names, and rules that span settings, run on the validated values."""
    database = config.get("target_database")
    if database is not None and not DATABASE_NAME.fullmatch(database):
        raise ConfigError(
            f"target_database: invalid name {database}; use letters, digits, _ and -, starting with a letter or digit"
        )
    for name in ("usage_table", "cost_table"):
        if not is_table_name(config[name]):
            raise ConfigError(
                f"{name}: invalid name {config[name]!r}; spaces, control characters and backslashes aren't allowed, "
                f"and a name can't start with #"
            )
    if config["usage_table"] == config["cost_table"]:
        raise ConfigError("usage_table and cost_table must differ")
    if not http and config["usage_lookback"] < bucket_delta(config["bucket_width"]):
        raise ConfigError(
            f"usage_lookback must be at least one bucket ({config['bucket_width']})"
        )
    if http:
        if "start" not in config:
            raise ConfigError("start is required for an HTTP request")
        config.setdefault("end", now)
        if config["end"] <= config["start"]:
            raise ConfigError("end must be after start")


def load_settings(
    influxdb3_local,
    task_id: str,
    args: dict | None,
    *,
    request_body=None,
    request_headers=None,
    query_parameters=None,
    http: bool = False,
    now: datetime | None = None,
) -> Settings:
    """
    Load settings from every layer and check the configuration of every provider of the run.

    Priority, lowest first: environment variables ``INFLUXDB3_LLM_USAGE_<SETTING>``,
    trigger arguments, TOML file, request body, request headers
    ``X-Influxdb3-Llm-Usage-<Setting>``, query parameters. Request layers are
    read only when ``http`` is true and never carry a ``*_env`` setting or
    ``config_file_path``. Each trigger type reads only its own settings
    (``SCHEDULED_NAMES`` or ``HTTP_NAMES``); the other type's are unknown keys.

    Raises:
        ConfigError: unknown key, unusable value, an HTTP request without a valid start and end,
            or a provider of the run whose settings, libraries or secrets are unusable.
    """
    args = args or {}
    now = now or datetime.now(timezone.utc)
    names = HTTP_NAMES if http else SCHEDULED_NAMES
    stored_names = [name for name in names if name != "config_file_path"]
    try:
        config_file_path = trimmed(args.get("config_file_path") or "")
        trigger_layers = [
            parse_env(env_spec(stored_names)),
            parse_trigger_args(args, KeySpec(allowlist=names, unknown="reject")),
            parse_toml(config_file_path, KeySpec(allowlist=stored_names, unknown="reject")),
        ]
        request_layers = []
        if http:
            request_keys = KeySpec(allowlist=REQUEST_NAMES, unknown="reject")
            request_layers = [
                parse_json_body(request_body, request_keys),
                parse_request_headers(request_headers, header_spec(REQUEST_NAMES)),
                parse_query_parameters(query_parameters, request_keys),
            ]
            check_request_providers(trigger_layers, request_layers)
        config = dict(
            load_config(*trigger_layers, *request_layers, validators=CONFIG_VALIDATORS)
        )
        check_relations(config, http, now)
        settings = Settings(
            providers=config["providers"],
            collect=config["collect"],
            usage_types=config["usage_types"],
            target_database=config.get("target_database"),
            usage_table=config["usage_table"],
            cost_table=config["cost_table"],
            bucket_width=config["bucket_width"],
            exclude_tags=config["exclude_tags"],
            usage_lookback=config["usage_lookback"],
            cost_lookback=config["cost_lookback"],
            cost_interval=config["cost_interval"],
            max_run_time=config["max_run_time"],
            start=config.get("start"),
            end=config.get("end"),
            provider={name: config.get(name) for name in PROVIDER_NAMES},
        )
        for name in settings.providers:
            CONNECTORS[name].check_config(settings)
    except ConfigError:
        raise
    except ValueError as error:
        raise ConfigError(str(error)) from error

    if config_file_path:
        influxdb3_local.info(f"[{task_id}] Loaded configuration from {config_file_path}")
    return settings


# --- records and writer ---

USAGE_TAG_ORDER = (
    "account",
    "model",
    "project",
    "region",
    "service",
    "api_key_id",
    "user_id",
)
COST_TAG_ORDER = (
    "account",
    "project",
    "region",
    "service",
    "api_key_id",
    "user_id",
    "model",
)


@dataclass
class UsageRecord:
    """Usage of one provider, usage type and tag set in one bucket."""

    time: datetime
    provider: str
    usage_type: str
    quantity: float
    tags: dict[str, str]
    api_key_name: str | None = None


@dataclass
class CostRecord:
    """Cost of one provider, billing item and tag set on one day."""

    time: datetime
    provider: str
    billing_item: str
    currency: str
    cost: float
    tags: dict[str, str]
    api_key_name: str | None = None


MAX_VALUE_TEXT = 50


def record_amount(record: UsageRecord | CostRecord) -> Decimal:
    """
    The quantity or cost of a record as a Decimal.

    Raises:
        ValueError: the provider gave a value that isn't a finite number.
    """
    is_usage = isinstance(record, UsageRecord)
    value = record.quantity if is_usage else record.cost
    try:
        amount = Decimal(str(value))
    except ArithmeticError:
        amount = None
    if amount is None or not amount.is_finite():
        item = record.usage_type if is_usage else record.billing_item
        raise ValueError(
            f"not a number: {repr(value)[:MAX_VALUE_TEXT]} ({record.provider} {item} at {iso_utc(record.time)})"
        )
    return amount


def prepare(
    records: Iterable[UsageRecord | CostRecord], exclude_tags: Sequence[str]
) -> list:
    """
    Drop excluded and empty tags, add up rows that become equal, drop zero rows.

    Raises:
        ValueError: a value isn't a finite number.
    """
    groups: dict[tuple, list] = {}
    for record in records:
        tags = {
            key: value
            for key, value in record.tags.items()
            if value and key not in exclude_tags
        }
        is_usage = isinstance(record, UsageRecord)
        identity = (
            (record.usage_type, "") if is_usage else (record.billing_item, record.currency.upper())
        )
        key = (is_usage, record.time, record.provider, identity, tuple(sorted(tags.items())))
        group = groups.setdefault(key, [record, tags, Decimal(0), set()])
        group[2] += record_amount(record)
        if "api_key_id" not in exclude_tags:
            group[3].add(record.api_key_name)

    prepared = []
    for record, tags, total, names in groups.values():
        if total == 0:
            continue
        api_key_name = next(iter(names)) if len(names) == 1 else None
        amount = {"quantity" if isinstance(record, UsageRecord) else "cost": float(total)}
        prepared.append(replace(record, tags=tags, api_key_name=api_key_name, **amount))
    return prepared


def build_usage_line(table: str, record: UsageRecord):
    tags = {
        "provider": record.provider,
        **{name: record.tags.get(name) for name in USAGE_TAG_ORDER},
        "usage_type": record.usage_type,
        "unit": USAGE_UNITS[record.usage_type],
    }
    fields = {"quantity": (record.quantity, "float"), "api_key_name": (record.api_key_name or None, "string")}
    return build_line_typed(
        LineBuilder, table, tags=tags, typed_fields=fields, time_ns=int(record.time.timestamp()) * 10**9
    )


def build_cost_line(table: str, record: CostRecord):
    tags = {
        "provider": record.provider,
        **{name: record.tags.get(name) for name in COST_TAG_ORDER},
        "billing_item": record.billing_item,
        "currency": record.currency.upper(),
    }
    fields = {"cost": (record.cost, "float"), "api_key_name": (record.api_key_name or None, "string")}
    return build_line_typed(
        LineBuilder, table, tags=tags, typed_fields=fields, time_ns=int(record.time.timestamp()) * 10**9
    )


WRITE_BATCH_LINES = 5000


class WriteFailed(Exception):
    """A write failed after ``written`` rows (usage first, then cost) had landed."""

    def __init__(self, error: Exception, written: int):
        super().__init__(str(error))
        self.written = written


def write_records(
    influxdb3_local,
    settings: Settings,
    usage: list[UsageRecord],
    cost: list[CostRecord],
) -> tuple[int, int]:
    """
    Write usage and cost rows to the target database in batches; return rows per table.

    Raises:
        WriteFailed: a batch was rejected (writes are synchronous); earlier batches stay written.
    """
    lines = [build_usage_line(settings.usage_table, record) for record in usage]
    lines += [build_cost_line(settings.cost_table, record) for record in cost]
    for first in range(0, len(lines), WRITE_BATCH_LINES):
        batch = lines[first : first + WRITE_BATCH_LINES]
        try:
            write_data(influxdb3_local, batch, retries=1, no_sync=True, database=settings.target_database)
        except Exception as error:
            raise WriteFailed(error, first) from error
    return len(usage), len(cost)


# --- http client ---

COST_MANAGEMENT_RETRY_PREFIX = "x-ms-ratelimit-microsoft.costmanagement-"
MAX_ERROR_TEXT = 200
MAX_RETRY_AFTER_SECONDS = 120.0
MIN_SECRET_LENGTH = 8
# every provider call is tried at most this many times, the first one included
MAX_ATTEMPTS = 3
# a call started shortly before the deadline still gets this long to answer
MIN_CALL_TIMEOUT = 1.0
TRANSPORT_ERRORS = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)


class ProviderError(Exception):
    """A provider call failed; the message never contains request headers."""

    def __init__(self, status: int | None, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class RunTimeExceeded(Exception):
    """The provider's share of max_run_time is used up: no further call of the run starts."""

    def __init__(self):
        super().__init__("max_run_time reached")


def missing_page_cursor(field: str) -> ProviderError:
    """A page said more follow but gave no cursor: fail the chunk rather than stop short silently."""
    return ProviderError(None, f"page cursor missing: has_more without {field}")


def next_page_cursor(body: dict, field: str, seen: set, fallback: str | None = None) -> str:
    """The cursor to request next; a missing or already used one fails the chunk instead of stopping short or looping."""
    cursor = body.get(field) or fallback
    if not cursor:
        raise missing_page_cursor(field)
    if cursor in seen:
        raise ProviderError(None, f"page cursor repeated: has_more with a {field} already followed")
    seen.add(cursor)
    return cursor


def next_page_token(token: str | None, field: str, seen: set) -> str | None:
    """The token of the next page, or None after the last; a token already followed fails instead of looping."""
    if not token:
        return None
    if token in seen:
        raise ProviderError(None, f"page cursor repeated: {field} already followed")
    seen.add(token)
    return token


def provider_failure_reason(error: ProviderError) -> str:
    """The error message: the HTTP status with the provider's masked text, or a transport error's class name."""
    return error.message or f"HTTP {error.status}"


def is_retry_after_header(name: str) -> bool:
    """Retry-After, or one of Azure Cost Management's per-quota x-ms-ratelimit-microsoft.costmanagement-<quota>-retry-after."""
    name = name.lower()
    return name == "retry-after" or (name.startswith(COST_MANAGEMENT_RETRY_PREFIX) and name.endswith("-retry-after"))


def retry_delay(headers, attempt: int) -> float:
    """Seconds to wait: the longest retry-after the provider sent, clamped to 0-120, else 1, 2, 4... seconds."""
    waits = []
    for name, value in headers.items():
        if not is_retry_after_header(name):
            continue
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(seconds):
            waits.append(seconds)
    if waits:
        return min(max(max(waits), 0.0), MAX_RETRY_AFTER_SECONDS)
    return float(2**attempt)


def provider_error_text(response, secrets: Sequence[str]) -> str:
    """The provider's own error message, shortened and stripped of any request secret."""
    try:
        error = response.json().get("error")
        text = error.get("message") if isinstance(error, dict) else error
    except (ValueError, AttributeError):
        text = None
    text = str(text or "")
    for secret in secrets:
        text = text.replace(secret, "***")
    return text[:MAX_ERROR_TEXT]


def request_secrets(headers, data) -> list[str]:
    """Header values, the token after a ``Bearer `` prefix and string form values of a request."""
    values = [str(value) for value in (headers or {}).values()]
    values += [value.removeprefix("Bearer ") for value in values if value.startswith("Bearer ")]
    if isinstance(data, dict):
        values += [value for value in data.values() if isinstance(value, str)]
    return sorted({value for value in values if len(value) >= MIN_SECRET_LENGTH}, key=len, reverse=True)


class HttpClient:
    """JSON over HTTP with retries inside an optional deadline; errors never quote request headers."""

    def __init__(self, session=None, *, sleep=time.sleep, max_attempts: int = MAX_ATTEMPTS, timeout: float = 60):
        self.session = session or requests.Session()
        self.sleep = sleep
        self.max_attempts = max_attempts
        self.timeout = timeout
        # time.monotonic() value after which no call or retry starts; None means no limit
        self.deadline: float | None = None

    def time_left(self) -> float:
        """Seconds until the deadline; infinite without one."""
        return math.inf if self.deadline is None else self.deadline - time.monotonic()

    def check_deadline(self) -> None:
        """
        Refuse to start work once the deadline has passed.

        Raises:
            RunTimeExceeded: the deadline has passed.
        """
        if self.time_left() <= 0:
            raise RunTimeExceeded()

    def request_json(self, method: str, url: str, *, headers=None, params=None, json=None, data=None) -> dict:
        """
        Call the API and return the JSON body; HTTP 204 (no body) returns an empty dict.

        A call is tried at most ``max_attempts`` times. After HTTP 429 or 5xx it waits as the
        provider asks (``retry_delay``), after a connection error or timeout 1, 2, ... seconds.
        No attempt starts after the deadline, a retry whose wait would outlast it isn't made,
        and an attempt waits for its answer no longer than the time left.

        Raises:
            RunTimeExceeded: the deadline passed before an attempt.
            ProviderError: any other non-2xx status, a transport failure or a non-JSON body.
        """
        failures = 0
        while True:
            self.check_deadline()
            timeout = min(self.timeout, max(self.time_left(), MIN_CALL_TIMEOUT))
            try:
                response = self.session.request(
                    method, url, headers=headers, params=params, json=json, data=data, timeout=timeout
                )
            except TRANSPORT_ERRORS as error:
                failure = ProviderError(None, type(error).__name__)
                delay = float(2**failures)
            except requests.RequestException as error:
                raise ProviderError(None, type(error).__name__) from None
            else:
                status = response.status_code
                if status == 204:
                    # No Content: an empty result, as Azure Cost Management's query documents it
                    return {}
                if 200 <= status < 300:
                    try:
                        return response.json()
                    except ValueError:
                        raise ProviderError(status, "response is not JSON") from None
                text = provider_error_text(response, request_secrets(headers, data))
                failure = ProviderError(status, f"HTTP {status}: {text}" if text else f"HTTP {status}")
                if not (status == 429 or 500 <= status <= 599):
                    raise failure
                delay = retry_delay(response.headers, failures)
            failures += 1
            if failures >= self.max_attempts or delay >= self.time_left():
                raise failure
            self.sleep(delay)


def make_http_client() -> HttpClient:
    return HttpClient(requests.Session())


# --- connectors ---


class Connector:
    """One provider: checks its configuration and yields normalized records."""

    name: str
    usage_types: frozenset[str]

    def __init__(self, settings: Settings, http: HttpClient, local):
        self.settings = settings
        self.http = http
        self.local = local
        self.errors: list[str] = []
        self.notes: list[str] = []
        self.resolved_key_names: dict[str, str] | None = None
        self.task_id = ""

    def warn(self, message: str) -> None:
        self.local.warn(f"[{self.task_id}] {self.name}: {message}")

    @classmethod
    def check_config(cls, settings: Settings) -> None:
        """Raise ConfigError when this provider's settings, libraries or secrets are unusable, without calling it."""

    @classmethod
    def require_settings(cls, settings: Settings) -> None:
        """Raise ConfigError naming the provider settings that are missing."""
        missing = [name for name in REQUIRED_PROVIDER_SETTINGS.get(cls.name, ()) if not settings.provider.get(name)]
        if missing:
            raise ConfigError(f"{' '.join(missing)} required when {cls.name} is listed in providers")

    def collects_cost(self) -> bool:
        """False for a provider that has no cost source; its cost section is skipped."""
        return True

    def oldest_usage(self, now: datetime) -> datetime | None:
        """The oldest moment usage can be served from, or None when unlimited."""
        return None

    def oldest_cost(self, now: datetime) -> date | None:
        """The oldest day cost can be served from, or None when unlimited."""
        return None

    def fetch_usage(
        self, start: datetime, end: datetime, bucket_width: str, usage_types: list[str]
    ) -> Iterator[UsageRecord]:
        raise NotImplementedError

    def fetch_cost(self, start_day: date, end_day: date) -> Iterator[CostRecord]:
        """Cost per day from start_day up to, not including, end_day."""
        raise NotImplementedError

    def closing_notes(self) -> list[str]:
        """Notes that sum up the whole run, read once after every fetch."""
        return []

    def record_chunk_failure(self, endpoint: str, chunk_start: str, chunk_end: str, error: ProviderError) -> None:
        """Note a skipped chunk by endpoint, period and failure reason."""
        self.errors.append(f"{endpoint} chunk {chunk_start} to {chunk_end} skipped: {provider_failure_reason(error)}")

    def collect_pages(self, url: str, params: dict) -> list[dict]:
        """Rows of every page of a cursor-paged GET, following `next_page` while `has_more`."""
        rows: list[dict] = []
        page, seen = None, set()
        while True:
            page_params = dict(params, page=page) if page else dict(params)
            body = self.http.request_json("GET", url, headers=self.headers(), params=page_params)
            rows.extend(body.get("data") or [])
            if not body.get("has_more"):
                return rows
            page = next_page_cursor(body, "next_page", seen)

    def key_name(self, row: dict) -> str | None:
        """Name of the row's API key; none when the key is unknown or `api_key_id` is excluded."""
        key_id = row.get("api_key_id")
        if not key_id or "api_key_id" in self.settings.exclude_tags:
            return None
        return self.key_names().get(key_id)

    def cached_key_names(self, cache_key: str, load: Callable[[], dict[str, str]]) -> dict[str, str]:
        """Key id to name from the trigger cache (an hour) or `load`; a failed lookup leaves names out."""
        if self.resolved_key_names is None:
            try:
                self.resolved_key_names = cached(self.local, cache_key, load, ttl_seconds=KEY_NAMES_TTL_SECONDS)
            except RunTimeExceeded:
                raise
            except Exception as error:
                reason = provider_failure_reason(error) if isinstance(error, ProviderError) else type(error).__name__
                self.warn(f"API key names unavailable ({reason})")
                self.resolved_key_names = {}
        return self.resolved_key_names


CONNECTORS: dict[str, type[Connector]] = {}
KEY_NAMES_TTL_SECONDS = 3600


def period_chunks(start: datetime | date, end: datetime | date, span: timedelta) -> Iterator[tuple]:
    """Half-open chunks of at most `span` that tile start..end (moments or days) without overlap."""
    while start < end:
        chunk_end = min(start + span, end)
        yield start, chunk_end
        start = chunk_end


def first_day_months_back(now: datetime, months: int) -> date:
    """The first day of the month `months` months before the month of `now`."""
    month_index = now.year * 12 + now.month - 1 - months
    return date(month_index // 12, month_index % 12 + 1, 1)


def register(cls: type[Connector]) -> type[Connector]:
    CONNECTORS[cls.name] = cls
    return cls


# --- anthropic ---

ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1/organizations"
ANTHROPIC_VERSION = "2026-10-21"
ANTHROPIC_BETA = "analytics-usage-cost-2026-08-24"
ANTHROPIC_HISTORY_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ANTHROPIC_HISTORY_DAYS = 365
ANTHROPIC_MAX_SPAN = timedelta(days=31)
ANTHROPIC_PAGE_LIMIT = 1000
ANTHROPIC_MAX_RESTARTS = 3
ANTHROPIC_KEY_PAGE_LIMIT = 100
ANTHROPIC_KEY_NAMES_CACHE_KEY = "api_key_names:anthropic"


def iso_utc(instant: datetime) -> str:
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


@register
class AnthropicConnector(Connector):
    """Anthropic usage and cost from the Usage and Cost Analytics API."""

    name = "anthropic"
    usage_types = frozenset(
        {
            "input_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "output_tokens",
            "requests",
            "web_search_requests",
        }
    )

    @classmethod
    def check_config(cls, settings: Settings) -> None:
        read_secret(settings.provider["anthropic_admin_key_env"])

    def oldest_usage(self, now: datetime) -> datetime:
        return max(ANTHROPIC_HISTORY_START, now - timedelta(days=ANTHROPIC_HISTORY_DAYS))

    def oldest_cost(self, now: datetime) -> date:
        """The first whole day inside the history window."""
        oldest = self.oldest_usage(now)
        day = oldest.date()
        return day if oldest == datetime.combine(day, datetime.min.time(), timezone.utc) else day + timedelta(days=1)

    def headers(self, beta: bool = True) -> dict[str, str]:
        headers = {
            "x-api-key": read_secret(self.settings.provider["anthropic_admin_key_env"]),
            "anthropic-version": ANTHROPIC_VERSION,
        }
        if beta:
            headers["anthropic-beta"] = ANTHROPIC_BETA
        return headers

    def group_by(self, cost: bool) -> list[str]:
        excluded = self.settings.exclude_tags
        dimensions = ["model"]
        if "project" not in excluded:
            dimensions.append("workspace_id")
        if "api_key_id" not in excluded:
            dimensions.append("api_key_id")
        if "user_id" not in excluded:
            dimensions += ["actor", "actor_type"]
        if cost:
            dimensions += ["cost_type", "token_type"]
        return dimensions

    def fetch_pages(self, endpoint: str, params: dict) -> list[dict]:
        """All rows of one chunk, or none when it fails (recorded in errors).

        A cursor expired by a data refresh (410) restarts the chunk.
        """
        for restart in range(ANTHROPIC_MAX_RESTARTS + 1):
            try:
                return self.collect_pages(f"{ANTHROPIC_BASE_URL}/analytics/{endpoint}", params)
            except ProviderError as error:
                if error.status != 410 or restart == ANTHROPIC_MAX_RESTARTS:
                    self.record_chunk_failure(endpoint, params["starting_at"], params["ending_at"], error)
                    return []
        raise AssertionError("unreachable")

    def key_names(self) -> dict[str, str]:
        return self.cached_key_names(ANTHROPIC_KEY_NAMES_CACHE_KEY, self.load_key_names)

    def load_key_names(self) -> dict[str, str]:
        names: dict[str, str] = {}
        params: dict[str, Any] = {"limit": ANTHROPIC_KEY_PAGE_LIMIT}
        seen: set[str] = set()
        while True:
            body = self.http.request_json(
                "GET", f"{ANTHROPIC_BASE_URL}/api_keys", headers=self.headers(beta=False), params=dict(params)
            )
            for key in body.get("data") or []:
                if key.get("id") and key.get("name"):
                    names[key["id"]] = key["name"]
            if not body.get("has_more"):
                return names
            params["after_id"] = next_page_cursor(body, "last_id", seen)

    def tags(self, group: dict) -> dict[str, str]:
        tags = {
            "account": self.settings.provider["anthropic_account"],
            "model": group.get("model"),
            "project": group.get("workspace_id"),
            "api_key_id": group.get("api_key_id"),
            "user_id": self.user_id(group),
        }
        return {key: value for key, value in tags.items() if value}

    @staticmethod
    def user_id(group: dict) -> str | None:
        """The acting identity's id; the actor's email address and name are never read."""
        actor = group.get("actor")
        if not isinstance(actor, dict):
            return None
        if group.get("actor_type") == "user_actor" or actor.get("type") == "user_actor":
            return actor.get("user_id")
        return actor.get("service_account_id") or actor.get("id")

    def fetch_usage(
        self, start: datetime, end: datetime, bucket_width: str, usage_types: list[str]
    ) -> Iterator[UsageRecord]:
        for chunk_start, chunk_end in period_chunks(start, end, ANTHROPIC_MAX_SPAN):
            params = {
                "starting_at": iso_utc(chunk_start),
                "ending_at": iso_utc(chunk_end),
                "time_bucket_width": bucket_width,
                "group_by[]": self.group_by(cost=False),
                "order_by": "time",
                "order": "asc",
                "limit": ANTHROPIC_PAGE_LIMIT,
            }
            for row in self.fetch_pages("usage", params):
                group, usage = row.get("group") or {}, row.get("usage") or {}
                creation = usage.get("cache_creation") or {}
                quantities = {
                    "input_tokens": usage.get("uncached_input_tokens"),
                    "cache_read_tokens": usage.get("cache_read_input_tokens"),
                    "cache_write_tokens": (creation.get("ephemeral_5m_input_tokens") or 0)
                    + (creation.get("ephemeral_1h_input_tokens") or 0),
                    "output_tokens": usage.get("output_tokens"),
                    "requests": usage.get("requests"),
                    "web_search_requests": (usage.get("server_tool_use") or {}).get("web_search_requests"),
                }
                for usage_type in usage_types:
                    yield UsageRecord(
                        parse_instant(row["starting_at"]),
                        self.name,
                        usage_type,
                        quantities.get(usage_type) or 0,
                        self.tags(group),
                        self.key_name(group),
                    )

    def fetch_cost(self, start_day: date, end_day: date) -> Iterator[CostRecord]:
        start = datetime.combine(start_day, datetime.min.time(), timezone.utc)
        end = datetime.combine(end_day, datetime.min.time(), timezone.utc)
        for chunk_start, chunk_end in period_chunks(start, end, ANTHROPIC_MAX_SPAN):
            params = {
                "starting_at": iso_utc(chunk_start),
                "ending_at": iso_utc(chunk_end),
                "time_bucket_width": "1d",
                "group_by[]": self.group_by(cost=True),
                "order_by": "time",
                "order": "asc",
                "limit": ANTHROPIC_PAGE_LIMIT,
            }
            for row in self.fetch_pages("cost", params):
                group, cost = row.get("group") or {}, row.get("cost") or {}
                if cost.get("amount") is None:
                    continue
                billing_item = "/".join(part for part in (group.get("cost_type"), group.get("token_type")) if part)
                yield CostRecord(
                    parse_instant(row["starting_at"]),
                    self.name,
                    billing_item,
                    cost.get("currency") or "USD",
                    float(Decimal(cost["amount"]) / 100),
                    self.tags(group),
                    self.key_name(group),
                )


# --- openai ---

OPENAI_BASE_URL = "https://api.openai.com/v1/organization"
OPENAI_HISTORY_DAYS = 365
OPENAI_USAGE_BUCKET_LIMITS = {"1h": 168, "1d": 31}
OPENAI_COST_BUCKET_LIMIT = 180
OPENAI_KEY_PAGE_LIMIT = 100
OPENAI_KEY_NAMES_CACHE_KEY = "api_key_names:openai"
OPENAI_ALL_DIMENSIONS = ("model", "project_id", "api_key_id", "user_id")
OPENAI_TEXT_USAGE = {
    "input_tokens": "input_uncached_tokens",
    "cache_read_tokens": "input_cached_tokens",
    "cache_write_tokens": "input_cache_write_tokens",
    "output_tokens": "output_tokens",
    "requests": "num_model_requests",
}
OPENAI_INPUT_USAGE = {"input_tokens": "input_tokens", "requests": "num_model_requests"}
# endpoint -> (usage type -> result field, group_by dimensions it supports)
OPENAI_USAGE_ENDPOINTS: dict[str, tuple[dict[str, str], tuple[str, ...]]] = {
    "usage/completions": (OPENAI_TEXT_USAGE, OPENAI_ALL_DIMENSIONS),
    "usage/decisions": (OPENAI_TEXT_USAGE, OPENAI_ALL_DIMENSIONS),
    "usage/embeddings": (OPENAI_INPUT_USAGE, OPENAI_ALL_DIMENSIONS),
    "usage/moderations": (OPENAI_INPUT_USAGE, OPENAI_ALL_DIMENSIONS),
    "usage/images": ({"images": "images"}, OPENAI_ALL_DIMENSIONS),
    "usage/audio_speeches": ({"tts_characters": "characters"}, OPENAI_ALL_DIMENSIONS),
    "usage/audio_transcriptions": ({"audio_seconds": "seconds"}, OPENAI_ALL_DIMENSIONS),
    "usage/web_search_calls": ({"web_search_requests": "num_requests"}, OPENAI_ALL_DIMENSIONS),
    "usage/file_search_calls": ({"file_search_requests": "num_requests"}, OPENAI_ALL_DIMENSIONS[1:]),
    "usage/code_interpreter_sessions": ({"code_interpreter_sessions": "num_sessions"}, ("project_id",)),
    "usage/vector_stores": ({"vector_store_bytes": "usage_bytes"}, ("project_id",)),
}


@register
class OpenAIConnector(Connector):
    """OpenAI usage and cost from the organization Usage and Costs APIs."""

    name = "openai"
    usage_types = frozenset(
        usage_type for fields, _ in OPENAI_USAGE_ENDPOINTS.values() for usage_type in fields
    )

    @classmethod
    def check_config(cls, settings: Settings) -> None:
        read_secret(settings.provider["openai_admin_key_env"])

    def oldest_usage(self, now: datetime) -> datetime:
        """Midnight UTC of the day 365 days back: OpenAI serves the past year from the same date's start."""
        return datetime.combine(self.oldest_cost(now), datetime.min.time(), timezone.utc)

    def oldest_cost(self, now: datetime) -> date:
        return (now - timedelta(days=OPENAI_HISTORY_DAYS)).date()

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {read_secret(self.settings.provider['openai_admin_key_env'])}"}

    def group_by(self, supported: Sequence[str]) -> list[str]:
        """Supported dimensions minus those whose tag is excluded."""
        excluded = {
            "project_id": "project" in self.settings.exclude_tags,
            "api_key_id": "api_key_id" in self.settings.exclude_tags,
            "user_id": "user_id" in self.settings.exclude_tags,
        }
        return [dimension for dimension in supported if not excluded.get(dimension)]

    def fetch_pages(self, endpoint: str, params: dict) -> list[dict]:
        """All buckets of one chunk, or none when it fails (recorded in errors)."""
        try:
            return self.collect_pages(f"{OPENAI_BASE_URL}/{endpoint}", params)
        except ProviderError as error:
            start = datetime.fromtimestamp(params["start_time"], timezone.utc)
            end = datetime.fromtimestamp(params["end_time"], timezone.utc)
            self.record_chunk_failure(endpoint, iso_utc(start), iso_utc(end), error)
            return []

    def list_all(self, path: str, params: dict[str, Any]) -> Iterator[dict]:
        """Items of a cursor-paged list, following `after=<last_id>` while has_more."""
        params = dict(params)
        seen: set[str] = set()
        while True:
            body = self.http.request_json("GET", f"{OPENAI_BASE_URL}/{path}", headers=self.headers(), params=dict(params))
            items = body.get("data") or []
            yield from items
            if not body.get("has_more"):
                return
            params["after"] = next_page_cursor(body, "last_id", seen, items[-1].get("id") if items else None)

    def key_names(self) -> dict[str, str]:
        return self.cached_key_names(OPENAI_KEY_NAMES_CACHE_KEY, self.load_key_names)

    def load_key_names(self) -> dict[str, str]:
        names: dict[str, str] = {}
        projects = self.list_all("projects", {"limit": OPENAI_KEY_PAGE_LIMIT, "include_archived": "true"})
        for project in list(projects):
            # without "any", keys whose owner lost access to the project can be left out
            key_params = {"limit": OPENAI_KEY_PAGE_LIMIT, "owner_project_access": "any"}
            for key in self.list_all(f"projects/{project['id']}/api_keys", key_params):
                if key.get("id") and key.get("name"):
                    names[key["id"]] = key["name"]
        return names

    def tags(self, row: dict, model: str | None) -> dict[str, str]:
        tags = {
            "account": self.settings.provider["openai_account"],
            "model": model,
            "project": row.get("project_id"),
            "api_key_id": row.get("api_key_id"),
            "user_id": row.get("user_id"),
        }
        return {key: value for key, value in tags.items() if value and key not in self.settings.exclude_tags}

    @staticmethod
    def quantity(row: dict, field: str) -> int | float:
        if field == "input_uncached_tokens" and row.get(field) is None:
            return (
                (row.get("input_tokens") or 0)
                - (row.get("input_cached_tokens") or 0)
                - (row.get("input_cache_write_tokens") or 0)
            )
        return row.get(field) or 0

    def fetch_usage(
        self, start: datetime, end: datetime, bucket_width: str, usage_types: list[str]
    ) -> Iterator[UsageRecord]:
        span = bucket_delta(bucket_width) * OPENAI_USAGE_BUCKET_LIMITS[bucket_width]
        for endpoint, (fields, dimensions) in OPENAI_USAGE_ENDPOINTS.items():
            wanted = [usage_type for usage_type in usage_types if usage_type in fields]
            if not wanted:
                continue
            for chunk_start, chunk_end in period_chunks(start, end, span):
                params: dict[str, Any] = {
                    "start_time": int(chunk_start.timestamp()),
                    "end_time": int(chunk_end.timestamp()),
                    "bucket_width": bucket_width,
                    "limit": OPENAI_USAGE_BUCKET_LIMITS[bucket_width],
                }
                if group_by := self.group_by(dimensions):
                    params["group_by"] = group_by
                for bucket in self.fetch_pages(endpoint, params):
                    for row in bucket.get("results") or []:
                        for usage_type in wanted:
                            yield UsageRecord(
                                datetime.fromtimestamp(bucket["start_time"], timezone.utc),
                                self.name,
                                usage_type,
                                self.quantity(row, fields[usage_type]),
                                self.tags(row, row.get("model")),
                                self.key_name(row),
                            )

    def fetch_cost(self, start_day: date, end_day: date) -> Iterator[CostRecord]:
        start = datetime.combine(start_day, datetime.min.time(), timezone.utc)
        end = datetime.combine(end_day, datetime.min.time(), timezone.utc)
        for chunk_start, chunk_end in period_chunks(start, end, timedelta(days=OPENAI_COST_BUCKET_LIMIT)):
            params: dict[str, Any] = {
                "start_time": int(chunk_start.timestamp()),
                "end_time": int(chunk_end.timestamp()),
                "bucket_width": "1d",
                "limit": OPENAI_COST_BUCKET_LIMIT,
                "group_by": self.group_by(("project_id", "user_id", "api_key_id")) + ["line_item"],
            }
            for bucket in self.fetch_pages("costs", params):
                for row in bucket.get("results") or []:
                    amount = row.get("amount") or {}
                    if amount.get("value") is None:
                        continue
                    line_item = row.get("line_item") or ""
                    model = line_item.split(", ")[0] if ", " in line_item else None
                    yield CostRecord(
                        datetime.fromtimestamp(bucket["start_time"], timezone.utc),
                        self.name,
                        line_item,
                        (amount.get("currency") or "USD").upper(),
                        float(amount["value"]),
                        self.tags(row, model),
                        self.key_name(row),
                    )


# --- bedrock ---

BEDROCK_COST_REGION = "us-east-1"
BEDROCK_PRODUCT_ATTRIBUTE_FROM = date(2026, 9, 1)
BEDROCK_SERVICE = "Amazon Bedrock"
BEDROCK_EDITION_SUFFIX = "(Amazon Bedrock Edition)"
BEDROCK_MAX_QUERIES = 500
# MetricDataResult.StatusCode values that mean the series has no or only part of its data
CLOUDWATCH_FAILED_STATUSES = frozenset({"InternalError", "Forbidden"})
BEDROCK_USAGE_RETENTION = timedelta(days=455)
BEDROCK_DISCOVERY_WINDOW = timedelta(days=14)
BEDROCK_COST_HISTORY_MONTHS = 13
BEDROCK_REGION_NAME = re.compile(r"[a-z]{2,}(-[a-z0-9]+)+-\d+")
# namespace -> (the one dimension its model series carry, metric -> usage type)
BEDROCK_NAMESPACES: dict[str, tuple[str, dict[str, str]]] = {
    "AWS/Bedrock": (
        "ModelId",
        {
            "InputTokenCount": "input_tokens",
            "CacheReadInputTokenCount": "cache_read_tokens",
            "CacheWriteInputTokenCount": "cache_write_tokens",
            "OutputTokenCount": "output_tokens",
            "Invocations": "requests",
            "OutputImageCount": "images",
        },
    ),
    "AWS/BedrockMantle": (
        "Model",
        {
            "TotalInputTokens": "input_tokens",
            "TotalOutputTokens": "output_tokens",
            "Inferences": "requests",
        },
    ),
}


def metric_query_id(namespace: str, dimensions: list[tuple[str, str]], metric: str) -> str:
    """A deterministic GetMetricData query Id for one metric series."""
    series = json.dumps([namespace, dimensions, metric])
    return "m" + hashlib.sha256(series.encode()).hexdigest()[:40]


def aws_failure_reason(error: Exception) -> str:
    """The AWS error code when there is one, else the exception class; never AWS's message, only the plugin's own."""
    if isinstance(error, ProviderError):
        return provider_failure_reason(error)
    response = getattr(error, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    return code or type(error).__name__


@register
class BedrockConnector(Connector):
    """AWS Bedrock usage from CloudWatch and cost from Cost Explorer."""

    name = "bedrock"
    usage_types = frozenset(
        usage_type for _, metrics in BEDROCK_NAMESPACES.values() for usage_type in metrics.values()
    )

    def __init__(self, settings: Settings, http: HttpClient, local):
        super().__init__(settings, http, local)
        self.boto_session = None
        self.cached_account_id: str | None = None
        # failed lookups are not repeated in later slices of the same run
        self.account_lookup_failed = False
        self.unreachable_regions: set[str] = set()

    @classmethod
    def check_config(cls, settings: Settings) -> None:
        cls.require_settings(settings)
        for region in settings.provider["bedrock_regions"]:
            if not BEDROCK_REGION_NAME.fullmatch(region):
                raise ConfigError(f"bedrock_regions: invalid region name {region}")
        try:
            import boto3  # noqa: F401
        except ImportError:
            raise ConfigError("boto3 is not installed; install it with `influxdb3 install package boto3`")
        if not settings.provider["bedrock_use_default_credentials"]:
            cls.credential_arguments(settings.provider)

    @staticmethod
    def credential_arguments(provider: dict[str, Any]) -> dict[str, str]:
        arguments = {
            "aws_access_key_id": read_secret(provider["bedrock_access_key_id_env"]),
            "aws_secret_access_key": read_secret(provider["bedrock_secret_access_key_env"]),
        }
        token_env = provider["bedrock_session_token_env"]
        if os.environ.get(token_env, "").strip():
            arguments["aws_session_token"] = read_secret(token_env)
        return arguments

    def _session(self):
        if self.boto_session is None:
            import boto3

            if self.settings.provider["bedrock_use_default_credentials"]:
                self.boto_session = boto3.Session()
            else:
                self.boto_session = boto3.Session(**self.credential_arguments(self.settings.provider))
        return self.boto_session

    def _client(self, service: str, region: str):
        from botocore.config import Config

        retries = {"mode": "standard", "total_max_attempts": MAX_ATTEMPTS}
        return self._session().client(service, region_name=region, config=Config(retries=retries))

    def _account_id(self) -> str:
        """The account ID from the first configured region whose STS answers."""
        if self.cached_account_id is None:
            error = None
            for region in self.settings.provider["bedrock_regions"]:
                try:
                    self.cached_account_id = self._client("sts", region).get_caller_identity()["Account"]
                    break
                except self.aws_errors() as caught:
                    error = error or caught
            if self.cached_account_id is None:
                raise error
        return self.cached_account_id

    @staticmethod
    def aws_errors() -> tuple[type[Exception], ...]:
        """Exceptions of a failed AWS call; a repeated page token (ProviderError) counts as one."""
        from botocore.exceptions import BotoCoreError, ClientError

        return BotoCoreError, ClientError, ProviderError

    def record_failure(self, what: str, error: Exception) -> None:
        """Note a skipped call by what it was and the AWS error code only."""
        self.errors.append(f"{what} skipped: {aws_failure_reason(error)}")

    def account_id_or_record(self) -> str | None:
        if self.account_lookup_failed:
            return None
        try:
            return self._account_id()
        except self.aws_errors() as error:
            self.account_lookup_failed = True
            self.record_failure("GetCallerIdentity", error)
            return None

    def note_unreachable(self, region: str, error: Exception) -> None:
        """Remember a region whose endpoint can't be reached, so later slices skip it."""
        from botocore.exceptions import ConnectionError as EndpointUnreachable

        if isinstance(error, EndpointUnreachable):
            self.unreachable_regions.add(region)

    def account_label(self, account_id: str | None) -> str | None:
        return self.settings.provider.get("bedrock_account") or account_id

    def oldest_usage(self, now: datetime) -> datetime | None:
        return now - BEDROCK_USAGE_RETENTION

    def oldest_cost(self, now: datetime) -> date | None:
        return first_day_months_back(now, BEDROCK_COST_HISTORY_MONTHS)

    def list_series(self, client, region: str, namespace: str, wanted: set[str]) -> list[tuple[str, str]]:
        """(metric, model) of every series of `namespace` whose only dimension is the model one."""
        dimension_name, metrics = BEDROCK_NAMESPACES[namespace]
        series: list[tuple[str, str]] = []
        token = None
        seen: set[str] = set()
        try:
            while True:
                params = {"Namespace": namespace}
                if token:
                    params["NextToken"] = token
                response = client.list_metrics(**params)
                for metric in response.get("Metrics") or []:
                    dimensions = metric.get("Dimensions") or []
                    if (
                        metric.get("MetricName") in metrics
                        and metrics[metric["MetricName"]] in wanted
                        and len(dimensions) == 1
                        and dimensions[0].get("Name") == dimension_name
                    ):
                        series.append((metric["MetricName"], dimensions[0]["Value"]))
                token = next_page_token(response.get("NextToken"), "NextToken", seen)
                if not token:
                    return series
        except self.aws_errors() as error:
            self.note_unreachable(region, error)
            self.record_failure(f"ListMetrics {namespace} in {region}", error)
            return []

    @staticmethod
    def metric_data_batch(
        client, batch: list[dict], start: datetime, end: datetime
    ) -> tuple[list[dict], dict[str, str]]:
        """All pages of one GetMetricData call, and Id -> status of the series CloudWatch returned incomplete."""
        results: list[dict] = []
        failed: dict[str, str] = {}
        token = None
        seen: set[str] = set()
        while True:
            params = {"MetricDataQueries": batch, "StartTime": start, "EndTime": end}
            if token:
                params["NextToken"] = token
            response = client.get_metric_data(**params)
            token = next_page_token(response.get("NextToken"), "NextToken", seen)
            for result in response.get("MetricDataResults") or []:
                status = result.get("StatusCode")
                # PartialData on the last page has no next page to complete it
                if status in CLOUDWATCH_FAILED_STATUSES or (status == "PartialData" and not token):
                    failed[result["Id"]] = status
                results.append(result)
            if not token:
                return results, failed

    def read_metric_data(
        self, client, region: str, queries: list[dict], start: datetime, end: datetime
    ) -> tuple[list[dict], set[str]]:
        """
        Results of the queries in batches of at most 500, and the Ids of the series without complete data.

        A failed call is recorded and fails its whole batch; series CloudWatch returns as failed are recorded in one error.
        """
        window = f"{iso_utc(start)} to {iso_utc(end)}"
        results: list[dict] = []
        incomplete: set[str] = set()
        failed: dict[str, str] = {}
        for offset in range(0, len(queries), BEDROCK_MAX_QUERIES):
            batch = queries[offset : offset + BEDROCK_MAX_QUERIES]
            try:
                batch_results, batch_failed = self.metric_data_batch(client, batch, start, end)
            except self.aws_errors() as error:
                self.note_unreachable(region, error)
                self.record_failure(f"GetMetricData in {region} {window}", error)
                incomplete.update(query["Id"] for query in batch)
                continue
            results.extend(batch_results)
            failed.update(batch_failed)
        if failed:
            codes = ", ".join(sorted(set(failed.values())))
            self.errors.append(
                f"GetMetricData in {region} {window}: {len(failed)} of {len(queries)} series failed ({codes}), "
                "rows of their model and usage type skipped"
            )
        return results, incomplete | failed.keys()

    def fetch_usage(
        self, start: datetime, end: datetime, bucket_width: str, usage_types: list[str]
    ) -> Iterator[UsageRecord]:
        now = datetime.now(timezone.utc)
        if start < now - BEDROCK_DISCOVERY_WINDOW:
            since = (now - BEDROCK_DISCOVERY_WINDOW).date().isoformat()
            note = (
                f"Bedrock usage before {since} covers only models with traffic since then: "
                "CloudWatch lists metrics with data in the last two weeks."
            )
            if note not in self.notes:
                self.notes.append(note)
        account_id = None
        # the account ID only fills the account tag here; cost resolves it again for its filter
        if not self.settings.provider.get("bedrock_account") and "account" not in self.settings.exclude_tags:
            account_id = self.account_id_or_record()
            if account_id is None:
                return
        label = self.account_label(account_id)
        period = int(bucket_delta(bucket_width).total_seconds())
        wanted = set(usage_types)
        for region in self.settings.provider["bedrock_regions"]:
            if region in self.unreachable_regions:
                continue
            # boto3 calls don't pass through the HTTP client, so the deadline is checked here
            self.http.check_deadline()
            client = self._client("cloudwatch", region)
            queries: list[dict] = []
            meaning: dict[str, tuple[str, str]] = {}
            for namespace, (dimension_name, metrics) in BEDROCK_NAMESPACES.items():
                for metric, model in self.list_series(client, region, namespace, wanted):
                    dimensions = [(dimension_name, model)]
                    query_id = metric_query_id(namespace, dimensions, metric)
                    if query_id in meaning:
                        continue
                    meaning[query_id] = (metrics[metric], model)
                    queries.append(
                        {
                            "Id": query_id,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": namespace,
                                    "MetricName": metric,
                                    "Dimensions": [{"Name": dimension_name, "Value": model}],
                                },
                                "Period": period,
                                "Stat": "Sum",
                            },
                            "ReturnData": True,
                        }
                    )
            results, incomplete = self.read_metric_data(client, region, queries, start, end)
            # one model's usage type can add up series of both namespaces: skip it unless all of them are complete
            skipped = {meaning[query_id] for query_id in incomplete}
            for result in results:
                usage_type, model = meaning[result["Id"]]
                if (usage_type, model) in skipped:
                    continue
                tags = {key: value for key, value in (("account", label), ("model", model), ("region", region)) if value}
                for moment, value in zip(result.get("Timestamps") or [], result.get("Values") or []):
                    yield UsageRecord(moment.astimezone(timezone.utc), self.name, usage_type, value, dict(tags))

    def bedrock_services(self, client, start_day: date, end_day: date) -> list[str]:
        """Bedrock service names Cost Explorer knows over the period, as found."""
        services: list[str] = []
        token = None
        seen: set[str] = set()
        while True:
            params: dict[str, Any] = {
                "TimePeriod": {"Start": start_day.isoformat(), "End": end_day.isoformat()},
                "Dimension": "SERVICE",
            }
            if token:
                params["NextPageToken"] = token
            response = client.get_dimension_values(**params)
            for item in response.get("DimensionValues") or []:
                value = item.get("Value") or ""
                if value == BEDROCK_SERVICE or value.endswith(BEDROCK_EDITION_SUFFIX):
                    services.append(value)
            token = next_page_token(response.get("NextPageToken"), "NextPageToken", seen)
            if not token:
                return services

    def cost_groups(
        self, client, region: str, account_id: str, start_day: date, end_day: date, services: list[str] | None
    ) -> list[dict]:
        """Daily result entries of one region and period part (`services` set before the product attribute existed)."""
        group_by = [{"Type": "DIMENSION", "Key": "USAGE_TYPE"}]
        conditions = [
            {"Dimensions": {"Key": "REGION", "Values": [region]}},
            {"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": [account_id]}},
        ]
        if services is None:
            group_by.insert(0, {"Type": "PRODUCT_ATTRIBUTE", "Key": "model"})
        else:
            conditions.append({"Dimensions": {"Key": "SERVICE", "Values": services}})
        entries: list[dict] = []
        token = None
        seen: set[str] = set()
        while True:
            params: dict[str, Any] = {
                "TimePeriod": {"Start": start_day.isoformat(), "End": end_day.isoformat()},
                "Granularity": "DAILY",
                "Metrics": ["UnblendedCost"],
                "GroupBy": group_by,
                "Filter": {"And": conditions},
            }
            if token:
                params["NextPageToken"] = token
            response = client.get_cost_and_usage(**params)
            entries.extend(response.get("ResultsByTime") or [])
            token = next_page_token(response.get("NextPageToken"), "NextPageToken", seen)
            if not token:
                return entries

    def fetch_cost(self, start_day: date, end_day: date) -> Iterator[CostRecord]:
        account_id = self.account_id_or_record()
        if account_id is None:
            return
        label = self.account_label(account_id)
        client = self._client("ce", BEDROCK_COST_REGION)
        parts: list[tuple[date, date, bool]] = []
        services: list[str] = []
        if start_day < BEDROCK_PRODUCT_ATTRIBUTE_FROM:
            part_end = min(end_day, BEDROCK_PRODUCT_ATTRIBUTE_FROM)
            parts.append((start_day, part_end, False))
            try:
                services = self.bedrock_services(client, start_day, part_end)
            except self.aws_errors() as error:
                self.record_failure(f"GetDimensionValues {start_day} to {part_end}", error)
        if end_day > BEDROCK_PRODUCT_ATTRIBUTE_FROM:
            parts.append((max(start_day, BEDROCK_PRODUCT_ATTRIBUTE_FROM), end_day, True))
        for region in self.settings.provider["bedrock_regions"]:
            for part_start, part_end, has_model in parts:
                if not has_model and not services:
                    continue
                self.http.check_deadline()
                try:
                    entries = self.cost_groups(
                        client, region, account_id, part_start, part_end, None if has_model else services
                    )
                except self.aws_errors() as error:
                    self.record_failure(f"GetCostAndUsage in {region} {part_start} to {part_end}", error)
                    continue
                for entry in entries:
                    day = datetime.strptime(entry["TimePeriod"]["Start"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    for group in entry.get("Groups") or []:
                        keys = group["Keys"]
                        model = keys[0].removeprefix("model$") if has_model else None
                        amount = group["Metrics"]["UnblendedCost"]
                        yield CostRecord(
                            day,
                            self.name,
                            keys[-1],
                            amount.get("Unit") or "USD",
                            float(amount["Amount"]),
                            {key: value for key, value in (("account", label), ("model", model), ("region", region)) if value},
                        )


# --- azure ---

AZURE_GUID = re.compile(r"[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}")
AZURE_LOGIN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
AZURE_ARM_URL = "https://management.azure.com"
AZURE_ARM_SCOPE = "https://management.azure.com/.default"
AZURE_METRICS_SCOPE = "https://metrics.monitor.azure.com/.default"
AZURE_ACCOUNTS_API_VERSION = "2024-10-01"
AZURE_METRICS_API_VERSION = "2023-10-01"
AZURE_COST_API_VERSION = "2025-03-01"
AZURE_LLM_KINDS = frozenset({"OpenAI", "AIServices"})
AZURE_TOKEN_MARGIN_SECONDS = 60
AZURE_BATCH_SIZE = 50
AZURE_METRICS_TOP = 1000
AZURE_USAGE_RETENTION = timedelta(days=93)
AZURE_COST_HISTORY_MONTHS = 13
AZURE_USAGE_SPANS = {"1h": timedelta(days=7), "1d": timedelta(days=31)}
AZURE_INTERVALS = {"1h": "PT1H", "1d": "P1D"}
# lower-cased metric name -> (metric name to request, usage type)
AZURE_METRICS = {
    metric.lower(): (metric, usage_type)
    for metric, usage_type in (
        ("InputTokens", "input_tokens"),
        ("OutputTokens", "output_tokens"),
        ("ModelRequests", "requests"),
        ("GeneratedImages", "images"),
        ("cacheReadInputTokens", "cache_read_tokens"),
        ("ephemeral5mInputTokens", "cache_write_tokens"),
        ("ephemeral1hInputTokens", "cache_write_tokens"),
    )
}


def parse_azure_time(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(timezone.utc)


def azure_usage_type(metric: dict) -> str | None:
    """The usage type of a getBatch metric item, or None for a metric the plugin doesn't read."""
    mapped = AZURE_METRICS.get(((metric.get("name") or {}).get("value") or "").lower())
    return mapped[1] if mapped else None


@register
class AzureConnector(Connector):
    """Azure OpenAI and Microsoft Foundry usage from Azure Monitor and cost from Cost Management."""

    name = "azure"
    usage_types = frozenset(usage_type for _, usage_type in AZURE_METRICS.values())

    def __init__(self, settings: Settings, http: HttpClient, local):
        super().__init__(settings, http, local)
        self.tokens: dict[str, tuple[str, float]] = {}
        self.found_accounts: list[dict] | None = None
        self.other_account_ids: set[str] = set()
        # subscription -> currency -> [rows, total] of cost rows whose resource wasn't found
        self.skipped_cost: dict[str, dict[str, list]] = {}

    @classmethod
    def check_config(cls, settings: Settings) -> None:
        cls.require_settings(settings)
        provider = settings.provider
        ids = [("azure_tenant_id", provider["azure_tenant_id"]), ("azure_client_id", provider["azure_client_id"])]
        ids += [("azure_subscription_ids", subscription) for subscription in provider["azure_subscription_ids"]]
        for name, value in ids:
            if not AZURE_GUID.fullmatch(value):
                raise ConfigError(f"{name}: invalid GUID {value}")
        read_secret(provider["azure_client_secret_env"])

    def token(self, scope: str) -> str:
        """A bearer token for `scope`, reused until shortly before it expires."""
        held = self.tokens.get(scope)
        if held and time.time() < held[1]:
            return held[0]
        provider = self.settings.provider
        body = self.http.request_json(
            "POST",
            AZURE_LOGIN_URL.format(tenant=provider["azure_tenant_id"]),
            data={
                "grant_type": "client_credentials",
                "client_id": provider["azure_client_id"],
                "client_secret": read_secret(provider["azure_client_secret_env"]),
                "scope": scope,
            },
        )
        token = body["access_token"]
        lifetime = float(body.get("expires_in") or 0)
        self.tokens[scope] = (token, time.time() + lifetime - AZURE_TOKEN_MARGIN_SECONDS)
        return token

    def auth_headers(self, scope: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token(scope)}"}

    def oldest_usage(self, now: datetime) -> datetime | None:
        return now - AZURE_USAGE_RETENTION

    def oldest_cost(self, now: datetime) -> date | None:
        return first_day_months_back(now, AZURE_COST_HISTORY_MONTHS)

    def account_label(self, subscription: str) -> str:
        return self.settings.provider.get("azure_account") or subscription

    def discover(self) -> list[dict]:
        """LLM accounts (kinds OpenAI and AIServices) of every configured subscription; other kinds are remembered by ID."""
        accounts: list[dict] = []
        for subscription in self.settings.provider["azure_subscription_ids"]:
            url = f"{AZURE_ARM_URL}/subscriptions/{subscription}/providers/Microsoft.CognitiveServices/accounts?api-version={AZURE_ACCOUNTS_API_VERSION}"
            seen = {url}
            try:
                while url:
                    body = self.http.request_json("GET", url, headers=self.auth_headers(AZURE_ARM_SCOPE))
                    for item in body.get("value") or []:
                        if item.get("kind") in AZURE_LLM_KINDS:
                            accounts.append(
                                {
                                    "id": item["id"],
                                    "name": item["name"],
                                    "location": item.get("location") or "",
                                    "subscription": subscription,
                                }
                            )
                        else:
                            self.other_account_ids.add(str(item.get("id") or "").lower())
                    url = next_page_token(body.get("nextLink"), "nextLink", seen)
            except ProviderError as error:
                self.errors.append(f"account discovery in subscription {subscription} skipped: {provider_failure_reason(error)}")
        return accounts

    def accounts(self) -> list[dict]:
        if self.found_accounts is None:
            self.found_accounts = self.discover()
        return self.found_accounts

    def batch_usage(
        self, subscription: str, location: str, ids: list[str], chunk_start: datetime, chunk_end: datetime,
        bucket_width: str, metric_names: list[str],
    ) -> list[dict]:
        """Result entries of one getBatch call, or none when it fails (recorded in errors)."""
        url = f"https://{location}.metrics.monitor.azure.com/subscriptions/{subscription}/metrics:getBatch"
        params = {
            "api-version": AZURE_METRICS_API_VERSION,
            "metricnamespace": "Microsoft.CognitiveServices/accounts",
            "starttime": iso_utc(chunk_start),
            "endtime": iso_utc(chunk_end),
            "metricnames": ",".join(metric_names),
            "aggregation": "total",
            "interval": AZURE_INTERVALS[bucket_width],
            "filter": "ModelDeploymentName eq '*' and ModelName eq '*'",
            "top": AZURE_METRICS_TOP,
        }
        try:
            body = self.http.request_json(
                "POST", url, headers=self.auth_headers(AZURE_METRICS_SCOPE), params=params, json={"resourceids": ids}
            )
        except ProviderError as error:
            self.record_chunk_failure(f"getBatch {subscription} {location}", params["starttime"], params["endtime"], error)
            return []
        return body.get("values") or []

    def fetch_usage(
        self, start: datetime, end: datetime, bucket_width: str, usage_types: list[str]
    ) -> Iterator[UsageRecord]:
        wanted = set(usage_types)
        metric_names = [metric for metric, usage_type in AZURE_METRICS.values() if usage_type in wanted]
        if not metric_names:
            return
        groups: dict[tuple[str, str], list[dict]] = {}
        for found in self.accounts():
            groups.setdefault((found["subscription"], found["location"]), []).append(found)
        for chunk_start, chunk_end in period_chunks(start, end, AZURE_USAGE_SPANS[bucket_width]):
            for (subscription, location), members in groups.items():
                names = {member["id"].lower(): member["name"] for member in members}
                what = f"getBatch {subscription} {location} chunk {iso_utc(chunk_start)} to {iso_utc(chunk_end)}"
                for offset in range(0, len(members), AZURE_BATCH_SIZE):
                    ids = [member["id"] for member in members[offset : offset + AZURE_BATCH_SIZE]]
                    entries = self.batch_usage(subscription, location, ids, chunk_start, chunk_end, bucket_width, metric_names)
                    for entry in entries:
                        resource = names.get((entry.get("resourceid") or "").lower())
                        if resource is not None:
                            # a usage type can add up several metrics (cache writes): skip it when any of them failed
                            failed = self.failed_usage_types(entry, resource, what)
                            yield from self.entry_records(entry, resource, subscription, location, wanted - failed)

    def failed_usage_types(self, entry: dict, resource: str, what: str) -> set[str]:
        """Usage types of the entry's metrics that came back with an error code instead of data, recorded under `what`."""
        codes: dict[str, str] = {}
        for metric in entry.get("value") or []:
            usage_type, code = azure_usage_type(metric), metric.get("errorCode") or "Success"
            if usage_type and code != "Success":
                codes[usage_type] = code
        if codes:
            self.errors.append(
                f"{what}: {', '.join(sorted(codes))} of {resource} skipped: {', '.join(sorted(set(codes.values())))}"
            )
        return set(codes)

    def entry_records(
        self, entry: dict, resource: str, subscription: str, location: str, wanted: set[str]
    ) -> Iterator[UsageRecord]:
        for metric in entry.get("value") or []:
            usage_type = azure_usage_type(metric)
            if usage_type not in wanted:
                continue
            for series in metric.get("timeseries") or []:
                dimensions = {
                    (item.get("name") or {}).get("value", "").lower(): item.get("value")
                    for item in series.get("metadatavalues") or []
                }
                deployment = dimensions.get("modeldeploymentname")
                tags = {
                    "account": self.account_label(subscription),
                    "model": dimensions.get("modelname"),
                    "project": f"{resource}/{deployment}" if deployment else resource,
                    "region": location,
                }
                for point in series.get("data") or []:
                    if point.get("total") is None:
                        continue
                    yield UsageRecord(
                        parse_azure_time(point["timeStamp"]), self.name, usage_type, point["total"], dict(tags)
                    )

    @staticmethod
    def month_chunks(start_day: date, end_day: date) -> Iterator[tuple[date, date]]:
        """Calendar-month pieces (first day, last included day) of start_day up to, not including, end_day."""
        last_day = end_day - timedelta(days=1)
        first = start_day
        while first <= last_day:
            month_end = (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            yield first, min(month_end, last_day)
            first = month_end + timedelta(days=1)

    def query_cost(self, subscription: str, first_day: date, last_day: date) -> Iterator[dict]:
        """Rows of one cost query, following nextLink; a failure is recorded and ends the chunk."""
        body = {
            "type": "ActualCost",
            "timeframe": "Custom",
            "timePeriod": {"from": f"{first_day}T00:00:00Z", "to": f"{last_day}T23:59:59Z"},
            "dataset": {
                "granularity": "Daily",
                "aggregation": {"cost": {"name": "Cost", "function": "Sum"}},
                "filter": {"dimensions": {"name": "ResourceType", "operator": "In", "values": ["microsoft.cognitiveservices/accounts"]}},
                "grouping": [
                    {"type": "Dimension", "name": "ResourceId"},
                    {"type": "Dimension", "name": "Meter"},
                ],
            },
        }
        url = f"{AZURE_ARM_URL}/subscriptions/{subscription}/providers/Microsoft.CostManagement/query"
        params: dict | None = {"api-version": AZURE_COST_API_VERSION}
        seen: set[str] = set()
        try:
            while url:
                response = self.http.request_json(
                    "POST", url, headers=self.auth_headers(AZURE_ARM_SCOPE), params=params, json=body
                )
                properties = response.get("properties") or {}
                names = [(column.get("name") or "").lower() for column in properties.get("columns") or []]
                for row in properties.get("rows") or []:
                    yield dict(zip(names, row))
                url, params = next_page_token(properties.get("nextLink"), "nextLink", seen), None
        except ProviderError as error:
            self.record_chunk_failure(f"cost query {subscription}", str(first_day), str(last_day), error)

    def fetch_cost(self, start_day: date, end_day: date) -> Iterator[CostRecord]:
        subscriptions: dict[str, dict[str, dict]] = {}
        for found in self.accounts():
            subscriptions.setdefault(found["subscription"], {})[found["id"].lower()] = found
        for subscription, by_id in subscriptions.items():
            label = self.account_label(subscription)
            for first_day, last_day in self.month_chunks(start_day, end_day):
                for row in self.query_cost(subscription, first_day, last_day):
                    if not row.get("cost"):
                        continue
                    resource_id = str(row.get("resourceid") or "").lower()
                    resource = by_id.get(resource_id)
                    if resource is None:
                        if resource_id not in self.other_account_ids:
                            self.count_skipped_cost(subscription, row)
                        continue
                    day = datetime.strptime(str(row["usagedate"]), "%Y%m%d").replace(tzinfo=timezone.utc)
                    tags = {"account": label, "project": resource["name"], "region": resource["location"]}
                    yield CostRecord(
                        day, self.name, row.get("meter") or "", row.get("currency") or "USD", float(row["cost"]), tags
                    )

    def count_skipped_cost(self, subscription: str, row: dict) -> None:
        """Add a cost row of a resource that wasn't discovered (deleted or moved) to the subscription's tally."""
        currency = str(row.get("currency") or "USD").upper()
        tally = self.skipped_cost.setdefault(subscription, {}).setdefault(currency, [0, Decimal(0)])
        tally[0] += 1
        tally[1] += Decimal(str(row["cost"]))

    def closing_notes(self) -> list[str]:
        notes = []
        for subscription, by_currency in self.skipped_cost.items():
            count = sum(rows for rows, _ in by_currency.values())
            amounts = ", ".join(f"{total:.2f} {currency}" for currency, (_, total) in by_currency.items())
            noun = "row" if count == 1 else "rows"
            notes.append(
                f"{count} cost {noun} of {amounts} skipped: their resources weren't found in subscription {subscription}"
            )
        return notes


# --- google ---

GOOGLE_MONITORING_SCOPE = "https://www.googleapis.com/auth/monitoring.read"
GOOGLE_BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"
GOOGLE_USAGE_RETENTION = timedelta(weeks=6)
GOOGLE_KEY_FIELDS = ("client_email", "private_key", "token_uri")
GOOGLE_KEY_PATH_MAX_LENGTH = 1024
GOOGLE_PROJECT_PATTERN = r"(?:[a-z0-9.-]+:)?[a-z][a-z0-9-]{4,28}[a-z0-9]"
GOOGLE_PROJECT_ID = re.compile(GOOGLE_PROJECT_PATTERN)
# the table name goes into SQL as an identifier, so only these characters pass
GOOGLE_BILLING_TABLE = re.compile(
    rf"(?P<project>{GOOGLE_PROJECT_PATTERN})\.(?P<dataset>[A-Za-z0-9_]{{1,1024}})\.(?P<table>[A-Za-z0-9_-]{{1,1024}})"
)
GOOGLE_BILLING_SERVICES = {"C7E2-9256-1C43": "vertex", "AEFD-7695-64FA": "gemini_api"}
BIGQUERY_URL = "https://bigquery.googleapis.com/bigquery/v2/projects/{project}"
BIGQUERY_WAIT_MS = 10_000
BIGQUERY_MAX_POLLS = 30
# `{table}` is the backtick-quoted, format-checked table name; every other value is a query parameter
BILLING_EXPORT_SQL = """
SELECT
  FORMAT_DATE('%Y-%m-%d', DATE(usage_start_time, 'UTC')) AS day,
  project.id AS project_id,
  location.location AS location_name,
  service.id AS service_id,
  sku.description AS sku_description,
  currency,
  SUM(cost) AS cost
FROM `{table}`
WHERE DATE(_PARTITIONTIME) >= DATE_SUB(@first_day, INTERVAL 1 DAY)
  AND usage_start_time >= @start AND usage_start_time < @end
  AND project.id IN UNNEST(@projects)
  AND service.id IN UNNEST(@services)
  AND cost_type IN ('regular', 'adjustment')
GROUP BY day, project_id, location_name, service_id, sku_description, currency
"""
GOOGLE_TIME_SERIES_URL = "https://monitoring.googleapis.com/v3/projects/{project}/timeSeries"
GOOGLE_ALIGNMENT = {"1h": "3600s", "1d": "86400s"}
VERTEX_TOKENS = "aiplatform.googleapis.com/publisher/online_serving/token_count"
VERTEX_INVOCATIONS = "aiplatform.googleapis.com/publisher/online_serving/model_invocation_count"
VERTEX_TOKEN_TYPES = {"input": "input_tokens", "output": "output_tokens"}
VERTEX_SUCCESS_FILTER = 'metric.label.response_code = starts_with("2")'
GEMINI_PREFIX = "generativelanguage.googleapis.com/"
GEMINI_OUTPUT_TOKENS = GEMINI_PREFIX + "generate_content_usage_output_token_count"
GEMINI_INPUT_TOKEN_QUOTAS = tuple(
    f"{GEMINI_PREFIX}quota/{name}/usage"
    for name in (
        "generate_content_free_tier_input_token_count",
        "generate_content_paid_tier_input_token_count",
        "generate_content_paid_tier_2_input_token_count",
        "generate_content_paid_tier_3_input_token_count",
    )
)
GEMINI_REQUEST_QUOTAS = tuple(
    f"{GEMINI_PREFIX}quota/{name}/usage"
    for name in (
        "generate_content_free_tier_requests",
        "generate_requests_per_model",
        "generate_content_paid_tier_2_requests",
        "generate_content_paid_tier_3_requests",
    )
)
GEMINI_QUOTA_GROUP_BY = ["metric.label.model", "metric.label.limit_name"]


def looks_like_path(value: str) -> bool:
    """A single line of at most 1024 characters without `{` or control and format characters."""
    return (
        len(value) <= GOOGLE_KEY_PATH_MAX_LENGTH
        and "{" not in value
        and not any(unicodedata.category(char) in ("Cc", "Cf") for char in value)
    )


def nameable_key_path(value: str) -> bool:
    """
    Whether an error may name `value` as a key file path: it ends with `.json`, or it names
    a directory other than the root that exists. Any other value could be a secret, some of which contain `/`.
    """
    if value.lower().endswith(".json"):
        return True
    try:
        return bool(Path(value).parent.name) and resolve_path(value).parent.is_dir()
    except OSError:  # e.g. a name too long, whose message would quote the value
        return False


def load_google_key(env_name: str) -> dict:
    """
    The service account key held by an environment variable: inline JSON or a key file path.

    Inline JSON starts with `{`; any other value is read as a path only when it looks like one,
    and a relative path is resolved against the plugin directory. Errors name the variable,
    and the path only when `nameable_key_path` allows it; never the key or any other value.
    """
    value = os.environ.get(env_name, "").strip()
    if not value:
        raise ConfigError(f"environment variable {env_name} is not set")
    not_a_key = ConfigError(f"environment variable {env_name} does not hold a service account key")
    if value.startswith("{"):
        text = value
    elif not looks_like_path(value):
        raise not_a_key
    else:
        path = resolve_path(value)
        try:
            text = path.read_text()
        except (OSError, UnicodeDecodeError):
            if not nameable_key_path(value):
                raise not_a_key from None
            raise ConfigError(f"environment variable {env_name} names key file {path}, which can't be read") from None
    try:
        key = json.loads(text)
    except ValueError:
        raise not_a_key from None
    if not isinstance(key, dict) or key.get("type") != "service_account" or not all(key.get(f) for f in GOOGLE_KEY_FIELDS):
        raise not_a_key
    return key


def google_credentials(env_name: str, scopes: Sequence[str] = (GOOGLE_MONITORING_SCOPE,)):
    """Service account credentials with the given scopes, built from the key an environment variable holds."""
    key = load_google_key(env_name)
    from google.oauth2 import service_account

    try:
        return service_account.Credentials.from_service_account_info(key, scopes=list(scopes))
    except Exception:
        raise ConfigError(f"environment variable {env_name} does not hold a service account key") from None


def point_value(point: dict) -> float:
    """The number of a time series point: `int64Value` arrives as a string, `doubleValue` as a float."""
    value = point.get("value") or {}
    return float(value["int64Value"] if "int64Value" in value else value.get("doubleValue") or 0)


def largest_limit_series(series: list[dict]) -> dict[str, dict]:
    """
    Per model, the one quota series with the largest total over the period.

    Every limit of a quota counts the same requests, so using one avoids double counting;
    ties go to the lowest `limit_name`. Fragments of one series split over pages are merged first.
    """
    def label(item: dict, name: str) -> str:
        return ((item.get("metric") or {}).get("labels") or {}).get(name) or ""

    def rank(item: dict) -> tuple[float, str]:
        return -sum(point_value(point) for point in item["points"]), label(item, "limit_name")

    merged: dict[tuple[str, str], dict] = {}
    for item in series:
        key = (label(item, "model"), label(item, "limit_name"))
        merged.setdefault(key, {**item, "points": []})["points"].extend(item.get("points") or [])
    by_model: dict[str, list[dict]] = {}
    for (model, _), item in merged.items():
        by_model.setdefault(model, []).append(item)
    return {model: min(items, key=rank) for model, items in by_model.items()}


def incomplete_result_reason(body: dict) -> str | None:
    """Why a timeSeries.list page may lack data (execution errors, unreachable regions), or None for a complete page."""
    reasons = []
    if body.get("executionErrors"):
        reasons.append("execution errors")
    if body.get("unreachable"):
        reasons.append("unreachable regions " + ", ".join(map(str, body["unreachable"])))
    return "incomplete result: " + "; ".join(reasons) if reasons else None


@register
class GoogleConnector(Connector):
    """Vertex AI and Gemini API usage from Cloud Monitoring."""

    name = "google"
    usage_types = frozenset({"input_tokens", "output_tokens", "requests"})

    def __init__(self, settings: Settings, http: HttpClient, local):
        super().__init__(settings, http, local)
        self.credentials = None
        self.dataset_location: str | None = None

    @classmethod
    def check_config(cls, settings: Settings) -> None:
        cls.require_settings(settings)
        provider = settings.provider
        table = provider["google_billing_table"]
        if "billing_export" in provider["google_sources"] and not table:
            raise ConfigError("google_billing_table required when billing_export is listed in google_sources")
        for project in provider["google_project_ids"]:
            if not GOOGLE_PROJECT_ID.fullmatch(project):
                raise ConfigError(f"google_project_ids: invalid project ID {project}")
        if table and not GOOGLE_BILLING_TABLE.fullmatch(table):
            raise ConfigError(f"google_billing_table: expected <project>.<dataset>.<table>, got {table}")
        try:
            import google.auth  # noqa: F401
            import google.oauth2.service_account  # noqa: F401
        except ImportError:
            raise ConfigError("google-auth is not installed; install it with `influxdb3 install package google-auth`")
        google_credentials(provider["google_credentials_env"])

    def collects_cost(self) -> bool:
        return "billing_export" in self.settings.provider["google_sources"]

    def oldest_usage(self, now: datetime) -> datetime | None:
        return now - GOOGLE_USAGE_RETENTION

    def account_label(self) -> str:
        return self.settings.provider["google_account"]

    def auth_headers(self) -> dict[str, str]:
        """A bearer header; the token is fetched through the plugin's HTTP session and reused until it expires."""
        from google.auth.transport.requests import Request

        if self.credentials is None:
            reads_export = self.collects_cost() and "cost" in self.settings.collect
            scopes = [GOOGLE_MONITORING_SCOPE] + ([GOOGLE_BIGQUERY_SCOPE] if reads_export else [])
            self.credentials = google_credentials(self.settings.provider["google_credentials_env"], scopes)
        if not self.credentials.valid:
            try:
                self.credentials.refresh(Request(session=self.http.session))
            except Exception as error:
                raise ProviderError(None, f"token request failed: {type(error).__name__}") from None
        return {"Authorization": f"Bearer {self.credentials.token}"}

    def time_series(
        self, project: str, metric_type: str, group_by: list[str], start: datetime, end: datetime,
        bucket_width: str, extra_filter: str = "",
    ) -> list[dict]:
        """
        All pages of one aligned, summed timeSeries.list query of a project.

        Raises:
            ProviderError: a call failed, or a page reports execution errors or unreachable regions.
        """
        metric_filter = f'metric.type = "{metric_type}"' + (f" AND {extra_filter}" if extra_filter else "")
        params = [
            ("filter", metric_filter),
            ("interval.startTime", iso_utc(start)),
            ("interval.endTime", iso_utc(end)),
            ("aggregation.alignmentPeriod", GOOGLE_ALIGNMENT[bucket_width]),
            ("aggregation.perSeriesAligner", "ALIGN_SUM"),
            ("aggregation.crossSeriesReducer", "REDUCE_SUM"),
            *[("aggregation.groupByFields", field) for field in group_by],
        ]
        series: list[dict] = []
        url = GOOGLE_TIME_SERIES_URL.format(project=project)
        page_token = None
        seen: set[str] = set()
        while True:
            page_params = params + ([("pageToken", page_token)] if page_token else [])
            body = self.http.request_json("GET", url, headers=self.auth_headers(), params=page_params)
            incomplete = incomplete_result_reason(body)
            if incomplete:
                raise ProviderError(None, incomplete)
            series.extend(body.get("timeSeries") or [])
            page_token = next_page_token(body.get("nextPageToken"), "nextPageToken", seen)
            if not page_token:
                return series

    def fetch_usage(
        self, start: datetime, end: datetime, bucket_width: str, usage_types: list[str]
    ) -> Iterator[UsageRecord]:
        wanted = set(usage_types)
        sources = [
            ("vertex_metrics", "Vertex AI", self.vertex_records),
            ("gemini_api_metrics", "Gemini API", self.gemini_records),
        ]
        for project in self.settings.provider["google_project_ids"]:
            for source, label, records in sources:
                if source not in self.settings.provider["google_sources"]:
                    continue
                try:
                    # all or nothing: rows of one model can add up several queries (Gemini quota tiers)
                    rows = list(records(project, start, end, bucket_width, wanted))
                except ProviderError as error:
                    self.errors.append(f"{label} metrics of project {project} skipped: {provider_failure_reason(error)}")
                    continue
                yield from rows

    def vertex_records(
        self, project: str, start: datetime, end: datetime, bucket_width: str, wanted: set[str]
    ) -> Iterator[UsageRecord]:
        location, model = "resource.label.location", "resource.label.model_user_id"
        if wanted & set(VERTEX_TOKEN_TYPES.values()):
            for series in self.time_series(
                project, VERTEX_TOKENS, [location, model, "metric.label.type"], start, end, bucket_width
            ):
                usage_type = VERTEX_TOKEN_TYPES.get((series.get("metric") or {}).get("labels", {}).get("type"))
                if usage_type in wanted:
                    yield from self.series_records(series, usage_type, project, "vertex")
        if "requests" in wanted:
            for series in self.time_series(
                project, VERTEX_INVOCATIONS, [location, model], start, end, bucket_width, VERTEX_SUCCESS_FILTER
            ):
                yield from self.series_records(series, "requests", project, "vertex")

    def gemini_records(
        self, project: str, start: datetime, end: datetime, bucket_width: str, wanted: set[str]
    ) -> Iterator[UsageRecord]:
        """Output tokens come from one metric; input tokens and requests from per-tier quota metrics.

        Each tier metric is reduced to one limit series per model; rows of different tiers share
        tags and are summed later with the rest of the batch.
        """
        if "output_tokens" in wanted:
            for series in self.time_series(project, GEMINI_OUTPUT_TOKENS, ["metric.label.model"], start, end, bucket_width):
                yield from self.series_records(series, "output_tokens", project, "gemini_api")
        for usage_type, metrics in (("input_tokens", GEMINI_INPUT_TOKEN_QUOTAS), ("requests", GEMINI_REQUEST_QUOTAS)):
            if usage_type not in wanted:
                continue
            for metric_type in metrics:
                series = self.time_series(project, metric_type, GEMINI_QUOTA_GROUP_BY, start, end, bucket_width)
                for item in largest_limit_series(series).values():
                    yield from self.series_records(item, usage_type, project, "gemini_api")

    def series_records(self, series: dict, usage_type: str, project: str, service: str) -> Iterator[UsageRecord]:
        labels = (series.get("resource") or {}).get("labels") or {}
        model = labels.get("model_user_id") or ((series.get("metric") or {}).get("labels") or {}).get("model")
        tags = {"account": self.account_label(), "project": project, "model": model, "service": service}
        if labels.get("location"):
            tags["region"] = labels["location"]
        for point in series.get("points") or []:
            yield UsageRecord(
                parse_azure_time(point["interval"]["startTime"]), self.name, usage_type, point_value(point), dict(tags)
            )

    def bigquery_get(self, url: str, params: dict) -> dict:
        return self.http.request_json("GET", url, headers=self.auth_headers(), params=params)

    def billing_location(self, table: re.Match) -> str:
        """The billing dataset's location, read once per run."""
        if self.dataset_location is None:
            url = f"{BIGQUERY_URL.format(project=table['project'])}/datasets/{table['dataset']}"
            self.dataset_location = self.bigquery_get(url, {})["location"]
        return self.dataset_location

    def billing_query(self, table: re.Match, first_day: date, end_day: date) -> Iterator[dict]:
        """Rows of the export query, polled until the job completes and followed through every page."""
        base_url = BIGQUERY_URL.format(project=table["project"])
        location = self.billing_location(table)

        def scalar(name: str, kind: str, value: str) -> dict:
            return {"name": name, "parameterType": {"type": kind}, "parameterValue": {"value": value}}

        def strings(name: str, values: Iterable[str]) -> dict:
            return {
                "name": name,
                "parameterType": {"type": "ARRAY", "arrayType": {"type": "STRING"}},
                "parameterValue": {"arrayValues": [{"value": value} for value in values]},
            }

        body = {
            "query": BILLING_EXPORT_SQL.format(table=table.group(0)),
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "location": location,
            "timeoutMs": BIGQUERY_WAIT_MS,
            "queryParameters": [
                scalar("first_day", "DATE", first_day.isoformat()),
                scalar("start", "TIMESTAMP", f"{first_day} 00:00:00+00"),
                scalar("end", "TIMESTAMP", f"{end_day} 00:00:00+00"),
                strings("projects", self.settings.provider["google_project_ids"]),
                strings("services", GOOGLE_BILLING_SERVICES),
            ],
        }
        page = self.http.request_json("POST", f"{base_url}/queries", headers=self.auth_headers(), json=body)
        job_id = page["jobReference"]["jobId"]
        results_url = f"{base_url}/queries/{job_id}"
        params = {"location": location, "timeoutMs": BIGQUERY_WAIT_MS}
        polls = 0
        seen: set[str] = set()
        while True:
            if page.get("jobComplete"):
                names = [field["name"] for field in (page.get("schema") or {}).get("fields") or []]
                for row in page.get("rows") or []:
                    yield dict(zip(names, (cell["v"] for cell in row["f"])))
                token = next_page_token(page.get("pageToken"), "pageToken", seen)
                if not token:
                    return
                params["pageToken"] = token
            else:
                polls += 1
                if polls > BIGQUERY_MAX_POLLS:
                    raise ProviderError(None, f"query did not complete; {self.cancel_job(base_url, job_id, location)}")
            page = self.bigquery_get(results_url, dict(params))

    def cancel_job(self, base_url: str, job_id: str, location: str) -> str:
        """Ask BigQuery to cancel a query the plugin gave up on; a failed cancel is reported, not raised."""
        try:
            self.http.request_json(
                "POST", f"{base_url}/jobs/{job_id}/cancel", headers=self.auth_headers(), params={"location": location}
            )
        except ProviderError as error:
            return f"cancelling the job failed: {provider_failure_reason(error)}"
        return "job cancelled"

    def fetch_cost(self, start_day: date, end_day: date) -> Iterator[CostRecord]:
        table = GOOGLE_BILLING_TABLE.fullmatch(self.settings.provider["google_billing_table"])
        label = self.account_label()
        try:
            for row in self.billing_query(table, start_day, end_day):
                tags = {
                    "account": label,
                    "project": row["project_id"],
                    "service": GOOGLE_BILLING_SERVICES[row["service_id"]],
                }
                if row.get("location_name"):
                    tags["region"] = row["location_name"]
                day = datetime.strptime(row["day"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                yield CostRecord(day, self.name, row["sku_description"], row["currency"], float(row["cost"]), tags)
        except ProviderError as error:
            self.record_chunk_failure("billing export", str(start_day), str(end_day - timedelta(days=1)), error)


# --- collector ---

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
COST_FETCHED_KEY = "cost_fetched_at:"
# a period is fetched, prepared and written in slices of whole buckets; default lookbacks fit in one
USAGE_SLICE_SPANS = {"1h": timedelta(days=7), "1d": timedelta(days=92)}
COST_SLICE_SPAN = timedelta(days=92)


def floor_to_bucket(instant: datetime, bucket_width: str) -> datetime:
    step = bucket_delta(bucket_width)
    return EPOCH + (instant - EPOCH) // step * step


def ceil_to_bucket(instant: datetime, bucket_width: str) -> datetime:
    floored = floor_to_bucket(instant, bucket_width)
    return floored if floored == instant else floored + bucket_delta(bucket_width)


def usage_window(now: datetime, bucket_width: str, lookback: timedelta) -> tuple[datetime, datetime]:
    """Complete buckets from ``now - lookback`` up to the current bucket."""
    return floor_to_bucket(now - lookback, bucket_width), floor_to_bucket(now, bucket_width)


def cost_window(now: datetime, lookback: timedelta) -> tuple[date, date]:
    """Days from ``today - lookback`` (rounded up to whole days) through today, end exclusive."""
    today = now.date()
    return today - timedelta(days=math.ceil(lookback / timedelta(days=1))), today + timedelta(days=1)


def backfill_usage_window(settings: Settings, now: datetime) -> tuple[datetime, datetime]:
    """Whole buckets around ``start``/``end``, ending no later than the start of the current bucket."""
    # an end after now counts as now, so a far-future end can't overflow the rounding
    end = min(ceil_to_bucket(min(settings.end, now), settings.bucket_width), floor_to_bucket(now, settings.bucket_width))
    return floor_to_bucket(settings.start, settings.bucket_width), end


def backfill_cost_window(settings: Settings, now: datetime) -> tuple[date, date]:
    """Days of ``start`` through ``end`` (an end after now counts as now); the last day counts unless at midnight."""
    end = min(settings.end, now)
    last_day = end.date()
    if end != datetime.combine(last_day, datetime.min.time(), timezone.utc):
        last_day += timedelta(days=1)
    return settings.start.date(), last_day


def cost_is_due(local, settings: Settings, provider: str, now: datetime) -> bool:
    fetched_at = local.cache.get(COST_FETCHED_KEY + provider)
    try:
        return now - datetime.fromisoformat(fetched_at) >= settings.cost_interval
    except (TypeError, ValueError):
        return True


def drain(records: Iterator, fail: Callable[[str], None], what: str) -> list:
    """
    Collect what the iterator yields; an exception keeps the rows read before it.

    Raises:
        RunTimeExceeded: passed on, so the slice in progress isn't written.
    """
    collected = []
    try:
        for record in records:
            collected.append(record)
    except RunTimeExceeded:
        raise
    except Exception as error:
        fail(f"{what} not fully read: {error}")
    return collected


def slice_text(start: datetime | date, end: datetime | date) -> str:
    """A slice as `<start> to <end>`: moments in the chunk-error form, days as dates."""
    if isinstance(start, datetime):
        return f"{iso_utc(start)} to {iso_utc(end)}"
    return f"{start} to {end}"


def collect_in_slices(
    local, settings, result, fail, kind: str, slices, fetch: Callable[..., Iterator], http: HttpClient
) -> None:
    """
    Fetch, prepare and write one slice at a time and add up the rows written.

    A failed fetch, a value that isn't a finite number or a failed write is recorded with its slice; such a slice
    isn't written (a failed fetch keeps the rows read before it), and the next slices still run. Once the
    provider's deadline has passed, the slice in progress and the rest are skipped with one error.
    """
    slices = list(slices)
    for slice_start, slice_end in slices:
        what = f"{kind} of {slice_text(slice_start, slice_end)}"
        try:
            http.check_deadline()
            records = drain(fetch(slice_start, slice_end), fail, what)
        except RunTimeExceeded as error:
            fail(f"{error}: {kind} of {slice_text(slice_start, slices[-1][1])} skipped")
            return
        try:
            rows = prepare(records, settings.exclude_tags)
            if not rows:
                continue
            usage, cost = (rows, []) if kind == "usage" else ([], rows)
            result[f"{kind}_rows"] += sum(write_records(local, settings, usage, cost))
        except WriteFailed as error:
            result[f"{kind}_rows"] += error.written
            fail(f"{what} not written: {error}")
        except Exception as error:
            fail(f"{what} not written: {error}")


def collect_usage(local, settings, connector, result, fail, now, backfill) -> None:
    usage_types = [name for name in settings.usage_types if name in connector.usage_types]
    start, end = (
        backfill_usage_window(settings, now)
        if backfill
        else usage_window(now, settings.bucket_width, settings.usage_lookback)
    )
    oldest = connector.oldest_usage(now)
    if oldest:
        oldest = ceil_to_bucket(oldest, settings.bucket_width)
    if oldest and start < oldest:
        start = oldest
        result["notes"].append(f"usage is available from {oldest.isoformat()}; the period starts there")
    if not usage_types or start >= end:
        return
    result["usage_period"] = [start.isoformat(), end.isoformat()]
    slices = period_chunks(start, end, USAGE_SLICE_SPANS[settings.bucket_width])
    fetch = partial(connector.fetch_usage, bucket_width=settings.bucket_width, usage_types=usage_types)
    collect_in_slices(local, settings, result, fail, "usage", slices, fetch, connector.http)


def collect_cost(local, settings, connector, result, fail, now, backfill) -> None:
    if not connector.collects_cost():
        return
    key = COST_FETCHED_KEY + connector.name
    if not backfill and not cost_is_due(local, settings, connector.name, now):
        return
    start_day, end_day = (
        backfill_cost_window(settings, now)
        if backfill
        else cost_window(now, settings.cost_lookback)
    )
    oldest = connector.oldest_cost(now)
    if oldest and start_day < oldest:
        start_day = oldest
        result["notes"].append(f"cost is available from {oldest.isoformat()}; the period starts there")
    if not backfill:
        local.cache.put(key, now.isoformat())
    if start_day >= end_day:
        return
    result["cost_period"] = [start_day.isoformat(), end_day.isoformat()]
    slices = period_chunks(start_day, end_day, COST_SLICE_SPAN)
    collect_in_slices(local, settings, result, fail, "cost", slices, connector.fetch_cost, connector.http)


def collect_provider(local, settings, task_id, name, now, backfill, deadline: float) -> dict:
    """Collect one provider's usage and cost; no call starts after `deadline` (a time.monotonic() value)."""
    result = {
        "usage_period": None,
        "cost_period": None,
        "usage_rows": 0,
        "cost_rows": 0,
        "errors": [],
        "notes": [],
    }

    def fail(message: str) -> None:
        """Record and log each distinct error once per run, however many slices repeat it."""
        if message in result["errors"]:
            return
        local.error(f"[{task_id}] {name}: {message}")
        result["errors"].append(message)

    try:
        http = make_http_client()
        http.deadline = deadline
        connector = CONNECTORS[name](settings, http, local)
        connector.task_id = task_id
    except Exception as error:
        fail(f"cannot start: {type(error).__name__}: {error}")
        return result

    for kind, section in (("usage", collect_usage), ("cost", collect_cost)):
        if kind not in settings.collect:
            continue
        try:
            section(local, settings, connector, result, fail, now, backfill)
        except Exception as error:
            fail(f"{kind} collection failed: {error}")
    for message in connector.errors:
        fail(message)
    result["notes"].extend(connector.notes + connector.closing_notes())
    return result


def collect(
    influxdb3_local,
    settings: Settings,
    task_id: str,
    *,
    now: datetime,
    backfill: bool,
) -> dict[str, dict]:
    """
    Collect usage and cost of every provider; one provider's failure never stops the others.

    Each provider gets an equal share of what is left of max_run_time, so a slow one can't take the others' time.
    """
    run_end = time.monotonic() + settings.max_run_time.total_seconds()
    results = {}
    for index, name in enumerate(settings.providers):
        share = (run_end - time.monotonic()) / (len(settings.providers) - index)
        results[name] = collect_provider(
            influxdb3_local, settings, task_id, name, now, backfill, time.monotonic() + share
        )
    return results


# --- entry points ---


def new_task_id() -> str:
    return uuid.uuid4().hex[:8]


def log_summary(influxdb3_local, task_id: str, results: dict[str, dict]) -> None:
    """One INFO line per provider, then a WARN line per note: a scheduled run has no response to carry them."""
    for name, result in results.items():
        influxdb3_local.info(
            f"[{task_id}] {name}: wrote {result['usage_rows']} usage and "
            f"{result['cost_rows']} cost rows, {len(result['errors'])} errors"
        )
        for note in result["notes"]:
            influxdb3_local.warn(f"[{task_id}] {name}: {note}")


def process_scheduled_call(influxdb3_local, call_time, args=None):
    """Collect the trailing usage and, when due, cost window for every provider."""
    task_id = new_task_id()
    # the engine passes a naive call_time in the server's local time zone; astimezone reads it as such
    now = call_time.astimezone(timezone.utc)
    try:
        settings = load_settings(influxdb3_local, task_id, args, now=now)
        results = collect(influxdb3_local, settings, task_id, now=now, backfill=False)
    except Exception as error:
        influxdb3_local.error(f"[{task_id}] Collection failed: {error}")
        return
    log_summary(influxdb3_local, task_id, results)


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
) -> dict | tuple[dict, int]:
    """
    Backfill the period given as ``start`` and ``end``.

    Returns:
        The outcome per provider, answered with HTTP 200 even when some provider calls failed;
        ``(error body, 400)`` for a configuration error, ``(error body, 500)`` for an unexpected failure.
    """
    task_id = new_task_id()
    now = datetime.now(timezone.utc)
    try:
        settings = load_settings(
            influxdb3_local,
            task_id,
            args,
            request_body=request_body,
            request_headers=request_headers,
            query_parameters=query_parameters,
            http=True,
            now=now,
        )
        results = collect(influxdb3_local, settings, task_id, now=now, backfill=True)
    except ConfigError as error:
        influxdb3_local.error(f"[{task_id}] Invalid request: {error}")
        return {"status": "error", "error": str(error)}, 400
    except Exception as error:
        influxdb3_local.error(f"[{task_id}] Request failed: {error}")
        return {"status": "error", "error": str(error)}, 500
    log_summary(influxdb3_local, task_id, results)
    return {"status": "completed", "providers": results}
