# tap-auth0

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Auth0's [Management API v2](https://auth0.com/docs/api/management/v2)
- Extracts the following resources from Auth0
  - [Users](https://auth0.com/docs/api/management/v2#!/Users/get_users)

## Installation

```bash
pip install tap-auth0
```

## Configuration

This tap requires a `config.json` which specifies details regarding [M2M](https://auth0.com/docs/dashboard/guides/applications/register-app-m2m) authentication. See [config.sample.json](config.sample.json) for an example.

To run `tap-auth0` with the configuration file, use this command:

```bash
tap-auth0 -c my-config.json -d state.json
```

## Changelog
0.1.0 - code restructure to allow data extraction using updated_at and state file from singer. This will allow incremental loads  


## To-Do
Schema Data Validation
external schema/catalog control 


- Add the following ressources from Auth0:
    - [Branding](https://auth0.com/docs/api/management/v2#!/Branding/get_branding)
    - [Client Grants](https://auth0.com/docs/api/management/v2#!/Client_Grants/get_client_grants)
    - [Clients](https://auth0.com/docs/api/management/v2#!/Clients/get_clients)
    - [Connections](https://auth0.com/docs/api/management/v2#!/Connections/get_connections)
    - [Custom Domains](https://auth0.com/docs/api/management/v2#!/Custom_Domains/get_custom_domains)
    - [Device Credentials](https://auth0.com/docs/api/management/v2#!/Device_Credentials/get_device_credentials)
    - [Grants](https://auth0.com/docs/api/management/v2#!/Grants/get_grants)
    - [Hooks](https://auth0.com/docs/api/management/v2#!/Hooks/get_hooks)
    - [Log Streams](https://auth0.com/docs/api/management/v2#!/Log_Streams/get_log_streams)
    - [Logs](https://auth0.com/docs/api/management/v2#!/Logs/get_logs)
    - [Prompts](https://auth0.com/docs/api/management/v2#!/Prompts/get_prompts)
    - [Resource Servers](https://auth0.com/docs/api/management/v2#!/Resource_Servers/get_resource_servers)
    - [Roles](https://auth0.com/docs/api/management/v2#!/Roles/get_roles)
    - [Rules](https://auth0.com/docs/api/management/v2#!/Rules/get_rules)
    - [Rules Config](https://auth0.com/docs/api/management/v2#!/Rules_Configs/get_rules_configs)
    - [User Blocks](https://auth0.com/docs/api/management/v2#!/User_Blocks/get_user_blocks)
    - [Blacklist](https://auth0.com/docs/api/management/v2#!/Blacklists/get_tokens)
    - [Email Templates](https://auth0.com/docs/api/management/v2#!/Email_Templates/get_email_templates_by_templateName)
    - [Emails](https://auth0.com/docs/api/management/v2#!/Emails/get_provider)
    - [Guardian](https://auth0.com/docs/api/management/v2#!/Guardian/get_factors)
    - [Jobs](https://auth0.com/docs/api/management/v2#!/Jobs/get_jobs_by_id)
    - [Stats](https://auth0.com/docs/api/management/v2#!/Stats/get_active_users)
    - [Tenants](https://auth0.com/docs/api/management/v2#!/Tenants/get_settings)
    - [Anomaly](https://auth0.com/docs/api/management/v2#!/Anomaly/get_ips_by_id)
    - [Tickets](https://auth0.com/docs/api/management/v2#!/Tickets/post_email_verification)
