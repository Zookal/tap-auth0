import os

import singer
from auth0.v3.authentication import GetToken
from auth0.v3.management import Auth0

LOGGER = singer.get_logger()

DEFAULT_PER_PAGE = 100

CONFIG = {
    "domain": None,
    "non_interactive_client_id": None,
    "non_interactive_client_secret": None,
    "per_page": DEFAULT_PER_PAGE,
    "start_date": None
}


def get_bookmark(state, stream_name, default):
    return state.get('bookmarks', {}).get(stream_name, default)


def get_auth0_client():
    get_token = GetToken(CONFIG['domain'])
    token = get_token.client_credentials(CONFIG['non_interactive_client_id'],
                                         CONFIG['non_interactive_client_secret'],
                                         'https://{}/api/v2/'.format(CONFIG['domain']))
    mgmt_api_token = token['access_token']
    return Auth0(CONFIG['domain'], mgmt_api_token)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity_name):
    schema = singer.utils.load_json(get_abs_path('schemas/{}.json'.format(entity_name)))
    return schema


def list_all_users(state):
    auth0 = get_auth0_client()
    users_schema = load_schema('users')
    singer.write_schema('users', users_schema, 'user_id')
    if state:
        state_last_updated_at = get_bookmark(state, "users", CONFIG["start_date"]).get("updated_at", CONFIG["start_date"])
    else:
        state_last_updated_at = CONFIG["start_date"]

    page = 0
    total_recs = 0
    max_recs = 1000  # limit by auth0 per search criteria
    filter_last_updated_at = state_last_updated_at
    LOGGER.info(f"Querying Users on Auth0 with the follwing filter: {filter_last_updated_at}")

    while True:
        filter = "updated_at:{" + filter_last_updated_at + " TO *]"

        resp = auth0.users.list(per_page=CONFIG["per_page"], page=page, sort="updated_at:1", q=filter)

        singer.write_records('users', resp["users"])

        page = page + 1
        number_records_retrieved = len(resp["users"])
        total_records_to_be_retrieved = resp.get("total", 0)
        total_recs = total_recs + number_records_retrieved
        LOGGER.info(
            f"""Processing page: {page}, total to be retrieved {total_records_to_be_retrieved} records: {number_records_retrieved} "total_recs: {total_recs} """)

        last_updated_at = resp["users"][number_records_retrieved - 1]["updated_at"]
        if last_updated_at >= state_last_updated_at:
            state_last_updated_at = last_updated_at
            state = singer.write_bookmark({}, "users", "updated_at", state_last_updated_at)

        if total_records_to_be_retrieved == total_recs and total_recs == max_recs:
            singer.write_state(state)
            page = 0
            total_recs = 0
            filter_last_updated_at = state_last_updated_at

        if total_records_to_be_retrieved < max_recs and total_recs == total_records_to_be_retrieved:
            singer.write_state(state)
            break


def main_impl():
    args = singer.utils.parse_args([
        "domain",
        "non_interactive_client_id",
        "non_interactive_client_secret"])

    CONFIG.update(args.config)

    list_all_users(args.state)


def main():
    try:
        main_impl()
    except Exception as exc:
        raise exc


if __name__ == '__main__':
    main()
