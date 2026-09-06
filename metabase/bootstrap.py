"""Create/update the three demo dashboards through the Metabase HTTP API."""
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE = os.environ.get('METABASE_URL', 'http://localhost:3000').rstrip('/')
# Browser-facing origin; BASE can be the internal Compose service address.
PUBLIC_BASE = os.environ.get('METABASE_PUBLIC_URL', 'http://localhost:3000').rstrip('/')
EMAIL = os.environ.get('METABASE_ADMIN_EMAIL', 'demo@tikblog.local')
PASSWORD = os.environ.get('METABASE_ADMIN_PASSWORD', 'Tikblog-demo-2026!')
TOKEN = None
ROOT = Path(__file__).parent
FILTERS = ('from_date', 'to_date', 'source')


def api(method, path, body=None):
    headers = {'Content-Type': 'application/json'}
    if TOKEN:
        headers['X-Metabase-Session'] = TOKEN
    request = Request(BASE + '/api' + path, method=method, headers=headers,
                      data=json.dumps(body).encode() if body is not None else None)
    try:
        with urlopen(request, timeout=120) as response:
            return json.loads(response.read() or b'null')
    except HTTPError as error:
        # Include validation errors so a failed bootstrap is actionable.
        detail = error.read().decode(errors='replace')
        raise RuntimeError(f'{method} {path}: HTTP {error.code}: {detail}') from error


def ensure_connection():
    databases = api('GET', '/database')['data']
    existing = next((d for d in databases if d['name'] == 'TikBlog Analytics'), None)
    config = {'name': 'TikBlog Analytics', 'engine': 'sparksql',
              'details': {'host': os.environ.get('SPARK_SQL_HOST', 'spark-sql'),
                          'port': int(os.environ.get('SPARK_SQL_PORT', '10000')),
                          'dbname': 'default', 'user': 'analytics', 'password': '',
                          'jdbc-flags': ''}}
    if existing:
        api('PUT', f'/database/{existing["id"]}', config)
        return existing['id']
    return api('POST', '/database', config)['id']


def provision(database_id):
    spec = json.loads((ROOT / 'dashboards.json').read_text())
    dashboards = api('GET', '/dashboard')
    cards = api('GET', '/card')
    public_links = []
    for title, queries in spec.items():
        dashboard = next((d for d in dashboards if d['name'] == title and not d.get('archived')), None)
        if dashboard is None:
            dashboard = api('POST', '/dashboard', {'name': title})
        current = api('GET', f'/dashboard/{dashboard["id"]}')
        existing_placements = {d['card_id']: d['id'] for d in current['dashcards']}
        placements = []
        management = title == 'Management'
        for position, item in enumerate(queries):
            name = f'{title}: {item["name"]}'
            sql = (ROOT / 'queries' / item['file']).read_text()
            tags = {}
            if management:
                tags = {name: {'id': name, 'name': name, 'display-name': name.replace('_', ' '),
                               'type': 'text' if name == 'source' else 'date'}
                        for name in FILTERS}
            query = {'type': 'native', 'database': database_id,
                     'native': {'query': sql, 'template-tags': tags}}
            settings = dict(item.get('settings', {}))
            if item.get('status_column'):
                settings['table.column_formatting'] = [
                    {'columns': [item['status_column']], 'type': 'single',
                     'operator': '=', 'value': status, 'color': color, 'highlight_row': True}
                    for status, color in (('GREEN', '#84BB4C'), ('YELLOW', '#F9CF48'),
                                          ('RED', '#ED6E6E'), ('UNKNOWN', '#949AAB'))]
            body = {'name': name, 'dataset_query': query, 'display': item.get('display', 'table'),
                    'description': item.get('description'), 'visualization_settings': settings}
            existing = next((c for c in cards if c['name'] == name and not c.get('archived')), None)
            card = api('PUT', f'/card/{existing["id"]}', body) if existing else api('POST', '/card', body)
            placements.append({'id': existing_placements.get(card['id'], -(position+1)), 'card_id': card['id'],
                'row': (position//2)*5, 'col': (position%2)*12, 'size_x': 12, 'size_y': 5,
                'series': [], 'visualization_settings': {},
                'parameter_mappings': [{'parameter_id': key, 'card_id': card['id'],
                    'target': ['variable', ['template-tag', key]]} for key in tags]})
        params = [{'id': name, 'name': name.replace('_', ' '), 'slug': name,
                   'type': 'string/=' if name == 'source' else 'date/single'}
                  for name in FILTERS] if management else []
        description = ('Unsubscribe requests received, not applied unsubscribes. Unique counts are exact '
                       'per day and source; they must not be summed across days or sources.' if management else
                       'Latest recorded statuses use configured pipeline thresholds. Check timestamps: '
                       'a recorded GREEN does not prove that a stopped pipeline is still healthy.')
        api('PUT', f'/dashboard/{dashboard["id"]}', {
            'name': title, 'description': description, 'parameters': params, 'dashcards': placements})
        public_uuid = current.get('public_uuid')
        if not public_uuid:
            public_uuid = api('POST', f'/dashboard/{dashboard["id"]}/public_link')['uuid']
        public_links.append((title, public_uuid))
    for title, public_uuid in public_links:
        print(f'{title}: {PUBLIC_BASE}/public/dashboard/{public_uuid}')


def main():
    global TOKEN
    for attempt in range(60):
        try:
            properties = api('GET', '/session/properties')
            break
        except (OSError, RuntimeError):
            if attempt == 59:
                raise
            time.sleep(5)
    setup = properties.get('setup-token')
    if setup and not properties.get('has-user-setup'):
        api('POST', '/setup', {'token': setup, 'prefs': {'site_name': 'TikBlog',
            'allow_tracking': False}, 'user': {'first_name': 'Demo', 'last_name': 'User',
            'email': EMAIL, 'password': PASSWORD}})
    TOKEN = api('POST', '/session', {'username': EMAIL, 'password': PASSWORD})['id']
    api('PUT', '/setting/enable-public-sharing', {'value': True})
    provision(ensure_connection())


if __name__ == '__main__':
    main()
