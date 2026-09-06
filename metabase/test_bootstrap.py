"""Offline API-contract checks; live query execution still needs Spark and Metabase."""

import copy
import json
import unittest
from unittest.mock import patch

from metabase import bootstrap


class MetabaseAPI:
    """Small stateful fake for the endpoints used by our bootstrap."""

    def __init__(self):
        self.databases = {}
        self.dashboards = {}
        self.cards = {}
        self.next_id = 1
        self.public_links_created = 0

    def create(self, records, body):
        result = copy.deepcopy(body)
        result['id'] = self.next_id
        self.next_id += 1
        records[result['id']] = result
        return copy.deepcopy(result)

    def __call__(self, method, path, body=None):
        parts = path.strip('/').split('/')
        records = {'database': self.databases, 'dashboard': self.dashboards, 'card': self.cards}[parts[0]]
        if parts[0] == 'card' and method in ('POST', 'PUT'):
            assert body.get('description') is None or body['description'].strip()
        if len(parts) == 3 and parts[2] == 'public_link' and method == 'POST':
            record = records[int(parts[1])]
            if not record.get('public_uuid'):
                record['public_uuid'] = f'00000000-0000-0000-0000-{record["id"]:012d}'
                self.public_links_created += 1
            return {'uuid': record['public_uuid']}
        if len(parts) == 1:
            if method == 'GET':
                values = copy.deepcopy(list(records.values()))
                return {'data': values} if parts[0] == 'database' else values
            if method == 'POST':
                if parts[0] == 'dashboard':
                    body = {**body, 'dashcards': []}
                return self.create(records, body)
        if len(parts) == 2:
            record = records[int(parts[1])]
            if method == 'PUT':
                body = copy.deepcopy(body)
                for card in body.get('dashcards', []):
                    if card['id'] < 0:
                        card['id'] = self.next_id
                        self.next_id += 1
                record.update(body)
            return copy.deepcopy(record)
        raise AssertionError(f'Unexpected API request: {method} {path}')


class BootstrapTest(unittest.TestCase):
    def test_repeated_bootstrap_updates_in_place_and_wires_filters(self):
        api = MetabaseAPI()
        with patch.object(bootstrap, 'api', api), patch('builtins.print') as output:
            database_id = bootstrap.ensure_connection()
            bootstrap.provision(database_id)
            initial = copy.deepcopy(api.dashboards)
            card_ids = set(api.cards)
            self.assertEqual(bootstrap.ensure_connection(), database_id)
            bootstrap.provision(database_id)

        self.assertEqual(len(api.databases), 1)
        self.assertEqual(api.databases[database_id]['details']['jdbc-flags'], '')
        self.assertEqual(api.public_links_created, 3)
        self.assertEqual(len(api.cards), 16)
        self.assertEqual(output.call_args_list[:3], output.call_args_list[3:])
        for dashboard in api.dashboards.values():
            output.assert_any_call(
                f'{dashboard["name"]}: {bootstrap.PUBLIC_BASE}/public/dashboard/'
                f'{dashboard["public_uuid"]}'
            )
        self.assertEqual(set(api.cards), card_ids)
        self.assertEqual(api.dashboards, initial)
        self.assertEqual({d['name'] for d in api.dashboards.values()},
                         {'Management', 'Data Quality', 'Pipeline Health'})
        for dashboard in api.dashboards.values():
            expected = set(bootstrap.FILTERS) if dashboard['name'] == 'Management' else set()
            self.assertEqual({p['id'] for p in dashboard['parameters']}, expected)
            for placement in dashboard['dashcards']:
                self.assertLessEqual(placement['col'] + placement['size_x'], 24)
                card = api.cards[placement['card_id']]
                self.assertEqual(card['dataset_query']['database'], database_id)
                self.assertEqual(set(card['dataset_query']['native']['template-tags']), expected)
                self.assertEqual({p['parameter_id'] for p in placement['parameter_mappings']}, expected)

    def test_setup_token_can_remain_after_first_user_is_created(self):
        for has_user in (False, True):
            with self.subTest(has_user=has_user), patch.object(bootstrap, 'api') as api, \
                    patch.object(bootstrap, 'ensure_connection', return_value=1), \
                    patch.object(bootstrap, 'provision') as provision, \
                    patch.object(bootstrap, 'TOKEN', None):
                api.side_effect = lambda method, path, body=None: {
                    '/session/properties': {'setup-token': 'token', 'has-user-setup': has_user},
                    '/setup': {}, '/session': {'id': 'session'},
                    '/setting/enable-public-sharing': None,
                }[path]
                bootstrap.main()
                setup_calls = [c for c in api.call_args_list if c.args[1] == '/setup']
                self.assertEqual(len(setup_calls), 0 if has_user else 1)
                api.assert_any_call('PUT', '/setting/enable-public-sharing', {'value': True})
                provision.assert_called_once_with(1)

    def test_status_formatting_and_daily_unique_semantics(self):
        api = MetabaseAPI()
        with patch.object(bootstrap, 'api', api), patch('builtins.print'):
            bootstrap.provision(1)
        status = next(c for c in api.cards.values() if c['name'] == 'Data Quality: Overall latest DQ status')
        formats = status['visualization_settings']['table.column_formatting']
        self.assertEqual({f['value'] for f in formats}, {'GREEN', 'YELLOW', 'RED', 'UNKNOWN'})
        unique = next(c for c in api.cards.values() if 'Daily unique' in c['name'])
        sql = unique['dataset_query']['native']['query'].lower()
        self.assertNotIn('sum(', sql)
        self.assertIn('event_date, source, unique_readers, unique_writers', sql)
        self.assertIn('not additive', unique['description'])


if __name__ == '__main__':
    unittest.main()
