import itertools
import threading

import sqlparse
from sqlparse import tokens
from sqlparse.sql import Identifier


class SQLCompleter(object):
    ACTIONS = [
        'SELECT',
        'SELECT *',
        'SELECT INTO',
        'SELECT TOP',

        'UPDATE',
        'INSERT',
        'DELETE',
        'CREATE',
        'ALTER',
        'DROP',
        'SHOW',
        'SET',
        'USE',
        'INSERT INTO',

        'TRUNCATE TABLE',
        'DECLARE'
    ]

    OBJECTS = [
        'DATABASE',
        'SCHEMA',
        'TABLE',
        'INDEX',
        'COLUMN',
        'VIEW',
        'SEQUENCE',
        'PRIMARY KEY',
    ]

    KEYWORDS = [
        'FROM',
        'DISTINCT',
        'ORDER BY',
        'GROUP BY',
        'ASC',
        'DESC',
        'IS',
        'NULL',
        'WHERE',
        'AS',
        'HAVING',
        'INNER JOIN',
        'LEFT JOIN',
        'RIGHT JOIN',
        'FULL JOIN',
        'UNION',
        'UNION ALL',
        'VALUES',
        'DEFAULT',

        'ALL',
        'AND',
        'ANY',
        'BETWEEN',
        'EXISTS',
        'IN',
        'LIKE',
        'NOT',
        'OR',
        'UNIQUE'
    ]

    FUNCTIONS = [
        'MAX',
        'MIN',
        'COUNT',
        'AVG',
        'SUM',
        'UPPER',
        'LOWER',
        'CONCAT',
        'IFNULL',
        'REPLACE',
        'TRIM'
    ]

    EXTRA_KEYWORDS = []
    EXTRA_ACTIONS = []
    EXTRA_OBJECTS = []
    EXTRA_FUNCTIONS = []

    def __init__(self, db=None):
        self.db = db.copy()
        self.actions = self.ACTIONS + self.EXTRA_ACTIONS
        self.keywords = self.KEYWORDS + self.EXTRA_KEYWORDS
        self.objects = self.OBJECTS + self.EXTRA_OBJECTS
        self.functions = self.FUNCTIONS + self.EXTRA_FUNCTIONS
        self.db_support = True

        self._identifiers = None
        self._fetch_thread = threading.Thread(target=self.fetchall)
        self._fetch_thread.daemon = True
        self._fetch_thread.start()

    @property
    def identifiers(self):
        if self._identifiers is None and not self._fetch_thread.is_alive():
            self.fetchall()
        return self._identifiers or {}

    def fetch_schema(self, db=None):
        pass

    def fetch_databases(self):
        pass

    def fetch_tables(self, schema=None, db=None):
        pass

    def fetch_users(self):
        pass

    def fetch_columns(self, table, schema=None, db=None):
        pass

    def fetch_indexes(self, table, schema=None, db=None):
        pass

    def fetchall(self, force=False):
        if self.identifiers and not force:
            return self.identifiers

        self._identifiers = results = {}

        def get_tables(schema, db=None):
            tables = self.fetch_tables(schema, db=db) or []
            for table in tables:
                yield table, {
                    'columns': self.fetch_columns(table, schema) or [],
                    'indexes': self.fetch_indexes(table, schema) or []
                }

        databases = self.fetch_databases() or ()
        if not databases:
            self.db_support = False
            for schema in (self.fetch_schema() or []):
                results[schema] = {table: info for table, info in get_tables(schema)}

        else:
            for db in databases:
                ret = {}
                for schema in (self.fetch_schema(db) or []):
                    ret[schema] = {table: info for table, info in get_tables(schema)}
                results[db] = ret

    def quote_name(self, name):
        return name

    def unquote_name(self, name):
        return name

    def get_choices(self, text: "str"):
        text = (text or '').strip()
        if text:
            current_word = text.rsplit(maxsplit=1)[-1]
            if current_word == text:
                yield from self._match_choices(current_word, self.actions)
            else:
                stmt = sqlparse.parse(text)[0]
                tk = stmt.tokens[-1]
                if not self._isspace(tk) and tk.ttype is not tokens.Wildcard:
                    yield from self._get_choices(current_word, stmt)

    def _get_choices(self, current, stmt):
        tk = stmt.tokens[-1]
        if isinstance(tk, Identifier):
            yield from self._get_possible_objs(current)

        else:
            for word in itertools.chain(self.actions, self.objects, self.functions, self.keywords):
                if word.lower().startswith(current):
                    yield word, -len(current)

    def _get_possible_objs(self, current):
        rel_cnt = current.count('.')

        if rel_cnt == 0:
            for val in self._yields_all(self.identifiers, current):
                yield val, -len(current)

        elif rel_cnt == 1:
            name, sub = current.split('.')
            name = self.unquote_name(name)
            sub = self.unquote_name(sub)
            print(f'name is: "%s", and sub is "%s"' % (name, sub))
            print(self.identifiers)
            for val in self._yields_all(self.identifiers, sub, key=name):
                yield val, -len(sub)

        elif rel_cnt == 2 and self.db_support:
            db, schema, name = current.split('.')
            db = self.unquote_name(db)
            schema = self.unquote_name(schema)
            name = self.unquote_name(name)
            for val in self._yields_all(self.identifiers.get(db, {}), name, key=schema):
                yield val, -len(name)

    def _yields_all(self, results, prefix, key=None):
        if isinstance(results, dict):
            for k, v in results.items():
                if key:
                    if k.lower() != key:
                        yield from self._yields_all(v, prefix, key=key)
                    else:
                        yield from self._yields_all(v, prefix)
                else:
                    if k.lower().startswith(prefix) and not isinstance(v, list):
                        yield self.quote_name(k)
                    yield from self._yields_all(v, prefix)

        elif isinstance(results, list):
            for r in results:
                if r.lower().startswith(prefix):
                    yield self.quote_name(r)

        else:
            yield self.quote_name(results)

    def _isspace(self, tk):
        return tk.ttype in [tokens.Newline, tokens.Whitespace]

    def _match_choices(self, text, choices):
        for choice in choices:
            if choice.lower().startswith(text.lower()):
                yield choice, -len(text)


class MySQLCompleter(SQLCompleter):
    EXTRA_KEYWORDS = [
        'AUTO_INCREMENT',
    ]

    def quote_name(self, name):
        if not name.startswith('`') and not name.endswith('`'):
            return f'`{name}`'
        return name

    def unquote_name(self, name):
        return name.strip('`')

    def fetch_schema(self, db=None):
        return ['mysql', 'test', 'dbus', 'django', 'nicedb']

    def fetch_tables(self, schema=None, db=None):
        return [f'table_{n}' for n in range(1, 10)]
