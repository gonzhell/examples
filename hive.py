from operator import itemgetter

import pyhs2
from R1.core.config import config

from R1.core.r1_behave.model.errors import StopTestException, get_traceback


class Hive(object):
    def __init__(self, host=None, port=None, authMechanism=None, user=None, password=None, database=None, logger=None):
        self.logger = logger
        self.host = host or config.getsafe('hive', 'host') or '10.50.49.21'
        self.port = port or int(config.getsafe('hive', 'port') or 10000)
        self.authMechanism = authMechanism or config.getsafe('hive', 'authMechanism') or 'PLAIN'
        self.user = user or config.getsafe('hive', 'user') or 'root'
        self.password = password or config.getsafe('hive', 'password')
        self.database = database or config.getsafe('hive', 'database') or 'default'
        self._connection = None
        self._cursor = None

    _column_name = itemgetter('columnName')

    @property
    def connection(self):
        if not self._connection:
                self._connection = pyhs2.connect(host=self.host,
                                                 port=self.port,
                                                 authMechanism=self.authMechanism,
                                                 user=self.user,
                                                 password=self.password,
                                                 database=self.database)
        else:
                if self.logger:
                    self.logger.debug('Connected to Hive on {0}'.format(self.host))
        return self._connection

    def close(self):
        del self.cursor
        try:
            self.connection.close()
        except Exception as e:
            if self.logger:
                self.logger.error('Error while disconnecting from Hive on {0}\n{0}: {1}'.format(self.host, e.__class__.__name__, e.message))
            raise StopTestException(e)
        else:
            if self.logger:
                self.logger.debug('Disconnected from Hive on {0}'.format(self.host))

    def __del__(self):
        self.close()

    def finalize(self):
        self.close()

    def execute(self, query):
        self.cursor.execute(query)

    def GetResult(self,query):
        self.cursor.execute("set hive.hadoop.supports.splittable.combineinputformat=false")
        self.execute(query)
        return self.cursor.fetch()

    @property
    def data(self):
        if not self._cursor or not self.cursor.hasMoreRows:
            return None
        return self.cursor.fetchall()

    @property
    def json(self):
        if not self._cursor or not self.cursor.hasMoreRows:
            return None
        schema = map(lambda row: row['columnName'].split('.')[-1], self.cursor.getSchema())
        return [dict(zip(schema, item)) for item in self.cursor.fetchall()]

    @property
    def data_gen(self):
        if not self._cursor or not self.cursor.hasMoreRows:
            raise StopIteration

        while self.cursor.hasMoreRows:
            data = self.cursor.fetchone()
            for row in data:
                yield row

    @property
    def json_gen(self):
        if not self._cursor or not self.cursor.hasMoreRows:
            raise StopIteration
        schema = map(lambda row: row['columnName'].split('.')[-1], self.cursor.getSchema())
        while self.cursor.hasMoreRows:
            try:
                data = self.cursor.fetchone()
            except AttributeError:
                # skip exception, there is no data available
                pass
            if not data:
                del self.cursor
                break
            yield dict((key, value) for key, value in dict(zip(schema, data)).items() if value is not None)

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.connection.cursor()
        return self._cursor

    @cursor.deleter
    def cursor(self):
        if self._cursor:
            self._cursor.close()
            self._cursor = None
