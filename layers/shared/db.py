import mysql.connector
import os
import datetime
import hashlib
from pathlib import Path
import json
import re
from pprint import pprint
import boto3 as boto
from botocore.config import Config
from exceptions import QueryParameterMismatch, WrongNumberOfRecordsUpdated, QueryDoesntReturnOneRow, QueryDoesntReturnOneColumn, QueryDoesntReturnOneValue

if os.environ.get('AWS_LAMBDA_FUNCTION_VERSION') is None:
    boto3 = boto.Session(profile_name=os.environ.get('AWS_PROFILE_RUN'))
else:
    boto3 = boto

class RDSResultSet:
    def __init__(self, data):
        self.data = data

    def is_empty(self):
        return self.data is None

    def list(self):
        return self.data

    def dict(self):
        if len(self.data) != 1:
            raise QueryDoesntReturnOneRow()
        return self.data[0] if self.data[0] else dict()

    def val_list(self):
        all_data = self.list()
        if len(all_data) == 0:
            return []

        if len(all_data[0].keys()) != 1:
            raise QueryDoesntReturnOneColumn()

        col_name = list(all_data[0].keys())[0]
        return [row[col_name] for row in all_data]

    def val(self):
        if len(self.data) != 1 or len(self.data[0].keys()) != 1:
            raise QueryDoesntReturnOneValue()
        val = self.data[0][list(self.data[0].keys())[0]]
        try:
            return json.loads(val)
        except TypeError:
            return val
        except json.JSONDecodeError:
            return val


class SDB:
    errors = mysql.connector.errors

    def __init__(self, event=None, key=None):
        username, password = SDB._read_credentials(key=key)

        self.connection = SDB.create_connection(
            username, password, key=key)
        self.event = event
        self.executed_queries = []

    @staticmethod
    def create_connection(username, password, key=None):
        while True:
            try:
                connection = SDB._create_connection(
                    username, password, key=key)
                break
            except (mysql.connector.errors.OperationalError, mysql.connector.errors.DatabaseError) as e:
                print(e, type(e))
            except (mysql.connector.errors.InterfaceError) as e:
                print(e, type(e))
                username, password = SDB._retrieve_credentials()

        return connection

    @staticmethod
    def _create_connection(username, password, key=None):
        host = os.environ.get('DB_ENDPOINT' + '' if key is None else f'_{key}')

        args = {
            'host': host,
            'user': username,
            'password': password,
        }
        connection = mysql.connector.connect(**args)

        return connection

    @staticmethod
    def create_cursor(connection):
        cursor = connection.cursor(buffered=True, dictionary=True)
        return cursor

    @staticmethod
    def _read_credentials(key=None):
        m = hashlib.sha256()
        m.update((os.environ.get('RDS_SECRET_NAME') +
                 '' if key is None else f'_{key}').encode())
        s3_digest = m.hexdigest()
        if os.environ.get('AWS_LAMBDA_FUNCTION_VERSION'):
            s3_digest = Path('/tmp') / s3_digest
        if os.path.isfile(s3_digest):
            with open(s3_digest, 'r') as f:
                return json.loads(f.read())
        else:
            return SDB._retrieve_credentials()

    @staticmethod
    def _write_credentials(username, password, key=None):
        m = hashlib.sha256()
        m.update((os.environ.get('RDS_SECRET_NAME') +
                 '' if key is None else f'_{key}').encode())
        s3_digest = m.hexdigest()
        if os.environ.get('AWS_LAMBDA_FUNCTION_VERSION'):
            s3_digest = Path('/tmp') / s3_digest
        with open(s3_digest, 'w') as f:
            f.write(json.dumps((username, password)))

    @staticmethod
    def _retrieve_credentials(key=None):
        ssm = boto3.client(service_name='secretsmanager', config=Config(
            region_name=os.environ.get('SECRETS_REGION', os.environ.get('AWS_REGION', 'us-east-2'))))
        creds = ssm.get_secret_value(
            SecretId=os.environ.get('RDS_SECRET_NAME') + '' if key is None else f'_{key}')
        username = json.loads(creds['SecretString'])['username']
        password = json.loads(creds['SecretString'])['password']
        SDB._write_credentials(username, password, key=key)
        return username, password

    @staticmethod
    def convert_to_escaped(parameters):
        def converter(match_obj):
            name = match_obj.group(1)
            if name is not None:
                try:
                    parameters[name]
                except (KeyError, TypeError):
                    raise QueryParameterMismatch(f"""parameter {name}""")

                for f in (int, float, bool, datetime):
                    try:
                        if isinstance(f(parameters[name]),datetime):
                            return f(parameters[name]).strftime('%Y-%m-%d')
                        f(parameters[name])
                        return f'%({name})s'
                    except (ValueError, TypeError):
                        pass

                return f'%({name})s'

        return converter

    def retry(f):
        def g(*args, **kwargs):
            n_attempts = 0
            while n_attempts < 10:
                n_attempts += 1
                try:
                    return f(*args, **kwargs)
                except mysql.connector.errors.OperationalError:
                    pass
        return g

    def _query(self, sql, parameters=None, is_select=False, debug=False, cursor=None, multi=False):
        execution = None

        if isinstance(parameters, dict) or not parameters:
            if not parameters:
                parameters = {}
            for k, v in parameters.items():
                if isinstance(v, dict):
                    parameters[k] = json.dumps(v)
                elif isinstance(v, list):
                    parameters[k] = json.dumps(v)

            sql = re.sub(r"\:([A-Za-z\_0-9]+)",
                         SDB.convert_to_escaped(parameters), sql)

            execution = cursor.execute(sql, parameters, multi=multi)
            self.executed_queries += [sql]
        elif isinstance(parameters, list) and parameters:
            for i, x in enumerate(parameters):
                for k, v in x.items():
                    if isinstance(v, dict):
                        parameters[i][k] = json.dumps(v)
                    elif isinstance(v, list):
                        parameters[i][k] = json.dumps(v)

            if debug:
                print('sql before sub:\n', sql)

            sql = re.sub(r"\:([A-Za-z\_0-9]+)",
                         SDB.convert_to_escaped(parameters[0]), sql)

            if debug:
                print('sql after regex sub:\n', sql)
                pprint(parameters)

            execution = cursor.executemany(sql, parameters)
            self.executed_queries += [sql]
        else:
            raise Exception(
                """Invalid parameters. It must be either a dict or a list of dicts!""")

        try:
            return cursor.rowcount
        except Exception as e:
            print(e)  # TODO: this exception is too broad!
            m = 0
            for result in execution:
                m += result.rowcount
            return m

    @retry
    def select(self, sql, parameters=None, cursor=None):
        if cursor is None:
            cursor = SDB.create_cursor(self.connection)

        self._query(sql, parameters, cursor=cursor, multi=False)

        if self.connection.in_transaction:
            self.connection.commit()

        data = list(cursor.fetchall())
        for i, x in enumerate(data):
            for k, v in x.items():
                if isinstance(v, bytes):
                    try:
                        data[i][k] = v.decode()
                    except UnicodeDecodeError:
                        data[i][k] = v
                elif isinstance(v, (datetime.datetime, datetime.date)):
                    data[i][k] = v.isoformat()

        cursor.close()
        return RDSResultSet(data)

    @retry
    def callselect(self, sql, parameters=None, cursor=None):
        if cursor is None:
            cursor = SDB.create_cursor(self.connection)

        cursor.callproc(sql, parameters)
        for result in cursor.stored_results():
            cols = result.column_names
            data = result.fetchall()
            data = [dict(zip(cols, item)) for item in data]

        for i, x in enumerate(data):
            for k, v in x.items():
                if isinstance(v, bytes):
                    try:
                        data[i][k] = v.decode()
                    except UnicodeDecodeError:
                        data[i][k] = v
                elif isinstance(v, datetime.datetime):
                    data[i][k] = v.strftime('%Y-%m-%d %H:%M:%S')

        cursor.close()
        return RDSResultSet(data)


    @retry
    def call(self, sql, parameters=None, cursor=None):
        if cursor is None:
            cursor = SDB.create_cursor(self.connection)

        if isinstance(parameters, dict) or not parameters:

            parametersTuple = tuple(parameters.values())
            print(parametersTuple)
            n = cursor.callproc(sql, parametersTuple)

            if enforce:
                if isinstance(enforce, int):
                    if n != enforce:
                        raise WrongNumberOfRecordsUpdated()
                elif isinstance(enforce, bool):
                    if n == 0:
                        raise WrongNumberOfRecordsUpdated()
        elif isinstance(parameters, list):
            parametersTuple = tuple(parameters.values())
            print(parametersTuple)
            n = cursor.callproc(sql, parametersTuple)
            if enforce:
                if isinstance(enforce, int):
                    if n != enforce:
                        raise WrongNumberOfRecordsUpdated()
                elif isinstance(enforce, bool):
                    if n == len(parameters):
                        raise WrongNumberOfRecordsUpdated()
        else:
            raise Exception(
                "Parameters must be either a dict or a list of dicts.")

        self.connection.commit()

        cursor.close()
        return RDSResultSet(data)
    @retry
    def change(self, sql, parameters=None, enforce=False, cursor=None, multi = False):
        if cursor is None:
            cursor = SDB.create_cursor(self.connection)

        if isinstance(parameters, dict) or not parameters:
            n = self._query(sql, parameters, cursor=cursor, multi=multi)

            print(n)

            if enforce:
                if isinstance(enforce, int):
                    if n != enforce:
                        raise WrongNumberOfRecordsUpdated()
                elif isinstance(enforce, bool):
                    if n == 0:
                        raise WrongNumberOfRecordsUpdated()
        elif isinstance(parameters, list):
            n = self._query(sql, parameters, cursor=cursor)
            if enforce:
                if isinstance(enforce, int):
                    if n != enforce:
                        raise WrongNumberOfRecordsUpdated()
                elif isinstance(enforce, bool):
                    if n == len(parameters):
                        raise WrongNumberOfRecordsUpdated()
        else:
            raise Exception(
                "Parameters must be either a dict or a list of dicts.")

        self.connection.commit()

        res = {
            'numberOfRecordsUpdated': n,
            'generatedFields': [{'longValue': cursor.lastrowid}]
        }

        cursor.close()

        return res

    def close(self):
        self.connection.close()


class RDS:
    def __call__(self, event=None, key=None):
        return SDB(event=event, key=key)


rds = RDS()
