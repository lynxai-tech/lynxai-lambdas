from exceptions import BadJsonError, PathParameterError, QueryParameterError, ParameterError
from db import rds
import json


class Event:
    def __init__(self, event):
        self.event = event
        self._rds = None

    #--------------------------------------------#
    #------------------ Request -----------------#
    #--------------------------------------------#

    def query(self, name):
        try:
            return self.event['queryStringParameters'][name]
        except (KeyError, TypeError):
            raise QueryParameterError()

    def path(self, name):
        try:
            return self.event['pathParameters'][name]
        except (KeyError, TypeError):
            raise PathParameterError()

    def body(self, name=None):
        body = self.event['body']
        if isinstance(body,str):    
            try:
                if name is None:
                    return json.loads(self.event['body'])
                else:
                    return json.loads(self.event['body']).get(name)
            except json.JSONDecodeError:
                raise BadJsonError()
            except:
                raise
        else:
            if name is None:
                return self.event['body']
            else:
                return json.loads(self.event['body']).get(name)

    def param(self, name):
        try:
            return self.path(name)
        except PathParameterError:
            try:
                return self.query(name)
            except QueryParameterError:
                try:
                    return self.body()[name]
                except (KeyError, TypeError, BadJsonError):
                    raise ParameterError()

    #--------------------------------------------#
    #----------------- Identity -----------------#
    #--------------------------------------------#

    def username(self):
        return self.event['requestContext']['authorizer']['claims']['username']

    def groups(self):
        claims = self.event['requestContext']['authorizer']['claims']
        if 'cognito:groups' in claims:
            return claims['cognito:groups'].split(',')
        else:
            return []

    #--------------------------------------------#
    #----------------- Database -----------------#
    #--------------------------------------------#
    @property
    def rds(self):
        if self._rds:
            self._rds.event = self
            return self._rds
        else:
            self._rds = rds(event=self)
            return self._rds

    @rds.setter
    def rds(self, rds):
        self._rds = rds

    def change(self, sql, parameters=None):
        return self.rds.change(sql, parameters)

    def call(self, sql, parameters=None):
        return self.rds.call(sql, parameters)

    def select(self, sql, parameters=None):
        return self.rds.select(sql, parameters)

